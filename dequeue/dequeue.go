package dequeue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

type dequeuer struct {
	c                    ingest.Client
	s                    storage.Storage
	l                    log.Logger
	r                    prometheus.Registerer
	q                    ingest.Queue
	cleanUp              bool
	webhookURL           string
	batchSize            int
	streamName           string
	consumerName         string
	subjectName          string
	dequeueAttemptsTotal *prometheus.CounterVec
	webhookRequestsTotal *prometheus.CounterVec
}

// New creates a new ingest.Dequeuer.
func New(webhookURL string, c ingest.Client, s storage.Storage, q ingest.Queue, streamName, consumerName, subjectName string, batchSize int, cleanUp bool, l log.Logger, r prometheus.Registerer) ingest.Dequeuer {
	if l == nil {
		l = log.NewNopLogger()
	}
	ic := &client{
		Client: c,
		operationsTotal: promauto.With(r).NewCounterVec(
			prometheus.CounterOpts{
				Name: "ingest_client_operations_total",
				Help: "Number of client operations",
			}, []string{"operation", "result"}),
	}
	return &dequeuer{
		c:            ic,
		s:            s,
		l:            l,
		r:            r,
		q:            q,
		cleanUp:      cleanUp,
		webhookURL:   webhookURL,
		batchSize:    batchSize,
		streamName:   streamName,
		consumerName: consumerName,
		subjectName:  subjectName,

		dequeueAttemptsTotal: promauto.With(r).NewCounterVec(
			prometheus.CounterOpts{
				Name: "ingest_dequeue_attempts_total",
				Help: "Number of dequeue sync attempts.",
			}, []string{"result"}),
		webhookRequestsTotal: promauto.With(r).NewCounterVec(
			prometheus.CounterOpts{
				Name: "ingest_webhook_http_client_requests_total",
				Help: "The number webhook HTTP requests.",
			}, []string{"result"}),
	}
}

func (d *dequeuer) Dequeue(ctx context.Context) error {
	level.Debug(d.l).Log("msg", "subscribing to stream", "consumer", d.consumerName, "stream", d.streamName)
	sub, err := d.q.PullSubscribe(d.subjectName, d.consumerName, nats.BindStream(d.streamName))
	if err != nil {
		return fmt.Errorf("failed to subscribe to stream: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return sub.Close()
		default:
		}

		tctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		msgs, err := sub.Pop(d.batchSize, nats.Context(tctx))
		cancel()
		if err != nil {
			level.Error(d.l).Log("msg", "failed to dequeue messages from queue", "err", err.Error())
			continue
		}
		level.Info(d.l).Log("msg", fmt.Sprintf("dequeued %d messages from queue", len(msgs)))

		uris := make([]string, 0, d.batchSize)
		for _, raw := range msgs {
			// item, ok := any((*new(T))).(ingest.Codec)
			// if !ok {
			item := new(ingest.SimpleCodec)
			//}
			if err := item.Unmarshal(raw.Data); err != nil {
				level.Error(d.l).Log("msg", "failed to marshal message", "err", err.Error())
				continue
			}
			u, err := d.process(ctx, item)
			if err != nil {
				level.Error(d.l).Log("msg", "failed to process message", "id", item.ID(), "name", item.Name(), "err", err.Error())
			} else {
				level.Info(d.l).Log("msg", "successfully processed message", "id", item.ID(), "name", item.Name(), "data", string(raw.Data))
			}
			if err := raw.AckSync(); err != nil {
				level.Error(d.l).Log("msg", "failed to ack message", "id", item.ID(), "name", item.Name(), "err", err.Error())
				continue
			}
			level.Debug(d.l).Log("msg", "acked message", "id", item.ID(), "name", item.Name(), "data", string(raw.Data))
			if u != nil {
				uris = append(uris, u.String())
			}
		}

		if d.webhookURL != "" && len(uris) > 0 {
			if err := d.callWebhook(ctx, uris); err != nil {
				d.webhookRequestsTotal.WithLabelValues("error").Inc()
				level.Warn(d.l).Log("warn", "failed to call a webhook", "msg", err.Error())
				continue
			}
			d.webhookRequestsTotal.WithLabelValues("success").Inc()
		}
	}
}

func (d *dequeuer) process(ctx context.Context, item ingest.Identifiable) (*url.URL, error) {
	var u *url.URL
	operation := func() error {
		_, err := d.s.Stat(ctx, item)
		if err == nil {
			if d.cleanUp {
				return d.c.CleanUp(ctx, item)
			}

			return nil
		}
		if !os.IsNotExist(err) {
			return err
		}

		u, err = d.s.Store(ctx, item, d.c.Download)
		if err != nil {
			return err
		}

		if d.cleanUp {
			return d.c.CleanUp(ctx, item)
		}

		return nil
	}

	if err := operation(); err != nil {
		d.dequeueAttemptsTotal.WithLabelValues("error").Inc()
		return nil, err
	}

	d.dequeueAttemptsTotal.WithLabelValues("success").Inc()
	return u, nil
}

func (d *dequeuer) callWebhook(ctx context.Context, data []string) error {
	requestData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", d.webhookURL, bytes.NewBuffer(requestData))
	if err != nil {
		return err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	defer io.Copy(io.Discard, res.Body)

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("webhook request failed with status code: %d", res.StatusCode)
	}

	return nil
}

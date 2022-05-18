package dequeue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

type dequeuer[T ingest.Identifiable] struct {
	c                           ingest.Client[T]
	s                           storage.Storage[T]
	l                           log.Logger
	r                           prometheus.Registerer
	q                           ingest.Queue
	cleanUp                     bool
	webhookURL                  string
	batchSize                   int
	streamName                  string
	consumerName                string
	subjectName                 string
	dequeuerErrorCounter        prometheus.Counter
	dequeuerAttemptCounter      prometheus.Counter
	webhookRequestsTotalCounter *prometheus.CounterVec
}

// New creates a new ingest.Dequeuer.
func New[T ingest.Identifiable](webhookURL string, c ingest.Client[T], s storage.Storage[T], q ingest.Queue, streamName, consumerName, subjectName string, batchSize int, cleanUp bool, l log.Logger, r prometheus.Registerer) ingest.Dequeuer {
	if l == nil {
		l = log.NewNopLogger()
	}
	return &dequeuer[T]{
		c:            c,
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

		dequeuerErrorCounter: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "dequeuer_errors_total",
			Help: "Number of errors that occured while syncing items.",
		}),
		dequeuerAttemptCounter: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "dequeuer_attempts_total",
			Help: "Number of item sync attempts.",
		}),
		webhookRequestsTotalCounter: promauto.With(r).NewCounterVec(
			prometheus.CounterOpts{
				Name: "webhook_http_client_requests_total",
				Help: "The number webhook HTTP requests.",
			}, []string{"result"}),
	}
}

func (d *dequeuer[T]) Dequeue(ctx context.Context) error {
	sub, err := d.q.PullSubscribe(d.subjectName, d.consumerName, nats.Bind(d.streamName, d.consumerName))
	if err != nil {
		return err
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
			var job T
			if err := json.Unmarshal(raw.Data, &job); err != nil {
				level.Error(d.l).Log("msg", "failed to marshal message", "err", err.Error())
				continue
			}
			u, err := d.process(ctx, job)
			if err != nil {
				level.Error(d.l).Log("msg", "failed to process message", "err", err.Error())
				continue
			}
			level.Info(d.l).Log("msg", "successfully processed message", "data", job)
			if err := raw.AckSync(); err != nil {
				level.Error(d.l).Log("msg", "failed to ack message", "err", err.Error())
				continue
			}
			level.Debug(d.l).Log("msg", "acked message", "data", job)
			if u != nil {
				uris = append(uris, u.String())
			}
		}

		if d.webhookURL != "" && len(uris) > 0 {
			if err := d.callWebhook(ctx, uris); err != nil {
				d.webhookRequestsTotalCounter.WithLabelValues("error").Inc()
				level.Warn(d.l).Log("warn", "failed to call a webhook", "msg", err.Error())
				continue
			}
			d.webhookRequestsTotalCounter.WithLabelValues("success").Inc()
		}
	}
}

func (d *dequeuer[T]) process(ctx context.Context, job T) (*url.URL, error) {
	d.dequeuerAttemptCounter.Inc()

	var u *url.URL
	operation := func() error {
		_, err := d.s.Stat(ctx, job)
		if err == nil {
			if d.cleanUp {
				return d.c.CleanUp(ctx, job)
			}

			return nil
		}
		if !os.IsNotExist(err) {
			return err
		}

		u, err = d.s.Store(ctx, job, d.c.Download)
		if err != nil {
			return err
		}

		if d.cleanUp {
			return d.c.CleanUp(ctx, job)
		}

		return nil
	}

	bctx := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	if err := backoff.Retry(operation, bctx); err != nil {
		d.dequeuerErrorCounter.Inc()
		return nil, err
	}

	return u, nil
}

func (d *dequeuer[Client]) callWebhook(ctx context.Context, data []string) error {
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
	defer io.Copy(ioutil.Discard, res.Body)

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("webhook request failed with status code: %d", res.StatusCode)
	}

	return nil
}

func doneKey(name string) string {
	return fmt.Sprintf("%s.done", name)
}

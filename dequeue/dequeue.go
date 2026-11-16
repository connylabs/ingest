package dequeue

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"

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
	concurrency          int
	streamName           string
	consumerName         string
	subjectName          string
	dequeueAttemptsTotal *prometheus.CounterVec
	webhookRequestsTotal *prometheus.CounterVec
}

// New creates a new ingest.Dequeuer.
func New(webhookURL string, c ingest.Client, s storage.Storage, q ingest.Queue, streamName, consumerName, subjectName string, batchSize, concurrency int, cleanUp bool, l log.Logger, r prometheus.Registerer) ingest.Dequeuer {
	if l == nil {
		l = log.NewNopLogger()
	}

	dequeueAttemptsTotal := promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_dequeue_attempts_total",
		Help: "Number of dequeue sync attempts.",
	}, []string{"result"})

	webhookRequestsTotal := promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_webhook_http_client_requests_total",
		Help: "The number webhook HTTP requests.",
	}, []string{"result"})

	for _, c := range []*prometheus.CounterVec{dequeueAttemptsTotal, webhookRequestsTotal} {
		for _, r := range []string{"error", "success"} {
			c.WithLabelValues(r).Add(0)
		}
	}

	return &dequeuer{
		c:                    NewInstrumentedClient(c, r),
		s:                    s,
		l:                    l,
		r:                    r,
		q:                    q,
		cleanUp:              cleanUp,
		webhookURL:           webhookURL,
		batchSize:            batchSize,
		concurrency:          concurrency,
		streamName:           streamName,
		consumerName:         consumerName,
		subjectName:          subjectName,
		dequeueAttemptsTotal: dequeueAttemptsTotal,
		webhookRequestsTotal: webhookRequestsTotal,
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

		msgs, err := sub.Pop(ctx, d.batchSize)
		if err != nil {
			level.Error(d.l).Log("msg", "failed to dequeue messages from queue", "err", err.Error())
			if errors.Is(err, nats.ErrConnectionClosed) {
				return err
			}
			continue

		}
		level.Info(d.l).Log("msg", fmt.Sprintf("dequeued %d messages from queue", len(msgs)))

		g, egCtx := errgroup.WithContext(ctx)
		g.SetLimit(d.concurrency)
		uris := make([]string, d.batchSize)
		for i, raw := range msgs {
			i, raw := i, raw
			g.Go(func() error {
				item := new(ingest.Codec)
				if err := item.Unmarshal(raw.Data); err != nil {
					level.Error(d.l).Log("msg", "failed to marshal message", "err", err.Error())
					return err
				}
				u, err := d.process(egCtx, *item)
				if err != nil {
					level.Error(d.l).Log("msg", "failed to process message", "id", item.ID, "name", item.Name, "err", err.Error())
				} else {
					level.Info(d.l).Log("msg", "successfully processed message", "id", item.ID, "name", item.Name, "data", string(raw.Data))
				}
				if err := raw.AckSync(); err != nil {
					level.Error(d.l).Log("msg", "failed to ack message", "id", item.ID, "name", item.Name, "err", err.Error())
					return err
				}
				level.Debug(d.l).Log("msg", "acked message", "id", item.ID, "name", item.Name, "data", string(raw.Data))
				if u != nil {
					uris[i] = u.String()
				}
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			level.Error(d.l).Log("msg", "at least one go routine returned an error", "err", err.Error())
		}

		filteredUIRs := make([]string, 0, d.batchSize)
		for _, uri := range uris {
			if uri != "" {
				filteredUIRs = append(filteredUIRs, uri)
			}
		}

		if d.webhookURL != "" && len(filteredUIRs) > 0 {
			if err := d.callWebhook(ctx, filteredUIRs); err != nil {
				d.webhookRequestsTotal.WithLabelValues("error").Inc()
				level.Warn(d.l).Log("warn", "failed to call a webhook", "msg", err.Error())
				continue
			}
			d.webhookRequestsTotal.WithLabelValues("success").Inc()
		}
	}
}

func (d *dequeuer) process(ctx context.Context, item ingest.Codec) (*url.URL, error) {
	var u *url.URL
	operation := func() error {
		var err error
		u, err = Transfer(ctx, d.c, d.s, item)
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
	return CallWebhook(ctx, d.webhookURL, data)
}

package enqueue

import (
	"context"
	"encoding/json"
	"io"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/connylabs/ingest"
)

type enqueuer[T any] struct {
	q                    ingest.Queue
	n                    ingest.Nexter[T]
	l                    log.Logger
	queueSubject         string
	enqueueAttemptsTotal *prometheus.CounterVec
}

// New creates new ingest.Enqueuer.
func New[T any](n ingest.Nexter[T], queueSubject string, q ingest.Queue, r prometheus.Registerer, l log.Logger) (ingest.Enqueuer, error) {
	if l == nil {
		l = log.NewNopLogger()
	}
	return &enqueuer[T]{
		q:            q,
		n:            n,
		l:            l,
		queueSubject: queueSubject,
		enqueueAttemptsTotal: promauto.With(r).NewCounterVec(
			prometheus.CounterOpts{
				Name: "ingest_enqueue_attempts_total",
				Help: "Number of enqueue sync attempts.",
			}, []string{"result"}),
	}, nil
}

// Enqueue will add all of the objects that the Nexter will produce into the queue.
// Note: Enqueue is not safe to call concurrently because it modifies the state
// of a single, shared Nexter.
func (e *enqueuer[Client]) Enqueue(ctx context.Context) error {
	err := e.n.Reset(ctx)
	if err != nil {
		return err
	}
	operation := func() error {
		for {
			item, err := e.n.Next(ctx)
			if err != nil {
				if err == io.EOF {
					return backoff.Permanent(err)
				}
				level.Warn(e.l).Log("msg", "failed to get next item", "err", err.Error())
				return err
			}
			data, err := json.Marshal(item)
			if err != nil {
				level.Warn(e.l).Log("msg", "failed to unmarshal retrieved item", "err", err.Error())
				return err
			}

			if err := e.q.Publish(e.queueSubject, data); err != nil {
				level.Warn(e.l).Log("msg", "failed to publish item to queue", "err", err.Error())
				return err
			}
		}
	}

	bctx := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	if err := backoff.Retry(operation, bctx); err != nil {
		if err == io.EOF {
			return nil
		}
		e.enqueueAttemptsTotal.WithLabelValues("error").Inc()
		return err
	}

	e.enqueueAttemptsTotal.WithLabelValues("success").Inc()
	return nil
}

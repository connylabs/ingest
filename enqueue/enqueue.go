package enqueue

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/mietright/ingest"
)

type enqueuer[T any] struct {
	q                     ingest.Queue
	n                     ingest.Nexter[T]
	l                     log.Logger
	interval              time.Duration
	queueSubject          string
	enqueueErrorCounter   prometheus.Counter
	enqueueAttemptCounter prometheus.Counter
}

// New creates new ingest.Enqueuer.
func New[T any](n ingest.Nexter[T], queueSubject string, q ingest.Queue, reg prometheus.Registerer, interval time.Duration, l log.Logger) (ingest.Enqueuer, error) {
	if l == nil {
		l = log.NewNopLogger()
	}
	return &enqueuer[T]{
		q:            q,
		n:            n,
		l:            l,
		interval:     interval,
		queueSubject: queueSubject,
		enqueueErrorCounter: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "enqueue_errors_total",
			Help: "Number of errors occurred while importing documents.",
		}),
		enqueueAttemptCounter: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "enqueue_attempts_total",
			Help: "Number of document import attempts.",
		}),
	}, nil
}

// Enqueue will add all of the objects that the Nexter will produce into the queue.
// Note: Enqueue is not safe to call concurrently because it modifies the state
// of a single, shared Nexter.
func (e *enqueuer[Client]) Enqueue(ctx context.Context) error {
	e.enqueueAttemptCounter.Inc()

	err := e.n.Reset(ctx)
	if err != nil {
		return err
	}
	operation := func() error {
		for {
			document, err := e.n.Next(ctx)
			if err != nil {
				if err == io.EOF {
					return backoff.Permanent(err)
				}
				level.Warn(e.l).Log("msg", "failed to get next document", "err", err.Error())
				return err
			}
			data, err := json.Marshal(document)
			if err != nil {
				level.Warn(e.l).Log("msg", "failed to unmarshal retrieved document", "err", err.Error())
				return err
			}

			if err := e.q.Publish(e.queueSubject, data); err != nil {
				level.Warn(e.l).Log("msg", "failed to publish document to queue", "err", err.Error())
				return err
			}
		}
	}

	bctx := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	if err := backoff.Retry(operation, bctx); err != nil {
		if err == io.EOF {
			return nil
		}
		e.enqueueErrorCounter.Inc()
		return err
	}
	return nil
}

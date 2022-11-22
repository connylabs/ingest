package enqueue

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/connylabs/ingest"
)

type enqueuer struct {
	q                    ingest.Queue
	n                    ingest.Nexter
	l                    log.Logger
	queueSubject         string
	enqueueAttemptsTotal *prometheus.CounterVec
}

// New creates new ingest.Enqueuer.
func New(n ingest.Nexter, queueSubject string, q ingest.Queue, r prometheus.Registerer, l log.Logger) (ingest.Enqueuer, error) {
	if l == nil {
		l = log.NewNopLogger()
	}

	enqueueAttemptsTotal := promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_enqueue_attempts_total",
		Help: "Number of enqueue sync attempts.",
	}, []string{"result"})

	for _, r := range []string{"error", "success"} {
		enqueueAttemptsTotal.WithLabelValues(r).Add(0)
	}

	return &enqueuer{
		q:                    q,
		n:                    n,
		l:                    l,
		queueSubject:         queueSubject,
		enqueueAttemptsTotal: enqueueAttemptsTotal,
	}, nil
}

// Enqueue will add all of the objects that the Nexter will produce into the queue.
// Note: Enqueue is not safe to call concurrently because it modifies the state
// of a single, shared Nexter.
func (e *enqueuer) Enqueue(ctx context.Context) error {
	if err := e.enqueue(ctx); err != nil {
		e.enqueueAttemptsTotal.WithLabelValues("error").Inc()
		return err
	}

	e.enqueueAttemptsTotal.WithLabelValues("success").Inc()
	return nil
}

// enqueue will add all of the objects that the Nexter will produce into the queue.
// Note: Enqueue is not safe to call concurrently because it modifies the state
// of a single, shared Nexter.
func (e *enqueuer) enqueue(ctx context.Context) error {
	if err := e.n.Reset(ctx); err != nil {
		return fmt.Errorf("failed to reset nexter: %w", err)
	}
	operation := func() error {
		level.Info(e.l).Log("msg", "getting next items from source")
		for {
			level.Debug(e.l).Log("msg", "attempting to get next item")
			codec, err := e.n.Next(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				level.Warn(e.l).Log("msg", "failed to get next item", "err", err.Error())

				return fmt.Errorf("failed to get next item: %w", err)
			}
			data, err := codec.Marshal()
			if err != nil {
				level.Warn(e.l).Log("msg", "failed to marshal retrieved item", "err", err.Error())

				return fmt.Errorf("failed to marshel retrieved item: %w", err)
			}

			if err := e.q.Publish(e.queueSubject, data); err != nil {
				level.Warn(e.l).Log("msg", "failed to publish item to queue", "err", err.Error())

				return fmt.Errorf("failed to publish item to queue: %w", err)
			}
		}
	}

	if err := operation(); err != nil {
		return err
	}

	level.Debug(e.l).Log("msg", "successfully enqueued items")
	return nil
}

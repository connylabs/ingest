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
		level.Error(e.l).Log("msg", "failed to get next item", "err", err.Error())
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
	level.Info(e.l).Log("msg", "getting next items from source")

	codec, err := e.n.Next(ctx)
	count := 0
	for ; err != nil; codec, err = e.n.Next(ctx) {
		data, err := codec.Marshal()
		if err != nil {
			return fmt.Errorf("failed to marshal retrieved item: %w", err)
		}

		if err := e.q.Publish(e.queueSubject, data); err != nil {
			return fmt.Errorf("failed to publish item to queue: %w", err)
		}
		count++
	}

	if errors.Is(err, io.EOF) {
		level.Info(e.l).Log("msg", "successfully enqueued items", "items", count)

		return nil
	}

	return fmt.Errorf("failed to get next item: %w", err)
}

package enqueue

import (
	"context"
	"io"

	"github.com/cenkalti/backoff/v4"
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
	backoff              func() backoff.BackOff
	queueSubject         string
	enqueueAttemptsTotal *prometheus.CounterVec
}

type configuration struct {
	backoff func() backoff.BackOff
}

// WithBackoff can be passes as an option to disable the exponential backoff strategy.
func WithBackoff(b bool) Option {
	return func(c *configuration) {
		if b {
			c.backoff = func() backoff.BackOff { return backoff.NewExponentialBackOff() }
			return
		}
		c.backoff = func() backoff.BackOff { return &backoff.StopBackOff{} }
	}
}

// Option modifies the handler's configuration.
type Option func(c *configuration)

// New creates new ingest.Enqueuer.
func New(n ingest.Nexter, queueSubject string, q ingest.Queue, r prometheus.Registerer, l log.Logger, opts ...Option) (ingest.Enqueuer, error) {
	if l == nil {
		l = log.NewNopLogger()
	}
	c := &configuration{
		backoff: func() backoff.BackOff { return backoff.NewExponentialBackOff() },
	}
	for _, o := range opts {
		o(c)
	}
	return &enqueuer{
		q:            q,
		n:            n,
		l:            l,
		backoff:      c.backoff,
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
	err := e.n.Reset(ctx)
	if err != nil {
		return err
	}
	operation := func() error {
		level.Info(e.l).Log("msg", "getting next item from source")
		for {
			level.Debug(e.l).Log("msg", "attempting to get next item")
			item, err := e.n.Next(ctx)
			if err != nil {
				if err == io.EOF {
					return backoff.Permanent(err)
				}
				level.Warn(e.l).Log("msg", "failed to get next item", "err", err.Error())
				return err
			}
			codec, ok := any(item).(ingest.Codec)
			if !ok {
				codec = &ingest.SimpleCodec{XID: item.ID(), XName: item.Name()}
			}
			data, err := codec.Marshal()
			if err != nil {
				level.Warn(e.l).Log("msg", "failed to marshal retrieved item", "err", err.Error())
				return err
			}

			if err := e.q.Publish(e.queueSubject, data); err != nil {
				level.Warn(e.l).Log("msg", "failed to publish item to queue", "err", err.Error())
				return err
			}
		}
	}
	level.Debug(e.l).Log("msg", "run backoff")

	bctx := backoff.WithContext(e.backoff(), ctx)
	if err := backoff.Retry(operation, bctx); err != nil && err != io.EOF {
		return err
	}

	return nil
}

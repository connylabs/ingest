package enqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/mietright/ingest/queue"
)

// Nexter is able to list the objects available in the external API and returns them one by one.
// A Nexter must be implemented for the specific service.
type Nexter[Job any] interface {
	// Reset initializes or resets the state of the Nexter.
	// After Reset, calls of Next should retrieve all objects.
	Reset(context.Context) (Nexter[Job], error)
	// Next returns one Job that represents an object.
	// If all objects were returned by Next io.EOF must be returned.
	Next(context.Context) (Job, error)
}

// Enqueue is able to enqueue jobs into NATS.
type Enqueue[Job any] interface {
	Enqueue(context.Context) error
	Run(context.Context, *run.Group, queue.Queue) error
}

func (e *enqueue[Job]) Run(pctx context.Context, g *run.Group, qc queue.Queue) error {
	ctx, cancel := context.WithCancel(pctx)
	g.Add(func() error {
		level.Info(e.l).Log("msg", "starting the bea-s3 enqueue")
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			if err := qc.Close(ctx); err != nil {
				level.Error(e.l).Log("error", "failed to gracefully close the connection to the queue", "msg", err.Error())
			}
		}()

		if e.timeout == 0 {
			if err := e.Enqueue(ctx); err != nil {
				return fmt.Errorf("enqueue exited unexpectedly: %w", err)
			}
			return nil
		}
		for {
			ticker := time.NewTicker(e.timeout)
			select {
			case <-ticker.C:
				if err := e.Enqueue(ctx); err != nil {
					level.Error(e.l).Log("error", "failed to import documents", "msg", err.Error())
				}
			case <-ctx.Done():
				ticker.Stop()
				return nil
			}
		}
	}, func(error) {
		cancel()
	})

	return nil
}

type enqueue[Job any] struct {
	q                     queue.Queue
	n                     Nexter[Job]
	l                     log.Logger
	timeout               time.Duration
	queueSubject          string
	enqueueErrorCounter   prometheus.Counter
	enqueueAttemptCounter prometheus.Counter
}

// New creates new enqueue
func New[Job any](n Nexter[Job], q queue.Queue, queueSubject string, reg prometheus.Registerer, timeout time.Duration, l log.Logger) Enqueue[Job] {
	return &enqueue[Job]{
		q:            q,
		n:            n,
		l:            l,
		timeout:      timeout,
		queueSubject: queueSubject,
		enqueueErrorCounter: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "enqueue_errors_total",
			Help: "Number of errors occured while importing documents.",
		}),
		enqueueAttemptCounter: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "enqueue_attempts_total",
			Help: "Number of document import attempts.",
		}),
	}
}

func (i *enqueue[Client]) Enqueue(ctx context.Context) error {
	i.enqueueAttemptCounter.Inc()

	n, err := i.n.Reset(ctx)
	if err != nil {
		return err
	}
	operation := func() error {
		for {
			document, err := n.Next(ctx)
			if err != nil {
				if err == io.EOF {
					return backoff.Permanent(err)
				}
				level.Warn(i.l).Log("msg", "failed to get next document", "err", err.Error())
				return err
			}
			data, err := json.Marshal(document)
			if err != nil {
				level.Warn(i.l).Log("msg", "failed to unmarshal retrieved document", "err", err.Error())
				return err
			}

			if err := i.q.Publish(i.queueSubject, data); err != nil {
				level.Warn(i.l).Log("msg", "failed to publish document to queue", "err", err.Error())
				return err
			}

			// skip infinite loop in e2e tests
			if i.timeout == 0 {
				return nil
			}
		}
	}

	ctx, cancel := context.WithTimeout(ctx, i.timeout)
	defer cancel()

	bctx := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	if err := backoff.Retry(operation, bctx); err != nil {
		if err == io.EOF {
			return nil
		}
		i.enqueueErrorCounter.Inc()
		return err
	}
	return nil
}

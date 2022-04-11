package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/log/level"

	"github.com/connylabs/ingest"
)

// NewEnqueuerRunner produces a runnable function from an ingest.Enqueuer.
// The Enqueue method will be executed every interval until the given context is cancelled.
func NewEnqueuerRunner(ctx context.Context, e ingest.Enqueuer, interval time.Duration, l log.Logger) func() error {
	if l == nil {
		l = log.NewNopLogger()
	}

	return func() error {
		level.Info(l).Log("msg", "starting the enqueuer")

		if interval == 0 {
			if err := e.Enqueue(ctx); err != nil {
				return fmt.Errorf("enqueuer exited unexpectedly: %w", err)
			}
			return nil
		}
		for {
			ticker := time.NewTicker(interval)
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(ctx, interval)
				if err := e.Enqueue(ctx); err != nil {
					level.Error(l).Log("msg", "failed to enqueue", "err", err.Error())
				}
				defer cancel()
			case <-ctx.Done():
				ticker.Stop()
				return nil
			}
		}
	}
}

// NewDequeuerRunner produces a runnable function from an ingest.Dequeuer.
// The Dequeue method will run until the given context is cancelled.
func NewDequeuerRunner(ctx context.Context, d ingest.Dequeuer, l log.Logger) func() error {
	if l == nil {
		l = log.NewNopLogger()
	}

	return func() error {
		level.Info(l).Log("msg", "starting the dequeuer")
		if err := d.Dequeue(ctx); err != nil {
			return fmt.Errorf("dequeuer exited unexpectedly: %w", err)
		}
		return nil
	}
}

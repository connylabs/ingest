package queue

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/connylabs/ingest"
)

type subscription struct {
	sub              *nats.Subscription
	popsTotalCounter *prometheus.CounterVec
}

func newSubscription(sub *nats.Subscription, cv *prometheus.CounterVec) ingest.Subscription {
	return &subscription{
		sub:              sub,
		popsTotalCounter: cv,
	}
}

func (s *subscription) Close() error {
	if s.sub == nil {
		return nil
	}
	return s.sub.Drain()
}

func (s *subscription) Pop(ctx context.Context, batch int) ([]*nats.Msg, error) {
	msgs, err := s.sub.Fetch(batch, nats.Context(ctx))
	for ; errors.Is(err, context.DeadlineExceeded); msgs, err = s.sub.Fetch(batch, nats.Context(ctx)) {
		select {
		case <-ctx.Done():
			// If the given context is done, then break.
			break
		default:
			// If the given context is not done, then NATS's internal timeout
			// was exceeded, so let's try again.
		}
	}
	if err != nil {
		s.popsTotalCounter.WithLabelValues("error").Inc()
		return nil, err
	}
	s.popsTotalCounter.WithLabelValues("success").Inc()
	return msgs, nil
}

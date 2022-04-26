package queue

import (
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/connylabs/ingest"
)

type subscription struct {
	sub                           *nats.Subscription
	queueInteractionsTotalCounter *prometheus.CounterVec
}

func newSubscription(sub *nats.Subscription, cv *prometheus.CounterVec) ingest.Subscription {
	return &subscription{
		sub:                           sub,
		queueInteractionsTotalCounter: cv,
	}
}

func (s *subscription) Close() error {
	if s.sub == nil {
		return nil
	}
	return s.sub.Drain()
}

func (s *subscription) Pop(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
	msgs, err := s.sub.Fetch(batch, opts...)
	if err != nil {
		s.queueInteractionsTotalCounter.WithLabelValues("pop", "error").Inc()
		return nil, err
	}
	s.queueInteractionsTotalCounter.WithLabelValues("pop", "success").Inc()

	return msgs, nil
}

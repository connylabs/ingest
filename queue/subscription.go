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

func (f *subscription) Close() error {
	if f.sub == nil {
		return nil
	}
	return f.sub.Drain()
}

func (f *subscription) Pop(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
	msgs, err := f.sub.Fetch(batch, opts...)
	if err != nil {
		f.queueInteractionsTotalCounter.WithLabelValues("pop", "error")
		return nil, err
	}
	f.queueInteractionsTotalCounter.WithLabelValues("pop", "success")

	return msgs, nil
}

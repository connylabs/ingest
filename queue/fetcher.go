package queue

import (
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Dequeuer is able to pull a batch of messages from a stream for a pull consumer.
type Dequeuer interface {
	Dequeue(int, ...nats.PullOpt) ([]*nats.Msg, error)
}

type dequeuer struct {
	sub                           *nats.Subscription
	queueInteractionsTotalCounter *prometheus.CounterVec
}

func newDequeuer(sub *nats.Subscription, reg prometheus.Registerer) Dequeuer {
	queueInteractionsTotalCounter := promauto.With(reg).NewCounterVec(
		prometheus.CounterOpts{
			Name: "queue_operations_total",
			Help: "The number of interactions with queue.",
		}, []string{"operation", "result"})

	return &dequeuer{
		sub:                           sub,
		queueInteractionsTotalCounter: queueInteractionsTotalCounter,
	}
}

func (f *dequeuer) Dequeue(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
	msgs, err := f.sub.Fetch(batch, opts...)
	if err != nil {
		f.queueInteractionsTotalCounter.WithLabelValues("fetch", "error")
		return nil, err
	}
	f.queueInteractionsTotalCounter.WithLabelValues("fetch", "success")

	return msgs, nil
}

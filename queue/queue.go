package queue

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/connylabs/ingest"
)

type queue struct {
	js                            nats.JetStreamContext
	conn                          *nats.Conn
	reg                           prometheus.Registerer
	queueInteractionsTotalCounter *prometheus.CounterVec
}

// New is able to connect to the queue
func New(url string, reg prometheus.Registerer) (ingest.Queue, error) {
	conn, err := nats.Connect(url)
	if err != nil {
		return &queue{conn: nil}, err
	}

	js, err := conn.JetStream()
	if err != nil {
		return &queue{conn: nil}, err
	}

	queueInteractionsTotalCounter := promauto.With(reg).NewCounterVec(
		prometheus.CounterOpts{
			Name: "ingest_queue_operations_total",
			Help: "The number of interactions with queue.",
		}, []string{"operation", "result"})

	return &queue{conn: conn, js: js, queueInteractionsTotalCounter: queueInteractionsTotalCounter}, nil
}

// Close closes the connection to the queue.
func (qc *queue) Close(ctx context.Context) error {
	defer qc.conn.Close()
	return qc.conn.FlushWithContext(ctx)
}

// Publish is able to publish message to queue
func (qc *queue) Publish(subject string, data []byte) error {
	_, err := qc.js.Publish(subject, data)
	if err != nil {
		qc.queueInteractionsTotalCounter.WithLabelValues("publish", "error").Inc()
		return err
	}
	qc.queueInteractionsTotalCounter.WithLabelValues("publish", "success").Inc()
	return nil
}

// PullSubscribe creates a Subscription that can fetch messages.
func (qc *queue) PullSubscribe(subject string, durable string, opts ...nats.SubOpt) (ingest.Subscription, error) {
	sub, err := qc.js.PullSubscribe(subject, durable, opts...)
	if err != nil {
		return nil, err
	}

	return newSubscription(sub, qc.queueInteractionsTotalCounter), nil
}

package queue

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/exp/slices"

	"github.com/connylabs/ingest"
)

type queue struct {
	js                          nats.JetStreamContext
	conn                        *nats.Conn
	queueOperationsTotalCounter *prometheus.CounterVec
}

// New is able to connect to the queue
func New(url string, stream string, replicas int, subjects []string, maxMsgs int64, reg prometheus.Registerer) (ingest.Queue, error) {
	conn, err := nats.Connect(url)
	if err != nil {
		return &queue{conn: nil}, err
	}

	js, err := conn.JetStream()
	if err != nil {
		return &queue{conn: nil}, err
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      stream,
		Subjects:  subjects,
		Retention: nats.InterestPolicy,
		Replicas:  replicas,
		MaxMsgs:   maxMsgs,
	})
	if errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		si, err := js.StreamInfo(stream)
		if err != nil {
			return &queue{conn: nil}, err
		}
		if si.Config.Retention != nats.InterestPolicy {
			return &queue{conn: nil}, errors.New("expected stream with interest based retention")
		}
		if !slices.Equal(si.Config.Subjects, subjects) {
			return &queue{conn: nil}, fmt.Errorf("expected stream with subjects %v", subjects)
		}

	} else if err != nil {
		return &queue{conn: nil}, err
	}

	queueOperationsTotalCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_queue_operations_total",
		Help: "The total number of queue operations.",
	}, []string{"operation", "result"})

	for _, o := range []string{"pop", "publish"} {
		for _, r := range []string{"error", "success"} {
			queueOperationsTotalCounter.WithLabelValues(o, r).Add(0)
		}
	}

	return &queue{conn: conn, js: js, queueOperationsTotalCounter: queueOperationsTotalCounter}, nil
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
		qc.queueOperationsTotalCounter.WithLabelValues("publish", "error").Inc()
		return err
	}
	qc.queueOperationsTotalCounter.WithLabelValues("publish", "success").Inc()
	return nil
}

// PullSubscribe creates a Subscription that can fetch messages.
func (qc *queue) PullSubscribe(subject string, durable string, opts ...nats.SubOpt) (ingest.Subscription, error) {
	sub, err := qc.js.PullSubscribe(subject, durable, opts...)
	if err != nil {
		return nil, err
	}

	return newSubscription(sub, qc.queueOperationsTotalCounter), nil
}

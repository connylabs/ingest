package dequeue

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/connylabs/ingest"
)

type client[T ingest.Identifiable] struct {
	ingest.Client[T]
	operationsTotal *prometheus.CounterVec
}

func (c *client[T]) Download(ctx context.Context, t T) (o ingest.Object, err error) {
	o, err = c.Client.Download(ctx, t)
	if err != nil {
		c.operationsTotal.WithLabelValues("download", "error").Inc()
		return
	}
	c.operationsTotal.WithLabelValues("download", "success").Inc()
	return
}

func (c *client[T]) CleanUp(ctx context.Context, t T) (err error) {
	err = c.Client.CleanUp(ctx, t)
	if err != nil {
		c.operationsTotal.WithLabelValues("cleanup", "error").Inc()
		return
	}
	c.operationsTotal.WithLabelValues("cleanup", "success").Inc()
	return
}

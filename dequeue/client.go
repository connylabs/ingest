package dequeue

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/connylabs/ingest"
)

// client is an instrumented client that can wrap any ingest.Client.
type client struct {
	ingest.Client
	operationsTotal *prometheus.CounterVec
}

func (c *client) Download(ctx context.Context, item ingest.Identifiable) (o ingest.Object, err error) {
	o, err = c.Client.Download(ctx, item)
	if err != nil {
		c.operationsTotal.WithLabelValues("download", "error").Inc()
		return
	}
	c.operationsTotal.WithLabelValues("download", "success").Inc()
	return
}

func (c *client) CleanUp(ctx context.Context, item ingest.Identifiable) (err error) {
	err = c.Client.CleanUp(ctx, item)
	if err != nil {
		c.operationsTotal.WithLabelValues("cleanup", "error").Inc()
		return
	}
	c.operationsTotal.WithLabelValues("cleanup", "success").Inc()
	return
}

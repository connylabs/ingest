package dequeue

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/connylabs/ingest"
)

// instrumentedClient is an instrumented instrumentedClient that can wrap any ingest.Client.
type instrumentedClient struct {
	ingest.Client
	operationsTotal *prometheus.CounterVec
}

func newInstrumentedClient(c ingest.Client, r prometheus.Registerer) ingest.Client {
	operationsTotal := promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_client_operations_total",
		Help: "Number of client operations",
	}, []string{"operation", "result"})

	for _, o := range []string{"download", "cleanup"} {
		for _, r := range []string{"error", "success"} {
			operationsTotal.WithLabelValues(o, r).Add(0)
		}
	}

	return &instrumentedClient{
		Client:          c,
		operationsTotal: operationsTotal,
	}
}

func (c *instrumentedClient) Download(ctx context.Context, item ingest.Identifiable) (o ingest.Object, err error) {
	o, err = c.Client.Download(ctx, item)
	if err != nil {
		c.operationsTotal.WithLabelValues("download", "error").Inc()
		return
	}
	c.operationsTotal.WithLabelValues("download", "success").Inc()
	return
}

func (c *instrumentedClient) CleanUp(ctx context.Context, item ingest.Identifiable) (err error) {
	err = c.Client.CleanUp(ctx, item)
	if err != nil {
		c.operationsTotal.WithLabelValues("cleanup", "error").Inc()
		return
	}
	c.operationsTotal.WithLabelValues("cleanup", "success").Inc()
	return
}

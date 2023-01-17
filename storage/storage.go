package storage

import (
	"context"
	"net/url"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/connylabs/ingest"
)

// ObjectInfo describes an object in storage.
type ObjectInfo struct {
	// URI is the location under which the object can be found.
	URI string
}

// Storage must know how to store and stat objects.
// it allows applications to plug different storage systems.
type Storage interface {
	// Stat can be used to find information about the object corresponding to the given element.
	// If the object does not exist, then Stat returns an error satisfied by os.IsNotExist.
	Stat(ctx context.Context, element ingest.Codec) (*ObjectInfo, error)
	Store(ctx context.Context, element ingest.Codec, obj ingest.Object) (*url.URL, error)
}

type instrumentedStorage struct {
	Storage
	operationsTotal   *prometheus.CounterVec
	operationDuration *prometheus.HistogramVec
}

func (i instrumentedStorage) Stat(ctx context.Context, element ingest.Codec) (*ObjectInfo, error) {
	start := time.Now()
	defer func() {
		i.operationDuration.WithLabelValues("stat").Observe(time.Since(start).Seconds())
	}()

	oi, err := i.Storage.Stat(ctx, element)
	if err == nil || os.IsNotExist(err) {
		i.operationsTotal.WithLabelValues("stat", "success").Inc()
	} else {
		i.operationsTotal.WithLabelValues("stat", "error").Inc()
	}
	return oi, err
}

func (i instrumentedStorage) Store(ctx context.Context, element ingest.Codec, obj ingest.Object) (*url.URL, error) {
	start := time.Now()
	defer func() {
		i.operationDuration.WithLabelValues("store").Observe(time.Since(start).Seconds())
	}()

	u, err := i.Storage.Store(ctx, element, obj)
	if err == nil || os.IsNotExist(err) {
		i.operationsTotal.WithLabelValues("store", "success").Inc()
	} else {
		i.operationsTotal.WithLabelValues("store", "error").Inc()
	}
	return u, err
}

// NewInstrumentedStorage adds Prometheus metrics to any Storage.
func NewInstrumentedStorage(s Storage, r prometheus.Registerer) Storage {
	operationsTotal := promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_storage_operations_total",
		Help: "Number of storage operations.",
	}, []string{"operation", "result"})

	operationDuration := promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ingest_storage_operation_duration_seconds",
		Help:    "How many seconds it took to perform the storage operation.",
		Buckets: []float64{0.01, 0.1, 0.3, 1, 3, 10, 30, 60, 90, 120, 240, 360, 600},
	}, []string{"operation"})

	for _, o := range []string{"stat", "store"} {
		operationDuration.WithLabelValues(o).Observe(0)
		for _, r := range []string{"error", "success"} {
			operationsTotal.WithLabelValues(o, r).Add(0)
		}
	}

	return &instrumentedStorage{
		s,
		operationsTotal,
		operationDuration,
	}
}

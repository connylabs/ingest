package storage

import (
	"context"
	"net/url"
	"os"

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
type Storage[T ingest.Identifiable] interface {
	// Stat can be used to find information about the object corresponding to the given element.
	// If the object does not exist, then Stat returns an error satisfied by os.IsNotExist.
	Stat(ctx context.Context, element T) (*ObjectInfo, error)
	Store(ctx context.Context, element T, download func(context.Context, T) (ingest.Object, error)) (*url.URL, error)
}

type instrumentedStorage[T ingest.Identifiable] struct {
	Storage[T]
	operationsTotal *prometheus.CounterVec
}

func (i instrumentedStorage[T]) Stat(ctx context.Context, element T) (*ObjectInfo, error) {
	oi, err := i.Storage.Stat(ctx, element)
	if err == nil || os.IsNotExist(err) {
		i.operationsTotal.WithLabelValues("stat", "success").Inc()
	} else {
		i.operationsTotal.WithLabelValues("stat", "error").Inc()
	}
	return oi, err

}

func (i instrumentedStorage[T]) Store(ctx context.Context, element T, download func(context.Context, T) (ingest.Object, error)) (*url.URL, error) {
	u, err := i.Storage.Store(ctx, element, download)
	if err == nil || os.IsNotExist(err) {
		i.operationsTotal.WithLabelValues("store", "success").Inc()
	} else {
		i.operationsTotal.WithLabelValues("store", "error").Inc()
	}
	return u, err

}

// NewInstrumentedStorage adds Prometheus metrics to any Storage.
func NewInstrumentedStorage[T ingest.Identifiable](s Storage[T], r prometheus.Registerer) Storage[T] {
	return &instrumentedStorage[T]{
		s,
		promauto.With(r).NewCounterVec(
			prometheus.CounterOpts{
				Name: "ingest_storage_operations_total",
				Help: "Number of storage operations.",
			}, []string{"operation", "result"}),
	}
}

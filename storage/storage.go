package storage

import (
	"context"
	"net/url"

	"github.com/connylabs/ingest"
)

// Storage must know how to store and stat objects.
// it allows applications to plug different storage systems.
type Storage[T ingest.Identifiable] interface {
	Store(ctx context.Context, element T, download func(context.Context, T) (ingest.Object, error)) (*url.URL, error)
}

package storage

import (
	"context"
	"net/url"

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

package plugin

import (
	"context"
	"errors"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

// ErrNotImplemented can be returned by a plugin to indicate
// that it does not support acting as a source or destination.
var ErrNotImplemented = errors.New("not implemented")

// A Register symbol must be exposed by a plugin's shared object binary.
// The Register method is called only once for every plugin, regardless of
// how many sources or destinations use the plugin.
type Register func() (Plugin, error)

// A Source represents an API from which objects should be downloaded.
type Source interface {
	ingest.Nexter
	ingest.Client
}

// A Destination represents an API to which objects should be uploaded.
type Destination interface {
	storage.Storage
}

// Plugin is the principal interface that all plugins must implement.
type Plugin interface {
	// NewSource creates a new Source given a configuration.
	// If the plugin does not support acting as a source, then it should return ErrNotImplemented.
	NewSource(ctx context.Context, config map[string]interface{}) (Source, error)
	// NewDestination creates a new Destination given a configuration.
	// If the plugin does not support acting as a destination, then it should return ErrNotImplemented.
	NewDestination(ctx context.Context, config map[string]interface{}) (Destination, error)
}

package config

import "github.com/connylabs/ingest/plugin"

// SourceTyper implements the plugin.Source interface and exposes an additional method
// to determine the kind of plugin that is wrapped.
type SourceTyper struct {
	plugin.SourceInternal
	t string
}

// Type exposes the kind of plugin.
func (st *SourceTyper) Type() string {
	return st.t
}

// DestinationTyper implements the plugin.Typer interface and exposes an additional method
// to determine the kind of plugin that is wrapped.
type DestinationTyper struct {
	plugin.Destination
	t string
}

// Type exposes the kind of plugin.
func (dt *DestinationTyper) Type() string {
	return dt.t
}

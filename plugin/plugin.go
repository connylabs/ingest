package plugin

import (
	"context"
	"errors"
	"net/rpc"

	hclog "github.com/hashicorp/go-hclog"
	hplugin "github.com/hashicorp/go-plugin"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

// ErrNotImplemented can be returned by a plugin to indicate
// that it does not support acting as a source or destination.
var ErrNotImplemented = errors.New("not implemented")

// A Source represents an API from which objects should be downloaded.
type Source interface {
	ingest.Nexter
	ingest.Client
	Configure(map[string]any) error
}

// A Destination represents an API to which objects should be uploaded.
type Destination interface {
	storage.Storage
	Configure(map[string]any) error
}

type pluginSource struct {
	impl Source
	l    hclog.Logger
	ctx  context.Context
}

func (p *pluginSource) Server(mb *hplugin.MuxBroker) (interface{}, error) {
	return &pluginSourceRPCServer{Impl: p.impl, mb: mb, l: p.l, ctx: p.ctx}, nil
}

func (p *pluginSource) Client(mb *hplugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &pluginSourceRPC{client: c, mb: mb}, nil
}

type pluginDestination struct {
	impl Destination
	l    hclog.Logger
	ctx  context.Context
}

func (p *pluginDestination) Client(mb *hplugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &pluginDestinationRPC{client: c, mb: mb}, nil
}

func (p *pluginDestination) Server(mb *hplugin.MuxBroker) (interface{}, error) {
	return &pluginDestinationRPCServer{Impl: p.impl, mb: mb, l: p.l, ctx: p.ctx}, nil
}

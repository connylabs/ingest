package plugin

import (
	"context"
	"errors"
	"net/rpc"

	hclog "github.com/hashicorp/go-hclog"
	hplugin "github.com/hashicorp/go-plugin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

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

type SourceInternal interface {
	Source
	prometheus.Gatherer
}

// A Destination represents an API to which objects should be uploaded.
type Destination interface {
	storage.Storage
	Configure(map[string]any) error
}

// A Destination represents an API to which objects should be uploaded.
type DestinationInternal interface {
	Destination
	prometheus.Gatherer
}

type pluginSource struct {
	impl Source
	cs   []prometheus.Collector
	l    hclog.Logger
	ctx  context.Context
}

func (p *pluginSource) Server(mb *hplugin.MuxBroker) (interface{}, error) {
	reg := prometheus.NewRegistry()

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	for _, c := range p.cs {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}

	return &pluginSourceRPCServer{Impl: p.impl, mb: mb, l: p.l, ctx: p.ctx, reg: reg}, nil
}

func (p *pluginSource) Client(mb *hplugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &pluginSourceRPC{client: c, mb: mb}, nil
}

type pluginDestination struct {
	impl Destination
	cs   []prometheus.Collector
	l    hclog.Logger
	ctx  context.Context
}

func (p *pluginDestination) Client(mb *hplugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &pluginDestinationRPC{client: c, mb: mb}, nil
}

func (p *pluginDestination) Server(mb *hplugin.MuxBroker) (interface{}, error) {
	reg := prometheus.NewRegistry()

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	for _, c := range p.cs {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}

	return &pluginDestinationRPCServer{Impl: p.impl, mb: mb, l: p.l, ctx: p.ctx, reg: reg}, nil
}

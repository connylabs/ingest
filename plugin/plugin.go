package plugin

import (
	"context"
	"errors"
	"fmt"
	"net/rpc"
	"os"
	"os/exec"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-multierror"
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

func rpcClient(path string) (hplugin.ClientProtocol, error) {
	handshakeConfig := hplugin.HandshakeConfig{
		ProtocolVersion:  PluginMagicProtocalVersion,
		MagicCookieKey:   PluginMagicCookieKey,
		MagicCookieValue: PluginCookieValue,
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Name:       "plugin",
		JSONFormat: true,
		Output:     os.Stdout,
		Level:      hclog.Debug,
	})

	pluginMap := map[string]hplugin.Plugin{
		"destination": &pluginDestination{},
		"source":      &pluginSource{},
	}

	client := hplugin.NewClient(&hplugin.ClientConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
		Cmd:             exec.Command(path),
		Logger:          logger,
	})

	rpcClient, err := client.Client()
	if err != nil {
		client.Kill()
		return nil, fmt.Errorf("failed to create rpc client interface: %w", err)
	}
	return rpcClient, nil
}

func NewDestination(path string) (Destination, error) {
	rc, err := rpcClient(path)
	if err != nil {
		return nil, err
	}
	return newDestination(rc)
}

func NewSource(path string) (Source, error) {
	rc, err := rpcClient(path)
	if err != nil {
		return nil, err
	}
	return newSource(rc)
}

func newDestination(cp hplugin.ClientProtocol) (Destination, error) {
	raw, err := cp.Dispense("destination")
	if err != nil {
		err = fmt.Errorf("failed to dispense destination: %w", err)
		if cErr := cp.Close(); cErr != nil {
			err = multierror.Append(err, fmt.Errorf("failed to close client: %w", cErr))
		}

		return nil, err

	}
	return raw.(Destination), nil
}

func newSource(cp hplugin.ClientProtocol) (Source, error) {
	raw, err := cp.Dispense("source")
	if err != nil {
		err = fmt.Errorf("failed to dispense source: %w", err)
		if cErr := cp.Close(); cErr != nil {
			err = multierror.Append(err, fmt.Errorf("failed to close client: %w", cErr))
		}

		return nil, err

	}
	return raw.(Source), nil
}

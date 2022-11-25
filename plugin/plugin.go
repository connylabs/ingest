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

func NewPlugin(ctx context.Context, path string) (Destination, Source, error) {
	handshakeConfig := hplugin.HandshakeConfig{
		ProtocolVersion:  PluginMagicProtocalVersion,
		MagicCookieKey:   PluginMagicCookieKey,
		MagicCookieValue: PluginCookieValue,
	}

	// pluginMap is the map of plugins we can dispense.
	pluginMap := map[string]hplugin.Plugin{
		"destination": &pluginDestination{},
		"source":      &pluginSource{},
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Name:       "plugin",
		JSONFormat: true,
		Output:     os.Stdout,
		Level:      hclog.Debug,
	})

	// We're a host! Start by launching the plugin process.
	client := hplugin.NewClient(&hplugin.ClientConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
		Cmd:             exec.CommandContext(ctx, path),
		Logger:          logger,
	})

	// Connect via RPC
	rpcClient, err := client.Client()
	if err != nil {
		client.Kill()
		return nil, nil, err
	}
	go func() {
		// TODO find out how to exit gracefully
		// Sometime the RPC server keeps running usings lots of CPU.

		// EDIT: We can also remove this because we kill the process when using exec.CommandContext
		<-ctx.Done()
		logger.Info("closing rpc client")
		if err := rpcClient.Close(); err != nil {
			logger.Error("failed to close rpc client", "err", err.Error())
		}
		client.Kill()
	}()

	mErr := &multierror.Error{}
	rawD, errD := rpcClient.Dispense("destination")
	if errD != nil {
		mErr = multierror.Append(mErr, errD)
	}
	rawS, errS := rpcClient.Dispense("source")
	if errS != nil {
		mErr = multierror.Append(mErr, errS)
	}
	if mErr.Len() == 2 {
		rpcClient.Close()
		client.Kill()
		return nil, nil, fmt.Errorf("failed to dispense any rpc client: %w", mErr)
	}
	return rawD.(Destination), rawS.(Source), mErr.ErrorOrNil()
}

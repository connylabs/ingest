package plugin

import (
	"context"
	"os"

	"github.com/hashicorp/go-hclog"
	hplugin "github.com/hashicorp/go-plugin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const (
	// The Version needs to be changed, when there is a change in the plugin interface.
	// This will fail loading old plugins with new version of ingest and vice versa.
	// External plugins will need to update their ingest version and recompile.
	PluginMagicProtocalVersion = 2
	PluginCookieValue          = "d404b451-5a08-44eb-b705-15324b4ff720"
	PluginMagicCookieKey       = "INGEST_PLUGIN"
)

type Option func(c *configuration)

func WithLogger(l hclog.Logger) Option {
	return func(c *configuration) {
		c.l = l
	}
}

func WithGatherer(g prometheus.Gatherer) Option {
	return func(c *configuration) {
		c.g = g
	}
}

type configuration struct {
	g prometheus.Gatherer
	l hclog.Logger
}

func RunPluginServer(s Source, d Destination, opts ...Option) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := prometheus.NewRegistry()

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	c := &configuration{
		l: hclog.New(&hclog.LoggerOptions{
			Level:      hclog.Info,
			Output:     os.Stderr,
			JSONFormat: true,
		}),
		g: reg,
	}

	for _, o := range opts {
		if o != nil {
			o(c)
		}
	}

	handshakeConfig := hplugin.HandshakeConfig{
		ProtocolVersion:  PluginMagicProtocalVersion,
		MagicCookieKey:   PluginMagicCookieKey,
		MagicCookieValue: PluginCookieValue,
	}

	pluginMap := map[string]hplugin.Plugin{
		"source": &pluginSource{
			impl: s,
			ctx:  ctx,
			g:    c.g,
			l:    c.l.With("component", "source"),
		},
		"destination": &pluginDestination{
			impl: d,
			ctx:  ctx,
			g:    c.g,
			l:    c.l.With("component", "destination"),
		},
	}

	hplugin.Serve(&hplugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
		Logger:          c.l,
	})
}

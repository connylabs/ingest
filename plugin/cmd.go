package plugin

import (
	"context"
	"os"

	"github.com/hashicorp/go-hclog"
	hplugin "github.com/hashicorp/go-plugin"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// The Version needs to be changed, when there is a change in the plugin interface.
	// This will fail loading old plugins with new version of ingest and vice versa.
	// External plugins will need to update their ingest version and recompile.
	PluginMagicProtocalVersion = 2
	PluginCookieValue          = "d404b451-5a08-44eb-b705-15324b4ff720"
	PluginMagicCookieKey       = "INGEST_PLUGIN"
)

func RunPluginServer(s Source, d Destination, collectors ...prometheus.Collector) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handshakeConfig := hplugin.HandshakeConfig{
		ProtocolVersion:  PluginMagicProtocalVersion,
		MagicCookieKey:   PluginMagicCookieKey,
		MagicCookieValue: PluginCookieValue,
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.Trace,
		Output:     os.Stderr,
		JSONFormat: true,
	})

	pluginMap := map[string]hplugin.Plugin{
		"source": &pluginSource{
			impl: s,
			ctx:  ctx,
			cs:   collectors,
			l:    logger.With("component", "source"),
		},
		"destination": &pluginDestination{
			impl: d,
			ctx:  ctx,
			cs:   collectors,
			l:    logger.With("component", "destination"),
		},
	}

	hplugin.Serve(&hplugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
		Logger:          logger,
	})
}

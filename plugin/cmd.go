package plugin

import (
	"os"

	"github.com/hashicorp/go-hclog"
	hplugin "github.com/hashicorp/go-plugin"
)

const (
	// The Version needs to be changed, when there is a change in the plugin interface.
	// This will fail loading old plugins with new version of ingest and vice versa.
	// External plugins will need to update their ingest version and recompile.
	PluginMagicProtocalVersion = 1
	PluginCookieValue          = "d404b451-5a08-44eb-b705-15324b4ff720"
	PluginMagicCookieKey       = "INGEST_PLUGIN"
)

func RunPluginServer(s Source, d Destination) {
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
		"source": &PluginSource{
			Impl: s,
		},
		"destination": &PluginDestination{
			Impl: d,
		},
	}

	hplugin.Serve(&hplugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
		Logger:          logger,
	})
}

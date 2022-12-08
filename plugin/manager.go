package plugin

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-multierror"
	hplugin "github.com/hashicorp/go-plugin"
)

// PluginManager can start new plugins watch and kill all plugins.
type PluginManager struct {
	Interval time.Duration

	sources     []withClient[Source]
	destination []withClient[Destination]
	m           sync.Mutex
}

// NewDestination returns a new Destination interface from a plugin path and configuration.
func (pm *PluginManager) NewDestination(path string, config map[string]any) (Destination, error) {
	pm.m.Lock()
	defer pm.m.Unlock()

	rc, err := client(path)
	if err != nil {
		return nil, err
	}
	d, err := newDestination(rc)
	if err != nil {
		return nil, err
	}

	if err := d.Configure(config); err != nil {
		return nil, fmt.Errorf("failed to configure destination: %w", err)
	}

	pm.destination = append(pm.destination, withClient[Destination]{t: d, c: rc, path: path, config: config})

	return d, nil
}

// NewSource returns a new Source interface from a plugin path and configuration.
func (pm *PluginManager) NewSource(path string, config map[string]any) (Source, error) {
	pm.m.Lock()
	defer pm.m.Unlock()

	rc, err := client(path)
	if err != nil {
		return nil, err
	}
	s, err := newSource(rc)
	if err != nil {
		return nil, err
	}
	if err := s.Configure(config); err != nil {
		return nil, fmt.Errorf("failed to configure source: %w", err)
	}
	pm.sources = append(pm.sources, withClient[Source]{t: s, c: rc, path: path, config: config})
	return s, nil
}

// Stop will block until all rpc clients are closed.
// After Stop was called the PluginManager should not be used anymore.
func (pm *PluginManager) Stop() {
	pm.m.Lock()
	defer pm.m.Unlock()

	hplugin.CleanupClients()
	pm.destination = nil
	pm.sources = nil
}

// Watch will return an error when a plugin can not be pinged anymore or return when ctx is done.
func (pm *PluginManager) Watch(ctx context.Context) error {
	t := time.NewTicker(pm.Interval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			for _, s := range pm.sources {
				err := s.c.Ping()
				if err != nil {
					return fmt.Errorf("failed to ping source: %w", err)
				}

			}
			for _, d := range pm.destination {
				err := d.c.Ping()
				if err != nil {
					return fmt.Errorf("failed to ping destination: %w", err)
				}

			}
		}
	}
}

func client(path string) (hplugin.ClientProtocol, error) {
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
		Logger:          logger.With("path", path),
		Managed:         true,
		AutoMTLS:        true,
	})

	rpcClient, err := client.Client()
	if err != nil {
		client.Kill()
		return nil, fmt.Errorf("failed to create rpc client interface: %w", err)
	}
	return rpcClient, nil
}

type withClient[T any] struct {
	t      T
	path   string
	c      hplugin.ClientProtocol
	config map[string]any
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

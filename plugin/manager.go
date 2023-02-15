package plugin

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-multierror"
	hplugin "github.com/hashicorp/go-plugin"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func NewPluginManager(i time.Duration, l log.Logger) *PluginManager {
	if l == nil {
		l = log.NewNopLogger()
	}

	return &PluginManager{
		Interval: i,
		l:        l,
	}
}

// PluginManager can start new plugins watch and kill all plugins.
type PluginManager struct {
	Interval time.Duration

	sources     []withClient[Source]
	destination []withClient[Destination]
	l           log.Logger
	m           sync.Mutex
}

func ptr[T any](t T) *T {
	return &t
}

func (pm *PluginManager) Gather() ([]*dto.MetricFamily, error) {
	g := multierror.Group{}
	pm.m.Lock()
	defer pm.m.Unlock()

	all := make([][]*dto.MetricFamily, len(pm.sources)+len(pm.destination))
	for i := range pm.sources {
		i := i
		g.Go(func() error {
			g, ok := pm.sources[i].t.(prometheus.Gatherer)
			if !ok {
				return errors.New("failed to cast")
			}
			mfs, err := g.Gather()
			if err != nil {
				return err
			}
			for _, mf := range mfs {
				for _, m := range mf.Metric {
					lps := []*dto.LabelPair{{Name: ptr("component"), Value: ptr("source")}}
					for k, v := range pm.sources[i].labels {
						lps = append(lps, &dto.LabelPair{Name: ptr(k), Value: ptr(v)})
					}
					m.Label = append(m.Label, lps...)
				}
			}
			all[i] = mfs
			return nil
		})
	}
	for i := range pm.destination {
		i := i
		g.Go(func() error {
			g, ok := pm.destination[i].t.(prometheus.Gatherer)
			if !ok {
				return errors.New("failed to cast")
			}

			mfs, err := g.Gather()
			if err != nil {
				return err
			}
			for _, mf := range mfs {
				for _, m := range mf.Metric {
					lps := []*dto.LabelPair{{Name: ptr("component"), Value: ptr("destination")}}
					for k, v := range pm.sources[i].labels {
						lps = append(lps, &dto.LabelPair{Name: ptr(k), Value: ptr(v)})
					}
					m.Label = append(m.Label, lps...)
				}
			}
			all[i+len(pm.sources)] = mfs
			return nil
		})
	}

	if err := g.Wait().ErrorOrNil(); err != nil {
		return nil, err
	}

	merged := make([]*dto.MetricFamily, 0)
	for _, m := range all {
		merged = append(merged, m...)
	}
	return merged, g.Wait().ErrorOrNil()
}

// NewDestination returns a new Destination interface from a plugin path and configuration.
func (pm *PluginManager) NewDestination(path string, config map[string]any, labels prometheus.Labels) (Destination, error) {
	pm.m.Lock()
	defer pm.m.Unlock()

	c := client(path)
	cp, err := c.Client()
	if err != nil {
		c.Kill()
		return nil, fmt.Errorf("failed to create rpc client interface: %w", err)
	}
	d, err := newDestination(cp)
	if err != nil {
		c.Kill()
		return nil, err
	}

	if err := d.Configure(config); err != nil {
		c.Kill()
		return nil, fmt.Errorf("failed to configure destination: %w", err)
	}

	pm.destination = append(pm.destination, withClient[Destination]{t: d, c: c, path: path, config: config, labels: labels})

	return d, nil
}

// NewSource returns a new Source interface from a plugin path and configuration.
func (pm *PluginManager) NewSource(path string, config map[string]any, labels prometheus.Labels) (Source, error) {
	pm.m.Lock()
	defer pm.m.Unlock()

	c := client(path)
	cp, err := c.Client()
	if err != nil {
		c.Kill()
		return nil, fmt.Errorf("failed to create rpc client interface: %w", err)
	}
	s, err := newSource(cp)
	if err != nil {
		c.Kill()
		return nil, err
	}
	if err := s.Configure(config); err != nil {
		c.Kill()
		return nil, fmt.Errorf("failed to configure source: %w", err)
	}
	pm.sources = append(pm.sources, withClient[Source]{t: s, c: c, path: path, config: config, labels: labels})
	return s, nil
}

// Stop will block until all rpc clients are closed.
// After Stop was called all currently managed plugins cannot be used anymore.
func (pm *PluginManager) Stop() {
	pm.m.Lock()
	defer pm.m.Unlock()

	g := &multierror.Group{}
	f := func(c *hplugin.Client) func() error {
		return func() error {
			c.Kill()
			return nil
		}
	}
	for _, c := range pm.sources {
		g.Go(f(c.c))
	}
	for _, c := range pm.destination {
		g.Go(f(c.c))
	}
	if err := g.Wait().ErrorOrNil(); err != nil {
		// We can panic here because none of the go routines in the group return errors.
		panic(err)
	}
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
		case start := <-t.C:
			g := multierror.Group{}
			for i := range pm.sources {
				i := i
				g.Go(func() error {
					cp, err := pm.sources[i].c.Client()
					if err != nil {
						return fmt.Errorf("source client not initialized: %w", err)
					}
					if err := cp.Ping(); err != nil {
						return fmt.Errorf("failed to ping source: %w", err)
					}
					return nil
				})
			}
			for i := range pm.destination {
				i := i
				g.Go(func() error {
					cp, err := pm.destination[i].c.Client()
					if err != nil {
						return fmt.Errorf("destination client not initialized: %w", err)
					}
					if err := cp.Ping(); err != nil {
						return fmt.Errorf("failed to ping destination: %w", err)
					}
					return nil
				})
			}

			level.Debug(pm.l).Log("msg", "successfully pinged all plugins", "duration", time.Since(start), "source plugins", len(pm.sources), "destination plugins", len(pm.destination))

			err := g.Wait().ErrorOrNil()
			if err != nil {
				return err
			}

		}
	}
}

func client(path string) *hplugin.Client {
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

	return hplugin.NewClient(&hplugin.ClientConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
		Cmd:             exec.Command(path),
		Logger:          logger.With("path", path),
		AutoMTLS:        true,
		Managed:         true,
	})
}

type withClient[T any] struct {
	t      T
	path   string
	c      *hplugin.Client
	config map[string]any
	labels prometheus.Labels
}

func newDestination(cp hplugin.ClientProtocol) (Destination, error) {
	raw, err := cp.Dispense("destination")
	if err != nil {
		return nil, fmt.Errorf("failed to dispense destination: %w", err)
	}
	return raw.(Destination), nil
}

func newSource(cp hplugin.ClientProtocol) (Source, error) {
	raw, err := cp.Dispense("source")
	if err != nil {
		return nil, fmt.Errorf("failed to dispense source: %w", err)
	}
	return raw.(Source), nil
}

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
	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-multierror"
	hplugin "github.com/hashicorp/go-plugin"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	ps "github.com/prometheus/pushgateway/storage"
)

func NewPluginManager(i time.Duration, l log.Logger) *PluginManager {
	return &PluginManager{
		Interval: i,
		ms:       ps.NewDiskMetricStore("", 0, nil, l),
	}
}

// PluginManager can start new plugins watch and kill all plugins.
type PluginManager struct {
	Interval time.Duration

	ms ps.MetricStore

	sources     []withClient[Source]
	destination []withClient[Destination]
	m           sync.Mutex
}

func (pm *PluginManager) Gather() ([]*dto.MetricFamily, error) {
	return pm.ms.GetMetricFamilies(), pm.ms.Healthy()
}

func (pm *PluginManager) GatherMetrics(ctx context.Context) error {
	g := multierror.Group{}
	pm.m.Lock()
	defer pm.m.Unlock()

	for i := range pm.sources {
		i := i
		g.Go(func() error {
			h := make(map[string]*dto.MetricFamily)
			g, ok := pm.sources[i].t.(prometheus.Gatherer)
			if !ok {
				return errors.New("failed to cast")
			}
			mfs, err := g.Gather()
			if err != nil {
				return err
			}
			ts := time.Now()
			for _, mf := range mfs {
				h[mf.GetName()] = mf
			}
			if err := pm.ms.Ready(); err != nil {
				return fmt.Errorf("metrics store not ready: %w", err)
			}
			if pm.sources[i].labels == nil {
				pm.sources[i].labels = prometheus.Labels{}
			}
			pm.sources[i].labels["instance"] = "plugin"
			pm.sources[i].labels["component"] = "source"
			pm.ms.SubmitWriteRequest(ps.WriteRequest{
				Labels:         pm.sources[i].labels,
				Timestamp:      ts,
				MetricFamilies: h,
			})
			return nil
		})
	}
	for i := range pm.destination {
		i := i
		g.Go(func() error {
			h := make(map[string]*dto.MetricFamily)
			g, ok := pm.destination[i].t.(prometheus.Gatherer)
			if !ok {
				return errors.New("failed to cast")
			}
			mfs, err := g.Gather()
			if err != nil {
				return err
			}
			ts := time.Now()
			for _, mf := range mfs {
				h[mf.GetName()] = mf
			}
			if err := pm.ms.Ready(); err != nil {
				return fmt.Errorf("metrics store not ready: %w", err)
			}
			if pm.destination[i].labels == nil {
				pm.destination[i].labels = prometheus.Labels{}
			}
			pm.destination[i].labels["instance"] = "plugin"
			pm.destination[i].labels["component"] = "destination"
			pm.ms.SubmitWriteRequest(ps.WriteRequest{
				Labels:         pm.destination[i].labels,
				Timestamp:      ts,
				MetricFamilies: h,
			})
			return nil
		})
	}
	return g.Wait().ErrorOrNil()
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

	_ = pm.ms.Shutdown()

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
		case <-t.C:
			for _, s := range pm.sources {
				cp, err := s.c.Client()
				if err != nil {
					return fmt.Errorf("source client not initialized: %w", err)
				}
				if err := cp.Ping(); err != nil {
					return fmt.Errorf("failed to ping source: %w", err)
				}
			}
			for _, d := range pm.destination {
				cp, err := d.c.Client()
				if err != nil {
					return fmt.Errorf("destination client not initialized: %w", err)
				}
				if err := cp.Ping(); err != nil {
					return fmt.Errorf("failed to ping destination: %w", err)
				}
			}
			// TODO: put somewhere else?
			if err := pm.GatherMetrics(ctx); err != nil {
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

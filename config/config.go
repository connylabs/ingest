package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ghodss/yaml"
	"github.com/mitchellh/mapstructure"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/plugin"
)

var defaultInterval = Duration(5 * time.Minute)

// NewFromPath creates a new Config from the given file path.
func NewFromPath(path string) (*Config, error) {
	f, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read tenant configuration file from path %q: %w", path, err)
	}
	return New(f)
}

// New creates a new Config from the given file content.
func New(buf []byte) (*Config, error) {
	c := new(Config)

	if err := yaml.Unmarshal(buf, c); err != nil {
		return nil, fmt.Errorf("unable to read configuration YAML: %w", err)
	}
	return c, nil
}

// Source is used to configure source plugins in the ingest configuration.
type Source struct {
	Name   string
	Type   string
	Config map[string]interface{} `json:"-" mapstructure:",remain"`
}

// UnmarshalJSON allows the source configuration to collect all unknown fields into the `Config` field.
func (s *Source) UnmarshalJSON(b []byte) error {
	raw := make(map[string]interface{})
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	return mapstructure.Decode(raw, s)
}

// Destination is used to configure destination plugins in the ingest configuration.
type Destination struct {
	Name   string
	Type   string
	Config map[string]interface{} `json:"-" mapstructure:",remain"`
}

// UnmarshalJSON allows the destination configuration to collect all unknown fields into the `Config` field.
func (d *Destination) UnmarshalJSON(b []byte) error {
	raw := make(map[string]interface{})
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	return mapstructure.Decode(raw, d)
}

// Workflow is used to configure ingestion pipelines between sources and destinations in the ingest configuration.
type Workflow struct {
	Name         string
	Source       string
	Destinations []string
	CleanUp      bool
	Interval     *Duration
	Concurrency  int
	BatchSize    int
	Webhook      string
}

// Config represents a configuration of sources, workflows and destinations.
type Config struct {
	Version      string
	Sources      []Source
	Destinations []Destination
	Workflows    []Workflow
}

// ConfigurePlugins configures the plugins found in path.
func (c *Config) ConfigurePlugins(ctx context.Context, paths []string) (map[string]plugin.Source, map[string]plugin.Destination, error) {
	// Collect all of the named pluginPaths.
	pluginPaths := make(map[string]string)
	sources := make(map[string]plugin.Source)
	destinations := make(map[string]plugin.Destination)
	pluginNames := make(map[string]struct{})
	sourceNames := make(map[string]struct{})
	destinationNames := make(map[string]struct{})
	workflowNames := make(map[string]struct{})
	// Validate the sources.
	for _, s := range c.Sources {
		pluginNames[s.Type] = struct{}{}
		if _, ok := sourceNames[s.Name]; ok {
			return nil, nil, fmt.Errorf("found duplicate source %q", s.Name)
		}
		sourceNames[s.Name] = struct{}{}
	}
	// Validate the destinations.
	for _, d := range c.Destinations {
		pluginNames[d.Type] = struct{}{}
		if _, ok := destinationNames[d.Name]; ok {
			return nil, nil, fmt.Errorf("found duplicate destination %q", d.Name)
		}
		destinationNames[d.Name] = struct{}{}
	}
	// Validate the workflows.
	for i, w := range c.Workflows {
		if _, ok := workflowNames[w.Name]; ok {
			return nil, nil, fmt.Errorf("found duplicate workflow %q", w.Name)
		}
		workflowNames[w.Name] = struct{}{}
		if _, ok := sourceNames[w.Source]; !ok {
			return nil, nil, fmt.Errorf("workflow %q references non-existent source %q", w.Name, w.Source)
		}
		for _, d := range w.Destinations {
			if _, ok := destinationNames[d]; !ok {
				return nil, nil, fmt.Errorf("workflow %q references non-existent destination %q", w.Name, d)
			}
		}
		if w.Interval == nil {
			c.Workflows[i].Interval = &defaultInterval
		}
		if w.BatchSize == 0 {
			c.Workflows[i].BatchSize = ingest.DefaultBatchSize
		}
		if w.Concurrency == 0 {
			c.Workflows[i].Concurrency = c.Workflows[i].BatchSize
		}
	}
	// Instantiate the plugins.
	for pn := range pluginNames {
		pp, err := firstPath(paths, pn)
		if err != nil {
			return nil, nil, fmt.Errorf("none of the given paths contains the filename %s: %w", pn, err)
		}
		pluginPaths[pn] = pp
	}
	// Instantiate the sources.
	for i := range c.Sources {
		// Of course we could use the parent context and only kill the failed rpc servers when exiting the main process to avoid the context cancel warnning.
		ctx, cancel := context.WithCancel(ctx) //nolint:govet

		_, s, err := plugin.NewPlugin(ctx, pluginPaths[c.Sources[i].Type])
		if s == nil {
			cancel()
			return nil, nil, fmt.Errorf("cannot instantiate source %q: %w", c.Sources[i].Name, err)
		}

		if err := s.Configure(c.Sources[i].Config); err != nil {
			cancel()
			return nil, nil, fmt.Errorf("failed to configure source %q: %w", c.Sources[i].Name, err)
		}
		sources[c.Sources[i].Name] = &SourceTyper{s, c.Sources[i].Type}
	}
	// Instantiate the destinations.
	for i := range c.Destinations {
		// Of course we could use the parent context and only kill the failed rpc servers when exiting the main process to avoid the context cancel warnning.
		ctx, cancel := context.WithCancel(ctx) //nolint:govet
		d, _, err := plugin.NewPlugin(ctx, pluginPaths[c.Destinations[i].Type])
		if d == nil {
			cancel()
			return nil, nil, fmt.Errorf("cannot instantiate destination %q: %w", c.Destinations[i].Name, err) //nolint:govet
		}

		if err := d.Configure(c.Destinations[i].Config); err != nil {
			cancel()
			return nil, nil, fmt.Errorf("failed to configure destination %q: %w", c.Destinations[i].Name, err)
		}
		destinations[c.Destinations[i].Name] = &DestinationTyper{d, c.Destinations[i].Type}

	}
	return sources, destinations, nil //nolint:govet
}

func firstPath(paths []string, filename string) (string, error) {
	for _, p := range paths {
		fpath := filepath.Join(p, filename)
		if _, err := os.Stat(fpath); errors.Is(err, syscall.ENOENT) {
			continue
		} else if err != nil {
			return "", err
		}
		return fpath, nil
	}
	return "", os.ErrNotExist
}

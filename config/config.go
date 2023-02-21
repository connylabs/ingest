package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ghodss/yaml"
	"github.com/mitchellh/mapstructure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/plugin"
)

var defaultInterval = Duration(5 * time.Minute)

// NewFromPath creates a new Config from the given file path.
func NewFromPath(path string, r prometheus.Registerer) (*Config, error) {
	f, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read tenant configuration file from path %q: %w", path, err)
	}
	return New(f, r)
}

// New creates a new Config from the given file content.
func New(buf []byte, r prometheus.Registerer) (*Config, error) {
	c := new(Config)

	if err := yaml.Unmarshal(buf, c); err != nil {
		return nil, fmt.Errorf("unable to read configuration YAML: %w", err)
	}
	c.workflowInstantiationFailuresTotal = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "ingest_workflow_instantiation_failures_total",
		Help: "Number of failures while instantiating workflows.",
	})

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

	workflowInstantiationFailuresTotal prometheus.Counter
}

// ConfigurePlugins configures the plugins found in path.
func (c *Config) ConfigurePlugins(pm *plugin.PluginManager, paths []string, strict bool) (map[string]plugin.Source, map[string]plugin.Destination, error) {
	// Collect all of the named pluginPaths.
	pluginPaths := make(map[string]string)
	sources := make(map[string]plugin.Source)
	destinations := make(map[string]plugin.Destination)
	pluginNames := make(map[string]struct{})
	sourceNames := make(map[string]int)
	destinationNames := make(map[string]int)
	workflowNames := make(map[string]struct{})
	// Validate the sources.
	for i, s := range c.Sources {
		pluginNames[s.Type] = struct{}{}
		if _, ok := sourceNames[s.Name]; ok {
			return nil, nil, fmt.Errorf("found duplicate source %q", s.Name)
		}
		sourceNames[s.Name] = i
	}
	// Validate the destinations.
	for i, d := range c.Destinations {
		pluginNames[d.Type] = struct{}{}
		if _, ok := destinationNames[d.Name]; ok {
			return nil, nil, fmt.Errorf("found duplicate destination %q", d.Name)
		}
		destinationNames[d.Name] = i
	}
	// Find plugin paths
	for pn := range pluginNames {
		pp, err := firstPath(paths, pn)
		if err != nil {
			return nil, nil, fmt.Errorf("none of the given paths contains the filename %s: %w", pn, err)
		}
		pluginPaths[pn] = pp
	}
	i := 0
	// Validate the workflows.
workflow:
	for _, w := range c.Workflows {
		if _, ok := workflowNames[w.Name]; ok {
			return nil, nil, fmt.Errorf("found duplicate workflow %q", w.Name)
		}
		if _, ok := sourceNames[w.Source]; !ok {
			if strict {
				return nil, nil, fmt.Errorf("workflow %q references non-existent source %q", w.Name, w.Source)
			}
			c.workflowInstantiationFailuresTotal.Inc()
			continue
		}
		// Instantiate the source.
		// Ensure a source is only instantiated once.
		if _, ok := sources[w.Source]; !ok {
			s, err := pm.NewSource(
				pluginPaths[c.Sources[sourceNames[w.Source]].Type],
				c.Sources[sourceNames[w.Source]].Config,
				prometheus.Labels{
					"workflow": w.Name,
					"type":     c.Sources[sourceNames[w.Source]].Type,
					"name":     c.Sources[sourceNames[w.Source]].Name,
				})
			if err != nil {
				if strict {
					return nil, nil, fmt.Errorf("cannot instantiate source %q: %w", w.Source, err)
				}
				c.workflowInstantiationFailuresTotal.Inc()
				continue
			}
			sources[w.Source] = &SourceTyper{s, c.Sources[sourceNames[w.Source]].Type}
		}

		for _, d := range w.Destinations {
			if _, ok := destinationNames[d]; !ok {
				if strict {
					return nil, nil, fmt.Errorf("workflow %q references non-existent destination %q", w.Name, d)
				}
				c.workflowInstantiationFailuresTotal.Inc()
				continue workflow
			}
			// Instantiate the destinations.
			// Ensure a destination is only instantiated once.
			if _, ok := destinations[d]; !ok {
				if _, ok := destinations[d]; !ok {
					dd, err := pm.NewDestination(
						pluginPaths[c.Destinations[destinationNames[d]].Type],
						c.Destinations[destinationNames[d]].Config,
						prometheus.Labels{
							"workflow": w.Name,
							"type":     c.Destinations[destinationNames[d]].Type,
							"name":     c.Destinations[destinationNames[d]].Name,
						})
					if err != nil {
						if strict {
							return nil, nil, fmt.Errorf("cannot instantiate destination %q: %w", d, err)
						}
						c.workflowInstantiationFailuresTotal.Inc()
						continue workflow
					}
					destinations[d] = &DestinationTyper{dd, c.Destinations[i].Type}
				}
			}
		}
		if w.Interval == nil {
			w.Interval = &defaultInterval
		}
		if w.BatchSize == 0 {
			w.BatchSize = ingest.DefaultBatchSize
		}
		if w.Concurrency == 0 {
			w.Concurrency = w.BatchSize
		}
		c.Workflows[i] = w
		workflowNames[w.Name] = struct{}{}
		i++
	}
	// Clean up unused workflows.
	for j := i; j < len(c.Workflows); j++ {
		c.Workflows[j] = Workflow{}
	}
	c.Workflows = c.Workflows[:i]

	return sources, destinations, nil
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

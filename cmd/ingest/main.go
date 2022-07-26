package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	stdlog "log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	stdplugin "plugin"
	"strings"
	"syscall"
	"time"

	"github.com/ghodss/yaml"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/metalmatze/signal/healthcheck"
	"github.com/metalmatze/signal/internalserver"
	"github.com/mitchellh/mapstructure"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/cmd"
	"github.com/connylabs/ingest/dequeue"
	"github.com/connylabs/ingest/enqueue"
	"github.com/connylabs/ingest/plugin"
	"github.com/connylabs/ingest/queue"
	"github.com/connylabs/ingest/storage"
	"github.com/connylabs/ingest/version"
)

const (
	logLevelAll   = "all"
	logLevelDebug = "debug"
	logLevelInfo  = "info"
	logLevelWarn  = "warn"
	logLevelError = "error"
	logLevelNone  = "none"
)

var availableLogLevels = strings.Join([]string{
	logLevelAll,
	logLevelDebug,
	logLevelInfo,
	logLevelWarn,
	logLevelError,
	logLevelNone,
}, ", ")

const (
	dequeueMode = "dequeue"
	enqueueMode = "enqueue"
)

var availableModes = strings.Join([]string{
	dequeueMode,
	enqueueMode,
}, ", ")

var defaultInterval = 5 * time.Minute

func main() {
	if err := Main(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

type flags struct {
	listenInternal  *string
	queueEndpoint   *string
	streamName      *string
	queueSubject    *string
	consumerName    *string
	printVersion    *bool
	logLevel        *string
	mode            *string
	help            *bool
	pluginDirectory *string
	configPath      *string
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
	Interval     *time.Duration
	BatchSize    int
	Webhook      string
}

type config struct {
	Version      string
	Sources      []Source
	Destinations []Destination
	Workflows    []Workflow
}

// Main is a convenience function that serves as a main that can return an error.
func Main() error {
	hd, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to find home directory: %w", err)
	}

	appFlags := &flags{
		listenInternal:  flag.String("listen", ":9090", "The address at which to listen for health and metrics"),
		queueEndpoint:   flag.String("queue-endpoint", "nats://localhost:4222", "The queue endpoint to which to connect"),
		streamName:      flag.String("stream-name", "ingest", "The stream name to which to connect"),
		queueSubject:    flag.String("queue-subject", "ingest", "The queue name to which to connect"),
		consumerName:    flag.String("consumer-name", "ingest", "The consumer name to which to connect"),
		printVersion:    flag.Bool("version", false, "Show version"),
		logLevel:        flag.String("log-level", logLevelInfo, fmt.Sprintf("Log level to use. Possible values: %s", availableLogLevels)),
		mode:            flag.String("mode", "", fmt.Sprintf("Mode of the service. Possible values: %s", availableModes)),
		help:            flag.Bool("h", false, "Show usage"),
		pluginDirectory: flag.String("plugins", filepath.Join(hd, ".config/ingest/plugins"), "The directory in which to look for plugins"),
		configPath:      flag.String("config", filepath.Join(hd, "ingest", "config"), "The path to the configuration file for ingest"),
	}

	flag.Parse()

	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
	switch *appFlags.logLevel {
	case logLevelAll:
		logger = level.NewFilter(logger, level.AllowAll())
	case logLevelDebug:
		logger = level.NewFilter(logger, level.AllowDebug())
	case logLevelInfo:
		logger = level.NewFilter(logger, level.AllowInfo())
	case logLevelWarn:
		logger = level.NewFilter(logger, level.AllowWarn())
	case logLevelError:
		logger = level.NewFilter(logger, level.AllowError())
	case logLevelNone:
		logger = level.NewFilter(logger, level.AllowNone())
	default:
		return fmt.Errorf("log level %v unknown; possible values are: %s", *appFlags.logLevel, availableLogLevels)
	}
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	logger = log.With(logger, "caller", log.DefaultCaller)
	stdlog.SetOutput(log.NewStdlibAdapter(logger))

	if *appFlags.help {
		flag.Usage()
		return nil
	}

	if *appFlags.printVersion {
		fmt.Println(version.Version)
		return nil
	}

	c := new(config)
	f, err := ioutil.ReadFile(*appFlags.configPath)
	if err != nil {
		return fmt.Errorf("cannot read tenant configuration file from path %q: %w", *appFlags.configPath, err)
	}

	if err := yaml.Unmarshal(f, c); err != nil {
		return fmt.Errorf("unable to read configuration YAML: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sources, destinations, err := configurePlugins(ctx, *appFlags.pluginDirectory, c)
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	q, err := queue.New(*appFlags.queueEndpoint, reg)
	if err != nil {
		return fmt.Errorf("failed to instantiate queue: %w", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		if err := q.Close(ctx); err != nil {
			level.Error(logger).Log("msg", "failed to close queue", "err", err.Error())
		}
	}()

	var g run.Group
	for _, w := range c.Workflows {
		logger = log.With(logger, "workflow", w.Name)
		switch *appFlags.mode {
		case enqueueMode:
			logger = log.With(logger, "mode", enqueueMode)
			logger = log.With(logger, "source", w.Source)
			qc, err := enqueue.New(sources[w.Source], strings.Join([]string{*appFlags.queueSubject, w.Name}, "."), q, reg, logger)
			if err != nil {
				return fmt.Errorf("failed to connect to the queue: %v", err)
			}
			g.Add(
				cmd.NewEnqueuerRunner(ctx, qc, *w.Interval, logger),
				func(error) {
					cancel()
				},
			)
		case dequeueMode:
			logger := log.With(logger, "mode", dequeueMode)
			for _, d := range w.Destinations {
				logger = log.With(logger, "destination", d)
				s := storage.NewInstrumentedStorage(destinations[d], reg)
				d := dequeue.New(w.Webhook, sources[w.Source], s, q,
					*appFlags.streamName,
					*appFlags.consumerName,
					strings.Join([]string{*appFlags.queueSubject, w.Name}, "."),
					w.BatchSize,
					w.CleanUp,
					logger,
					reg,
				)
				g.Add(
					cmd.NewDequeuerRunner(ctx, d, logger),
					func(error) {
						cancel()
					},
				)
			}
		default:
			flag.Usage()
			return fmt.Errorf("unsupported mode %q", *appFlags.mode)
		}
	}

	{
		// Run the internal HTTP server.
		logger := log.With(logger, "component", "internal-server")
		healthchecks := healthcheck.NewMetricsHandler(healthcheck.NewHandler(), reg)
		h := internalserver.NewHandler(
			internalserver.WithName("Internal - ingest"),
			internalserver.WithHealthchecks(healthchecks),
			internalserver.WithPrometheusRegistry(reg),
			internalserver.WithPProf(),
		)
		l, err := net.Listen("tcp", *appFlags.listenInternal)
		if err != nil {
			return fmt.Errorf("failed to listen on %s: %v", *appFlags.listenInternal, err)
		}

		g.Add(func() error {
			level.Info(logger).Log("msg", fmt.Sprintf("starting the ingest internal HTTP server at %s", *appFlags.listenInternal))

			if err := http.Serve(l, h); err != nil && err != http.ErrServerClosed {
				return fmt.Errorf("error: internal server exited unexpectedly: %v", err)
			}
			return nil
		}, func(error) {
			l.Close()
		})
	}

	{
		// Exit gracefully on SIGINT and SIGTERM.
		term := make(chan os.Signal, 1)
		signal.Notify(term, syscall.SIGINT, syscall.SIGTERM)
		cancel := make(chan struct{})
		g.Add(func() error {
			for {
				select {
				case <-term:
					level.Info(logger).Log("msg", "caught interrupt; gracefully cleaning up; see you next time!")
					return nil
				case <-cancel:
					return nil
				}
			}
		}, func(error) {
			close(cancel)
		})
	}

	return g.Run()
}

func configurePlugins(ctx context.Context, path string, c *config) (map[string]plugin.Source, map[string]plugin.Destination, error) {
	// Collect all of the named plugins.
	plugins := make(map[string]plugin.Plugin)
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
	}
	// Instantiate the plugins.
	for pn := range pluginNames {
		raw, err := stdplugin.Open(filepath.Join(path, pn))
		if err != nil {
			return nil, nil, fmt.Errorf("could not open plugin %q: %w", pn, err)
		}
		r, err := raw.Lookup("Register")
		if err != nil {
			return nil, nil, fmt.Errorf("could not find symbol in plugin %q: %w", pn, err)
		}
		p, err := (*r.(*plugin.Register))()
		if err != nil {
			return nil, nil, fmt.Errorf("could not register plugin %q: %w", pn, err)
		}
		plugins[pn] = p
	}
	// Instantiate the sources.
	for i := range c.Sources {
		s, err := plugins[c.Sources[i].Type].NewSource(ctx, c.Sources[i].Config)
		if errors.Is(err, plugin.ErrNotImplemented) {
			return nil, nil, fmt.Errorf("cannot instantiate source %q: plugin %q does not support acting as a source", c.Sources[i].Name, c.Sources[i].Type)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("cannot instantiate source %q: %w", c.Sources[i].Name, err)
		}
		sources[c.Sources[i].Name] = s
	}
	// Instantiate the destinations.
	for i := range c.Destinations {
		d, err := plugins[c.Destinations[i].Type].NewDestination(ctx, c.Destinations[i].Config)
		if errors.Is(err, plugin.ErrNotImplemented) {
			return nil, nil, fmt.Errorf("cannot instantiate destination %q: plugin %q does not support acting as a destination", c.Destinations[i].Name, c.Destinations[i].Type)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("cannot instantiate destination %q: %w", c.Destinations[i].Name, err)
		}
		destinations[c.Destinations[i].Name] = d
	}
	return sources, destinations, nil
}

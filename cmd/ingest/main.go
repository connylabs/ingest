package main

import (
	"context"
	"fmt"
	stdlog "log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/metalmatze/signal/healthcheck"
	"github.com/metalmatze/signal/internalserver"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	flag "github.com/spf13/pflag"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/cmd"
	"github.com/connylabs/ingest/config"
	"github.com/connylabs/ingest/dequeue"
	"github.com/connylabs/ingest/enqueue"
	"github.com/connylabs/ingest/plugin"
	"github.com/connylabs/ingest/queue"
	"github.com/connylabs/ingest/storage"
	"github.com/connylabs/ingest/storage/multi"
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

	watchPluginInterval = 5 * time.Second
)

var availableModes = strings.Join([]string{
	dequeueMode,
	enqueueMode,
}, ", ")

func main() {
	if err := Main(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

type flags struct {
	listenInternal    *string
	queueEndpoint     *string
	replicas          *int
	stream            *string
	subject           *string
	consumer          *string
	maxMsgs           *int64
	printVersion      *bool
	logLevel          *string
	mode              *string
	help              *bool
	pluginDirectories *[]string
	configPath        *string
	dryRun            *bool
}

// Main is a convenience function that serves as a main that can return an error.
func Main() error {
	hd, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to find home directory: %w", err)
	}

	appFlags := &flags{
		listenInternal:    flag.String("listen", ":9090", "The address at which to listen for health and metrics"),
		queueEndpoint:     flag.String("queue-endpoint", "nats://localhost:4222", "The queue endpoint to which to connect"),
		replicas:          flag.Int("stream-replicas", 1, "The replicas of the NATS stream"),
		stream:            flag.String("stream", "ingest", "The stream name to which to connect"),
		subject:           flag.String("subject", "ingest", "The subject name to which to connect"),
		consumer:          flag.String("consumer", "ingest", "The prefix to use for dynamically created consumer names"),
		maxMsgs:           flag.Int64("max-msgs", 0, "The maximum amount of messages in the jet stream. Set to 0 to remove limit"),
		printVersion:      flag.Bool("version", false, "Show version"),
		logLevel:          flag.String("log-level", logLevelInfo, fmt.Sprintf("Log level to use. Possible values: %s", availableLogLevels)),
		mode:              flag.String("mode", "", fmt.Sprintf("Mode of the service. Possible values: %s", availableModes)),
		help:              flag.Bool("h", false, "Show usage"),
		pluginDirectories: flag.StringSlice("plugins", []string{filepath.Join(hd, ".config/ingest/plugins")}, "The directories in which to look for plugins. Directories are searched in the order specified with the first match taking precedence"),
		configPath:        flag.String("config", filepath.Join(hd, ".config/ingest/config"), "The path to the configuration file for ingest"),
		dryRun:            flag.Bool("dry-run", false, "Only load the configuration and exit without performing any copy operations"),
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
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	stdlog.SetOutput(log.NewStdlibAdapter(logger))

	if *appFlags.help {
		flag.Usage()
		return nil
	}

	if *appFlags.printVersion {
		fmt.Println(version.Version)
		return nil
	}

	c, err := config.NewFromPath(*appFlags.configPath)
	if err != nil {
		return fmt.Errorf("failed to create configuration: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	pm := &plugin.PluginManager{Interval: watchPluginInterval}
	sources, destinations, err := c.ConfigurePlugins(pm, *appFlags.pluginDirectories)
	if err != nil {
		return err
	}
	if *appFlags.dryRun {
		return nil
	}
	q, err := queue.New(*appFlags.queueEndpoint, *appFlags.stream, *appFlags.replicas, []string{strings.Join([]string{*appFlags.subject, "*"}, ".")}, *appFlags.maxMsgs, reg)
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
	if err := runGroup(ctx, &g, q, appFlags, sources, destinations, c.Workflows, logger, reg); err != nil {
		return err
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

		// Stop the application if a plugin has crashed.
		{
			ctx, cancel := context.WithCancel(ctx)
			g.Add(func() error {
				level.Info(logger).Log("msg", "start to watch plugins", "interval", watchPluginInterval.String())

				if err := pm.Watch(ctx); err != nil {
					return fmt.Errorf("plugin crash detected: %w", err)
				}
				return nil
			},
				func(error) {
					cancel()
					level.Info(logger).Log("msg", "stopping all plugins")
					pm.Stop()
				})
		}
	}

	// Exit gracefully on SIGINT and SIGTERM.
	g.Add(run.SignalHandler(ctx, syscall.SIGINT, syscall.SIGTERM))

	return g.Run()
}

func runGroup(ctx context.Context, g *run.Group, q ingest.Queue, appFlags *flags, sources map[string]plugin.Source, destinations map[string]plugin.Destination, workflows []config.Workflow, logger log.Logger, reg prometheus.Registerer) error {
	for _, w := range workflows {
		logger = log.With(logger, "workflow", w.Name)
		reg := prometheus.WrapRegistererWith(prometheus.Labels{
			"source":   w.Source,
			"workflow": w.Name,
		}, reg)
		switch *appFlags.mode {
		case enqueueMode:
			ctx, cancel := context.WithCancel(ctx)
			logger := log.With(logger, "mode", enqueueMode, "source", w.Source)
			qc, err := enqueue.New(sources[w.Source], strings.Join([]string{*appFlags.subject, w.Name}, "."), q, reg, logger)
			if err != nil {
				cancel()
				return fmt.Errorf("failed to connect to the queue: %v", err)
			}
			g.Add(
				cmd.NewEnqueuerRunner(ctx, qc, time.Duration(*w.Interval), logger),
				func(error) {
					cancel()
				},
			)
		case dequeueMode:
			logger := log.With(logger, "mode", dequeueMode)
			ss := make([]storage.Storage, 0, len(w.Destinations))
			for _, d := range w.Destinations {
				t := "unknown"
				if dt, ok := destinations[d].(config.DestinationTyper); ok {
					t = dt.Type()
				}
				reg := prometheus.WrapRegistererWith(prometheus.Labels{
					"destination": d,
					"plugin":      t,
				}, reg)
				ss = append(ss, storage.NewInstrumentedStorage(destinations[d], reg))
			}
			s := multi.NewMultiStorage(ss...)
			if len(ss) > 1 {
				s = storage.NewInstrumentedStorage(s, prometheus.WrapRegistererWith(prometheus.Labels{"destination": "multi", "plugin": "multi"}, reg))
			}
			d := dequeue.New(
				w.Webhook, sources[w.Source],
				s,
				q,
				*appFlags.stream,
				strings.Join([]string{*appFlags.consumer, w.Name}, "__"),
				strings.Join([]string{*appFlags.subject, w.Name}, "."),
				w.BatchSize,
				w.Concurrency,
				w.CleanUp,
				logger,
				reg,
			)
			ctx, cancel := context.WithCancel(ctx)
			g.Add(
				cmd.NewDequeuerRunner(ctx, d, logger),
				func(error) {
					cancel()
				},
			)

		default:
			flag.Usage()
			return fmt.Errorf("unsupported mode %q", *appFlags.mode)
		}
	}
	return nil
}

package main

import (
	"context"
	"flag"
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

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/cmd"
	"github.com/connylabs/ingest/config"
	"github.com/connylabs/ingest/dequeue"
	"github.com/connylabs/ingest/enqueue"
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

func main() {
	if err := Main(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

type flags struct {
	listenInternal  *string
	queueEndpoint   *string
	stream          *string
	subject         *string
	consumer        *string
	printVersion    *bool
	logLevel        *string
	mode            *string
	help            *bool
	pluginDirectory *string
	configPath      *string
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
		stream:          flag.String("stream", "ingest", "The stream name to which to connect"),
		subject:         flag.String("subject", "ingest", "The subject name to which to connect"),
		consumer:        flag.String("consumer", "ingest", "The prefix to use for dymanically created consumer names"),
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
	if err := runGroup(ctx, &g, q, appFlags, c, logger, reg); err != nil {
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
	}

	// Exit gracefully on SIGINT and SIGTERM.
	g.Add(run.SignalHandler(ctx, syscall.SIGINT, syscall.SIGTERM))

	return g.Run()
}

func runGroup(ctx context.Context, g *run.Group, q ingest.Queue, appFlags *flags, c *config.Config, logger log.Logger, reg prometheus.Registerer) error {
	sources, destinations, err := c.ConfigurePlugins(ctx, *appFlags.pluginDirectory)
	if err != nil {
		return err
	}

	for _, w := range c.Workflows {
		logger = log.With(logger, "workflow", w.Name)
		reg := prometheus.WrapRegistererWith(prometheus.Labels{"workflow": w.Name}, reg)
		switch *appFlags.mode {
		case enqueueMode:
			ctx, cancel := context.WithCancel(ctx)
			reg := prometheus.WrapRegistererWith(prometheus.Labels{"source": w.Source}, reg)
			logger := log.With(logger, "mode", enqueueMode, "source", w.Source)
			qc, err := enqueue.New(sources[w.Source], strings.Join([]string{*appFlags.subject, w.Name}, "."), q, reg, logger)
			if err != nil {
				cancel()
				return fmt.Errorf("failed to connect to the queue: %v", err)
			}
			g.Add(
				cmd.NewEnqueuerRunner(ctx, qc, *w.Interval, logger),
				func(err error) {
					// Do not cancel the enqueuer if other enqueuers exited cleanly.
					// Instead, let the enqueuers finish.
					if err != nil {
						cancel()
					}
				},
			)
		case dequeueMode:
			logger := log.With(logger, "mode", dequeueMode)
			var s []storage.Storage
			for _, d := range w.Destinations {
				reg := prometheus.WrapRegistererWith(prometheus.Labels{"destination": d}, reg)
				s = append(s, storage.NewInstrumentedStorage(destinations[d], reg))
			}
			d := dequeue.New(
				w.Webhook, sources[w.Source],
				storage.NewInstrumentedStorage(storage.NewMultiStorage(s...), prometheus.WrapRegistererWith(prometheus.Labels{"destination": "multi"}, reg)),
				q,
				*appFlags.stream,
				strings.Join([]string{*appFlags.consumer, w.Name}, "__"),
				strings.Join([]string{*appFlags.subject, w.Name}, "."),
				w.BatchSize,
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

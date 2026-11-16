package temporal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.temporal.io/sdk/activity"
	sdktemporal "go.temporal.io/sdk/temporal"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/dequeue"
	"github.com/connylabs/ingest/storage"
)

const (
	// ListItemsActivityName is the registered name of Activities.ListItems.
	ListItemsActivityName = "ListItems"
	// TransferActivityName is the registered name of Activities.Transfer.
	TransferActivityName = "Transfer"
	// CleanUpActivityName is the registered name of Activities.CleanUp.
	CleanUpActivityName = "CleanUp"
	// CallWebhookActivityName is the registered name of Activities.CallWebhook.
	CallWebhookActivityName = "CallWebhook"
)

// Source is a source plugin, which can list, download and clean up items.
type Source interface {
	ingest.Nexter
	ingest.Client
}

// Workflow is an ingest workflow that runs on Temporal.
type Workflow struct {
	Name           string
	SourceName     string
	Source         Source
	Storage        storage.Storage
	Webhook        string
	Interval       time.Duration
	BatchSize      int
	Concurrency    int
	CleanUp        bool
	SchedulePaused bool
}

// TransferResult is the result of the Transfer activity.
type TransferResult struct {
	// URI of the newly stored object; empty if the object already existed.
	URI string
}

type workflowRuntime struct {
	Workflow
	client               ingest.Client
	enqueueAttemptsTotal *prometheus.CounterVec
	dequeueAttemptsTotal *prometheus.CounterVec
	webhookRequestsTotal *prometheus.CounterVec
}

// Activities holds the activities of all ingest workflows that run on Temporal.
type Activities struct {
	workflows map[string]*workflowRuntime
	// sourceLocks serializes the listing of a source,
	// because a Nexter keeps its cursor and is not safe for concurrent use.
	sourceLocks map[string]*sync.Mutex
	l           log.Logger
}

// NewActivities creates the activities for the given workflows.
// The registerer is labeled per workflow like the registerers of the NATS enqueuer and dequeuer,
// so that existing dashboards and SLOs keep working.
func NewActivities(workflows []Workflow, r prometheus.Registerer, l log.Logger) (*Activities, error) {
	if l == nil {
		l = log.NewNopLogger()
	}
	a := &Activities{
		workflows:   make(map[string]*workflowRuntime, len(workflows)),
		sourceLocks: make(map[string]*sync.Mutex),
		l:           l,
	}
	for _, w := range workflows {
		if _, ok := a.workflows[w.Name]; ok {
			return nil, fmt.Errorf("found duplicate workflow %q", w.Name)
		}
		reg := prometheus.WrapRegistererWith(prometheus.Labels{
			"source":   w.SourceName,
			"workflow": w.Name,
		}, r)
		wr := &workflowRuntime{
			Workflow: w,
			client:   dequeue.NewInstrumentedClient(w.Source, reg),
			enqueueAttemptsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "ingest_enqueue_attempts_total",
				Help: "Number of enqueue sync attempts.",
			}, []string{"result"}),
			dequeueAttemptsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "ingest_dequeue_attempts_total",
				Help: "Number of dequeue sync attempts.",
			}, []string{"result"}),
			webhookRequestsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "ingest_webhook_http_client_requests_total",
				Help: "The number webhook HTTP requests.",
			}, []string{"result"}),
		}
		for _, c := range []*prometheus.CounterVec{wr.enqueueAttemptsTotal, wr.dequeueAttemptsTotal, wr.webhookRequestsTotal} {
			for _, r := range []string{"error", "success"} {
				c.WithLabelValues(r).Add(0)
			}
		}
		a.workflows[w.Name] = wr
		if _, ok := a.sourceLocks[w.SourceName]; !ok {
			a.sourceLocks[w.SourceName] = &sync.Mutex{}
		}
	}
	return a, nil
}

// ListItems returns all items that the source of the workflow currently provides.
func (a *Activities) ListItems(ctx context.Context, workflow string) ([]ingest.Codec, error) {
	w, err := a.workflow(workflow)
	if err != nil {
		return nil, err
	}
	lock := a.sourceLocks[w.SourceName]
	lock.Lock()
	defer lock.Unlock()

	items, err := withHeartbeat(ctx, func() ([]ingest.Codec, error) {
		if err := w.Source.Reset(ctx); err != nil {
			return nil, fmt.Errorf("failed to reset nexter: %w", err)
		}
		var items []ingest.Codec
		for {
			c, err := w.Source.Next(ctx)
			if errors.Is(err, io.EOF) {
				return items, nil
			}
			if err != nil {
				return nil, fmt.Errorf("failed to get next item: %w", err)
			}
			items = append(items, *c)
		}
	})
	if err != nil {
		w.enqueueAttemptsTotal.WithLabelValues("error").Inc()
		return nil, err
	}
	w.enqueueAttemptsTotal.WithLabelValues("success").Inc()
	level.Info(a.l).Log("msg", "listed items from source", "workflow", w.Name, "items", len(items))
	return items, nil
}

// Transfer copies the item to the destinations of the workflow unless it is already stored.
// Download and store must happen in the same activity, because the object is streamed from the plugin.
func (a *Activities) Transfer(ctx context.Context, workflow string, item ingest.Codec) (*TransferResult, error) {
	w, err := a.workflow(workflow)
	if err != nil {
		return nil, err
	}
	u, err := withHeartbeat(ctx, func() (string, error) {
		u, err := dequeue.Transfer(ctx, w.client, w.Storage, item)
		if err != nil || u == nil {
			return "", err
		}
		return u.String(), nil
	})
	if err != nil {
		w.dequeueAttemptsTotal.WithLabelValues("error").Inc()
		return nil, err
	}
	w.dequeueAttemptsTotal.WithLabelValues("success").Inc()
	return &TransferResult{URI: u}, nil
}

// CleanUp cleans up the item in the source of the workflow, e.g. moves a beA message to another folder.
func (a *Activities) CleanUp(ctx context.Context, workflow string, item ingest.Codec) error {
	w, err := a.workflow(workflow)
	if err != nil {
		return err
	}
	if _, err := withHeartbeat(ctx, func() (struct{}, error) {
		return struct{}{}, w.client.CleanUp(ctx, item)
	}); err != nil {
		w.dequeueAttemptsTotal.WithLabelValues("error").Inc()
		return err
	}
	return nil
}

// CallWebhook sends the URIs of newly stored objects to the webhook of the workflow.
func (a *Activities) CallWebhook(ctx context.Context, workflow string, uris []string) error {
	w, err := a.workflow(workflow)
	if err != nil {
		return err
	}
	if w.Webhook == "" {
		return nil
	}
	if err := dequeue.CallWebhook(ctx, w.Webhook, uris); err != nil {
		w.webhookRequestsTotal.WithLabelValues("error").Inc()
		return err
	}
	w.webhookRequestsTotal.WithLabelValues("success").Inc()
	return nil
}

func (a *Activities) workflow(name string) (*workflowRuntime, error) {
	w, ok := a.workflows[name]
	if !ok {
		// The workflow is not configured on this worker, so retrying cannot succeed.
		return nil, sdktemporal.NewNonRetryableApplicationError(fmt.Sprintf("unknown ingest workflow %q", name), "UnknownWorkflow", nil)
	}
	return w, nil
}

// withHeartbeat runs f and records an activity heartbeat while it is running.
// The plugin calls are blocking and do not report progress themselves.
func withHeartbeat[T any](ctx context.Context, f func() (T, error)) (T, error) {
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(heartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				activity.RecordHeartbeat(ctx)
			}
		}
	}()
	return f()
}

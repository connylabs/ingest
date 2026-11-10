package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/uber-go/tally/v4"
	tallyprom "github.com/uber-go/tally/v4/prometheus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	sdktally "go.temporal.io/sdk/contrib/tally"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// SchedulePrefix prefixes the IDs of all Schedules managed by ingest.
const SchedulePrefix = "ingest-"

// Config configures the connection to Temporal.
type Config struct {
	HostPort  string
	Namespace string
	TaskQueue string
	// SearchAttributes sets the IngestWorkflow and ItemID search attributes on every item.
	SearchAttributes bool
}

// ScheduleID returns the ID of the Schedule of the given ingest workflow.
func ScheduleID(workflowName string) string {
	return SchedulePrefix + workflowName
}

// NewMetricsHandler exposes the metrics of the Temporal SDK via the given Prometheus registry.
// The returned closer must be called on shutdown.
func NewMetricsHandler(r *prometheus.Registry) (client.MetricsHandler, func() error, error) {
	reporter, err := tallyprom.Configuration{TimerType: "histogram"}.NewReporter(tallyprom.ConfigurationOptions{
		Registry: r,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Prometheus reporter: %w", err)
	}
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		CachedReporter:  reporter,
		Separator:       tallyprom.DefaultSeparator,
		SanitizeOptions: &sdktally.PrometheusSanitizeOptions,
		Prefix:          "temporal",
	}, time.Second)
	return sdktally.NewMetricsHandler(scope), closer.Close, nil
}

// Dial connects to Temporal.
func Dial(cfg Config, mh client.MetricsHandler, l log.Logger) (client.Client, error) {
	c, err := client.Dial(client.Options{
		HostPort:       cfg.HostPort,
		Namespace:      cfg.Namespace,
		Logger:         NewLogger(l),
		MetricsHandler: mh,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Temporal at %s: %w", cfg.HostPort, err)
	}
	return c, nil
}

// NewWorker creates a worker that runs the ingest workflows and activities on the task queue.
func NewWorker(c client.Client, taskQueue string, a *Activities) worker.Worker {
	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(SyncWorkflow, workflow.RegisterOptions{Name: SyncWorkflowName})
	w.RegisterWorkflowWithOptions(ItemWorkflow, workflow.RegisterOptions{Name: ItemWorkflowName})
	w.RegisterActivityWithOptions(a.ListItems, activity.RegisterOptions{Name: ListItemsActivityName})
	w.RegisterActivityWithOptions(a.Transfer, activity.RegisterOptions{Name: TransferActivityName})
	w.RegisterActivityWithOptions(a.CleanUp, activity.RegisterOptions{Name: CleanUpActivityName})
	w.RegisterActivityWithOptions(a.CallWebhook, activity.RegisterOptions{Name: CallWebhookActivityName})
	return w
}

// ReconcileSchedules creates or updates one Schedule per workflow and
// pauses all other Schedules managed by ingest.
// The configuration is the source of truth: the paused state of every
// Schedule is set from SchedulePaused on every start of the worker.
func ReconcileSchedules(ctx context.Context, c client.Client, cfg Config, workflows []Workflow, l log.Logger) error {
	sc := c.ScheduleClient()
	managed := make(map[string]struct{}, len(workflows))
	for _, w := range workflows {
		id := ScheduleID(w.Name)
		managed[id] = struct{}{}
		spec := client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: w.Interval}}}
		action := &client.ScheduleWorkflowAction{
			ID:       "ingest-sync/" + w.Name,
			Workflow: SyncWorkflowName,
			Args: []any{SyncInput{
				Workflow:         w.Name,
				BatchSize:        w.BatchSize,
				Concurrency:      w.Concurrency,
				CleanUp:          w.CleanUp,
				Webhook:          w.Webhook != "",
				SearchAttributes: cfg.SearchAttributes,
			}},
			TaskQueue: cfg.TaskQueue,
			// The Schedule skips new runs while a run is still open,
			// so bound a stuck run to not block the Schedule forever.
			WorkflowRunTimeout: 6 * time.Hour,
		}

		h := sc.GetHandle(ctx, id)
		_, err := h.Describe(ctx)
		var notFound *serviceerror.NotFound
		switch {
		case errors.As(err, &notFound):
			if _, err := sc.Create(ctx, client.ScheduleOptions{
				ID:      id,
				Spec:    spec,
				Action:  action,
				Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
				Paused:  w.SchedulePaused,
				Note:    "managed by ingest",
			}); err != nil {
				return fmt.Errorf("failed to create schedule %q: %w", id, err)
			}
			level.Info(l).Log("msg", "created schedule", "schedule", id, "paused", w.SchedulePaused, "interval", w.Interval.String())
		case err != nil:
			return fmt.Errorf("failed to describe schedule %q: %w", id, err)
		default:
			paused := w.SchedulePaused
			if err := h.Update(ctx, client.ScheduleUpdateOptions{
				DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
					s := in.Description.Schedule
					s.Spec = &spec
					s.Action = action
					if s.Policy == nil {
						s.Policy = &client.SchedulePolicies{}
					}
					s.Policy.Overlap = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
					if s.State == nil {
						s.State = &client.ScheduleState{}
					}
					s.State.Paused = paused
					s.State.Note = "managed by ingest"
					return &client.ScheduleUpdate{Schedule: &s}, nil
				},
			}); err != nil {
				return fmt.Errorf("failed to update schedule %q: %w", id, err)
			}
			level.Info(l).Log("msg", "updated schedule", "schedule", id, "paused", paused, "interval", w.Interval.String())
		}
	}

	// Pause the Schedules of workflows that were removed or moved back to NATS,
	// so that a workflow is never processed by both backends.
	it, err := sc.List(ctx, client.ScheduleListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list schedules: %w", err)
	}
	for it.HasNext() {
		e, err := it.Next()
		if err != nil {
			return fmt.Errorf("failed to list schedules: %w", err)
		}
		if !strings.HasPrefix(e.ID, SchedulePrefix) {
			continue
		}
		if _, ok := managed[e.ID]; ok || e.Paused {
			continue
		}
		if err := sc.GetHandle(ctx, e.ID).Pause(ctx, client.SchedulePauseOptions{
			Note: "paused by ingest: workflow is not configured with the temporal backend",
		}); err != nil {
			return fmt.Errorf("failed to pause schedule %q: %w", e.ID, err)
		}
		level.Warn(l).Log("msg", "paused schedule of a workflow that is not configured with the temporal backend", "schedule", e.ID)
	}
	return nil
}

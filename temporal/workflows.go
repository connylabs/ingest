// Package temporal runs ingest workflows on Temporal instead of NATS JetStream.
//
// Every ingest workflow configured with the "temporal" backend gets a Temporal Schedule
// that periodically starts a SyncWorkflow. The SyncWorkflow lists the items of the source
// and starts one ItemWorkflow per item, which copies the item to the destinations
// and cleans it up in the source.
package temporal

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/connylabs/ingest"
)

const (
	// SyncWorkflowName is the registered name of SyncWorkflow.
	SyncWorkflowName = "IngestSync"
	// ItemWorkflowName is the registered name of ItemWorkflow.
	ItemWorkflowName = "IngestItem"
)

var (
	// IngestWorkflowKey is the search attribute holding the name of the ingest workflow.
	IngestWorkflowKey = sdktemporal.NewSearchAttributeKeyKeyword("IngestWorkflow")
	// ItemIDKey is the search attribute holding the ID of the item, e.g. the beA message ID.
	ItemIDKey = sdktemporal.NewSearchAttributeKeyKeyword("ItemID")
)

// SyncInput is the input of SyncWorkflow.
// It is set as the argument of the workflow's Schedule so that a configuration change
// never changes the behavior of a SyncWorkflow that is already running.
type SyncInput struct {
	// Workflow is the name of the ingest workflow.
	Workflow    string
	BatchSize   int
	Concurrency int
	CleanUp     bool
	// Webhook is true if the ingest workflow has a webhook configured.
	Webhook bool
	// SearchAttributes sets the IngestWorkflow and ItemID search attributes on every ItemWorkflow.
	// The search attributes must exist in the Temporal namespace.
	SearchAttributes bool
}

// SyncResult summarizes one run of SyncWorkflow.
type SyncResult struct {
	Listed  int
	Stored  int
	Existed int
	Skipped int
	Failed  int
}

// ItemInput is the input of ItemWorkflow.
type ItemInput struct {
	Workflow string
	Item     ingest.Codec
	CleanUp  bool
}

// ItemResult is the result of ItemWorkflow.
type ItemResult struct {
	// URI of the newly stored object; empty if the object already existed.
	URI string
}

// ItemWorkflowID returns the ID of the ItemWorkflow for the given item.
// Using a deterministic ID ensures that an item is never processed twice at the same time.
func ItemWorkflowID(workflowName, itemID string) string {
	return fmt.Sprintf("ingest/%s/%s", workflowName, itemID)
}

// SyncWorkflow lists all items of the source and processes them in batches.
// After each batch, the webhook is called with the URIs of the newly stored objects,
// which mirrors the behavior of the NATS dequeuer.
// A failed item does not fail the SyncWorkflow: the item remains in the source and
// is picked up again by the next run.
func SyncWorkflow(ctx workflow.Context, in SyncInput) (*SyncResult, error) {
	logger := workflow.GetLogger(ctx)

	var items []ingest.Codec
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, listItemsOptions), ListItemsActivityName, in.Workflow).Get(ctx, &items); err != nil {
		return nil, fmt.Errorf("failed to list items: %w", err)
	}
	res := &SyncResult{Listed: len(items)}

	batchSize := max(in.BatchSize, 1)
	concurrency := max(in.Concurrency, 1)
	for start := 0; start < len(items); start += batchSize {
		batch := items[start:min(start+batchSize, len(items))]
		var uris []string
		for g := 0; g < len(batch); g += concurrency {
			group := batch[g:min(g+concurrency, len(batch))]
			futures := make([]workflow.ChildWorkflowFuture, len(group))
			for i, item := range group {
				futures[i] = workflow.ExecuteChildWorkflow(
					workflow.WithChildOptions(ctx, itemChildOptions(in, item)),
					ItemWorkflowName,
					ItemInput{Workflow: in.Workflow, Item: item, CleanUp: in.CleanUp},
				)
			}
			for i, f := range futures {
				var ir ItemResult
				if err := f.Get(ctx, &ir); err != nil {
					if sdktemporal.IsWorkflowExecutionAlreadyStartedError(err) {
						logger.Info("item is already being processed", "id", group[i].ID)
						res.Skipped++
						continue
					}
					logger.Error("failed to process item", "id", group[i].ID, "name", group[i].Name, "error", err)
					res.Failed++
					continue
				}
				if ir.URI == "" {
					res.Existed++
					continue
				}
				res.Stored++
				uris = append(uris, ir.URI)
			}
		}

		if in.Webhook && len(uris) > 0 {
			if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, webhookOptions), CallWebhookActivityName, in.Workflow, uris).Get(ctx, nil); err != nil {
				logger.Error("failed to call webhook", "uris", uris, "error", err)
			}
		}
	}

	logger.Info("finished sync", "listed", res.Listed, "stored", res.Stored, "existed", res.Existed, "skipped", res.Skipped, "failed", res.Failed)
	return res, nil
}

// ItemWorkflow copies one item to the destinations and cleans it up in the source.
// The clean up only runs after the item was stored successfully.
func ItemWorkflow(ctx workflow.Context, in ItemInput) (*ItemResult, error) {
	var tr TransferResult
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, transferOptions), TransferActivityName, in.Workflow, in.Item).Get(ctx, &tr); err != nil {
		return nil, fmt.Errorf("failed to transfer item: %w", err)
	}

	if in.CleanUp {
		if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, cleanUpOptions), CleanUpActivityName, in.Workflow, in.Item).Get(ctx, nil); err != nil {
			return nil, fmt.Errorf("failed to clean up item: %w", err)
		}
	}

	return &ItemResult{URI: tr.URI}, nil
}

func itemChildOptions(in SyncInput, item ingest.Codec) workflow.ChildWorkflowOptions {
	o := workflow.ChildWorkflowOptions{
		WorkflowID: ItemWorkflowID(in.Workflow, item.ID),
		// An item that is still in the source must be processable again by a later run.
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		// Let an item finish even if its SyncWorkflow is terminated.
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
	}
	if in.SearchAttributes {
		o.TypedSearchAttributes = sdktemporal.NewSearchAttributes(
			IngestWorkflowKey.ValueSet(in.Workflow),
			ItemIDKey.ValueSet(item.ID),
		)
	}
	return o
}

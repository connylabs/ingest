package temporal

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/connylabs/ingest"
)

func newTestEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	a, err := NewActivities(nil, prometheus.NewRegistry(), nil)
	require.NoError(t, err)
	env.RegisterWorkflowWithOptions(SyncWorkflow, workflow.RegisterOptions{Name: SyncWorkflowName})
	env.RegisterWorkflowWithOptions(ItemWorkflow, workflow.RegisterOptions{Name: ItemWorkflowName})
	env.RegisterActivityWithOptions(a.ListItems, activity.RegisterOptions{Name: ListItemsActivityName})
	env.RegisterActivityWithOptions(a.Transfer, activity.RegisterOptions{Name: TransferActivityName})
	env.RegisterActivityWithOptions(a.CleanUp, activity.RegisterOptions{Name: CleanUpActivityName})
	env.RegisterActivityWithOptions(a.CallWebhook, activity.RegisterOptions{Name: CallWebhookActivityName})
	return env
}

func itemWithID(id string) interface{} {
	return mock.MatchedBy(func(c ingest.Codec) bool { return c.ID == id })
}

func TestSyncWorkflow(t *testing.T) {
	items := []ingest.Codec{{ID: "1", Name: "1.tar"}, {ID: "2", Name: "2.tar"}, {ID: "3", Name: "3.tar"}}

	t.Run("stores new items and calls the webhook once per batch", func(t *testing.T) {
		env := newTestEnv(t)
		env.OnActivity(ListItemsActivityName, mock.Anything, "w").Return(items, nil).Once()
		env.OnActivity(TransferActivityName, mock.Anything, "w", itemWithID("1")).Return(&TransferResult{URI: "s3://b/1.tar"}, nil).Once()
		env.OnActivity(TransferActivityName, mock.Anything, "w", itemWithID("2")).Return(&TransferResult{}, nil).Once()
		env.OnActivity(TransferActivityName, mock.Anything, "w", itemWithID("3")).Return(&TransferResult{URI: "s3://b/3.tar"}, nil).Once()
		env.OnActivity(CleanUpActivityName, mock.Anything, "w", mock.Anything).Return(nil).Times(3)
		env.OnActivity(CallWebhookActivityName, mock.Anything, "w", []string{"s3://b/1.tar"}).Return(nil).Once()
		env.OnActivity(CallWebhookActivityName, mock.Anything, "w", []string{"s3://b/3.tar"}).Return(nil).Once()

		env.ExecuteWorkflow(SyncWorkflowName, SyncInput{Workflow: "w", BatchSize: 2, Concurrency: 2, CleanUp: true, Webhook: true})

		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		var res SyncResult
		require.NoError(t, env.GetWorkflowResult(&res))
		require.Equal(t, SyncResult{Listed: 3, Stored: 2, Existed: 1}, res)
		env.AssertExpectations(t)
	})

	t.Run("a failed item does not fail the sync and is not cleaned up", func(t *testing.T) {
		env := newTestEnv(t)
		env.OnActivity(ListItemsActivityName, mock.Anything, "w").Return(items[:2], nil).Once()
		env.OnActivity(TransferActivityName, mock.Anything, "w", itemWithID("1")).Return(nil, errors.New("download failed"))
		env.OnActivity(TransferActivityName, mock.Anything, "w", itemWithID("2")).Return(&TransferResult{URI: "s3://b/2.tar"}, nil).Once()
		env.OnActivity(CleanUpActivityName, mock.Anything, "w", itemWithID("2")).Return(nil).Once()
		env.OnActivity(CallWebhookActivityName, mock.Anything, "w", []string{"s3://b/2.tar"}).Return(nil).Once()

		env.ExecuteWorkflow(SyncWorkflowName, SyncInput{Workflow: "w", BatchSize: 8, Concurrency: 8, CleanUp: true, Webhook: true})

		require.NoError(t, env.GetWorkflowError())
		var res SyncResult
		require.NoError(t, env.GetWorkflowResult(&res))
		require.Equal(t, SyncResult{Listed: 2, Stored: 1, Failed: 1}, res)
		env.AssertExpectations(t)
		env.AssertActivityNumberOfCalls(t, TransferActivityName, 1+int(transferOptions.RetryPolicy.MaximumAttempts))
	})

	t.Run("no webhook and no clean up when disabled", func(t *testing.T) {
		env := newTestEnv(t)
		env.OnActivity(ListItemsActivityName, mock.Anything, "w").Return(items[:1], nil).Once()
		env.OnActivity(TransferActivityName, mock.Anything, "w", itemWithID("1")).Return(&TransferResult{URI: "s3://b/1.tar"}, nil).Once()

		env.ExecuteWorkflow(SyncWorkflowName, SyncInput{Workflow: "w", BatchSize: 1, Concurrency: 1})

		require.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
		env.AssertActivityNotCalled(t, CleanUpActivityName, mock.Anything, mock.Anything, mock.Anything)
		env.AssertActivityNotCalled(t, CallWebhookActivityName, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("a failed listing fails the sync", func(t *testing.T) {
		env := newTestEnv(t)
		env.OnActivity(ListItemsActivityName, mock.Anything, "w").Return(nil, errors.New("bea-expert unavailable"))

		env.ExecuteWorkflow(SyncWorkflowName, SyncInput{Workflow: "w", BatchSize: 1, Concurrency: 1})

		require.Error(t, env.GetWorkflowError())
		env.AssertActivityNumberOfCalls(t, ListItemsActivityName, int(listItemsOptions.RetryPolicy.MaximumAttempts))
	})
}

func TestItemWorkflow(t *testing.T) {
	item := ingest.Codec{ID: "1", Name: "1.tar"}

	t.Run("clean up is attempted at most twice", func(t *testing.T) {
		env := newTestEnv(t)
		env.OnActivity(TransferActivityName, mock.Anything, "w", itemWithID("1")).Return(&TransferResult{URI: "s3://b/1.tar"}, nil).Once()
		env.OnActivity(CleanUpActivityName, mock.Anything, "w", itemWithID("1")).Return(errors.New("move failed"))

		env.ExecuteWorkflow(ItemWorkflowName, ItemInput{Workflow: "w", Item: item, CleanUp: true})

		require.Error(t, env.GetWorkflowError())
		env.AssertActivityNumberOfCalls(t, CleanUpActivityName, 2)
	})

	t.Run("already stored item is cleaned up", func(t *testing.T) {
		env := newTestEnv(t)
		env.OnActivity(TransferActivityName, mock.Anything, "w", itemWithID("1")).Return(&TransferResult{}, nil).Once()
		env.OnActivity(CleanUpActivityName, mock.Anything, "w", itemWithID("1")).Return(nil).Once()

		env.ExecuteWorkflow(ItemWorkflowName, ItemInput{Workflow: "w", Item: item, CleanUp: true})

		require.NoError(t, env.GetWorkflowError())
		var res ItemResult
		require.NoError(t, env.GetWorkflowResult(&res))
		require.Empty(t, res.URI)
		env.AssertExpectations(t)
	})
}

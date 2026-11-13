package temporal

import (
	"time"

	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// heartbeatInterval is how often long-running activities record a heartbeat.
// It must be well below the HeartbeatTimeout of the activity options.
const heartbeatInterval = 10 * time.Second

// Note: the plugin RPC does not propagate context cancellation,
// so a timed-out activity keeps running in the plugin until the call returns.
// The timeouts are therefore generous.
var (
	listItemsOptions = workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy: &sdktemporal.RetryPolicy{
			InitialInterval:    10 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    2 * time.Minute,
			MaximumAttempts:    3,
		},
	}

	transferOptions = workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy: &sdktemporal.RetryPolicy{
			InitialInterval:    10 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    5 * time.Minute,
			MaximumAttempts:    5,
		},
	}

	// A clean up is not necessarily idempotent, e.g. the bea-expert source
	// drafts an EEB confirmation, so it is retried at most once.
	cleanUpOptions = workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy: &sdktemporal.RetryPolicy{
			InitialInterval: 10 * time.Second,
			MaximumAttempts: 2,
		},
	}

	webhookOptions = workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &sdktemporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    5,
		},
	}
)

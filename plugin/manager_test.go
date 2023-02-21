package plugin

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPluginManagerWatch(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		pm := NewPluginManager(time.Millisecond, nil)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() {
			cancel()
			pm.Stop()
		})

		p, err := pm.NewSource(noopPath, nil, nil)
		require.NoError(t, err)

		go func() {
			// Watch will return when the context is cancelled.
			time.Sleep(2 * time.Millisecond)
			cancel()
		}()

		assert.NoError(t, pm.Watch(ctx))
		assert.NoError(t, p.Reset(ctx))
	})
	t.Run("killed plugin", func(t *testing.T) {
		pm := NewPluginManager(time.Millisecond, nil)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() {
			cancel()
		})

		_, err := pm.NewSource(noopPath, nil, nil)
		require.NoError(t, err)

		pm.sources[0].c.Kill()
		assert.Error(t, pm.Watch(ctx), "the watcher is expected to return an error")
	})

	t.Run("metrics", func(t *testing.T) {
		pm := NewPluginManager(time.Millisecond, nil)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(pm.Stop)
		t.Cleanup(cancel)

		_, err := pm.NewSource(noopPath, nil, prometheus.Labels{"plugin": "test"})
		require.NoError(t, err)

		go func() {
			_ = pm.Watch(ctx)
		}()

		time.Sleep(10 * time.Millisecond)
		mfs, err := pm.Gather()
		assert.NoError(t, err)
		assert.NotEmpty(t, mfs)
		p, err := testutil.GatherAndLint(pm)
		assert.NoError(t, err)
		assert.Empty(t, p)
	})
}

package plugin

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPluginManagerWatch(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		pm := &PluginManager{Interval: time.Millisecond}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() {
			cancel()
			pm.Stop()
		})

		p, err := pm.NewSource(noopPath, nil)
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
		pm := &PluginManager{Interval: time.Millisecond}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() {
			cancel()
		})

		_, err := pm.NewSource(noopPath, nil)
		require.NoError(t, err)

		pm.sources[0].c.Kill()
		assert.Error(t, pm.Watch(ctx), "the watcher is expected to return an error")
	})
}

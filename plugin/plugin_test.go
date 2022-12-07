package plugin

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/connylabs/ingest"
)

var noopPath string = fmt.Sprintf("../bin/plugin/%s/%s/noop", runtime.GOOS, runtime.GOARCH)

func TestNewPluginSource(t *testing.T) {
	t.Run("Next and Reset methods", func(t *testing.T) {
		pm := &PluginManager{}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() {
			cancel()
			if err := pm.Stop(); err != nil {
				// For some reason stopping the manager can fail in github actions.
				t.Logf("failed to stop PluginManager: %s\n", err.Error())
			}
		})

		p, err := pm.NewSource(noopPath, nil)
		require.NoError(t, err)

		err = p.Configure(nil)
		require.NoError(t, err)

		n, err := p.Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, defaultCodec, *n)

		n, err = p.Next(ctx)
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, io.EOF)
		assert.Nil(t, n)

		err = p.Reset(ctx)
		assert.NoError(t, err)

		n, err = p.Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, defaultCodec, *n)

		require.NoError(t, p.Configure(map[string]any{"resetErr": true}))

		assert.Error(t, p.Reset(ctx))
	})

	t.Run("Download and CleanUp", func(t *testing.T) {
		pm := &PluginManager{}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() {
			cancel()
			if err := pm.Stop(); err != nil {
				// For some reason stopping the manager can fail in github actions.
				t.Logf("failed to stop PluginManager: %s\n", err.Error())
			}
		})

		p, err := pm.NewSource(noopPath, nil)
		require.NoError(t, err)

		err = p.Configure(nil)
		require.NoError(t, err)

		n, err := p.Next(ctx)
		require.NoError(t, err)

		obj, err := p.Download(ctx, *n)
		require.NoError(t, err)

		b, err := io.ReadAll(obj.Reader)
		require.NoError(t, err)
		assert.Equal(t, defaultObjContent, string(b))

		require.NoError(t, p.CleanUp(ctx, *n))

		require.Error(t, p.CleanUp(ctx, ingest.NewCodec("unknown", "nobody")))
	})

	t.Run("Configure", func(t *testing.T) {
		pm := &PluginManager{}
		t.Cleanup(func() {
			if err := pm.Stop(); err != nil {
				// For some reason stopping the manager can fail in github actions.
				t.Logf("failed to stop PluginManager: %s\n", err.Error())
			}
		})

		p, err := pm.NewSource(noopPath, nil)
		require.NoError(t, err)

		assert.Error(t, p.Configure(map[string]any{"error": "an error"}))
		assert.Equal(t, "an error", p.Configure(map[string]any{"error": "an error"}).Error())
		assert.NoError(t, p.Configure(nil))
	})
}

func TestPluginDestination(t *testing.T) {
	t.Run("Configure", func(t *testing.T) {
		pm := &PluginManager{}
		t.Cleanup(func() {
			if err := pm.Stop(); err != nil {
				// For some reason stopping the manager can fail in github actions.
				t.Logf("failed to stop PluginManager: %s\n", err.Error())
			}
		})
		p, err := pm.NewDestination(noopPath, nil)
		require.NoError(t, err)
		require.NotNil(t, p)

		assert.NoError(t, p.Configure(nil))
		assert.Error(t, p.Configure(map[string]any{"error": "config error"}))
	})

	t.Run("Stat", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		pm := &PluginManager{}
		t.Cleanup(func() {
			cancel()
			if err := pm.Stop(); err != nil {
				// For some reason stopping the manager can fail in github actions.
				t.Logf("failed to stop PluginManager: %s\n", err.Error())
			}
		})
		p, err := pm.NewDestination(noopPath, nil)
		require.NoError(t, err)
		require.NotNil(t, p)

		require.NoError(t, p.Configure(map[string]any{}))

		u, err := p.Stat(ctx, defaultCodec)
		require.NoError(t, err)
		assert.Equal(t, defaultObjURL, u.URI)

		u, err = p.Stat(ctx, ingest.NewCodec("fake id", "unknown"))
		assert.Nil(t, u)
		assert.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("Store", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		pm := &PluginManager{}
		t.Cleanup(func() {
			cancel()
			if err := pm.Stop(); err != nil {
				// For some reason stopping the manager can fail in github actions.
				t.Logf("failed to stop PluginManager: %s\n", err.Error())
			}
		})
		p, err := pm.NewDestination(noopPath, nil)
		require.NoError(t, err)
		require.NotNil(t, p)

		require.NoError(t, p.Configure(nil))

		u, err := p.Store(ctx, defaultCodec, ingest.Object{
			Reader: strings.NewReader(defaultObjContent),
		})

		require.NoError(t, err)
		assert.Equal(t, defaultObjURL, u.String())

		u, err = p.Store(ctx, defaultCodec, ingest.Object{
			Reader: strings.NewReader("other content"),
		})
		require.Error(t, err)
		assert.Nil(t, u)
	})
}

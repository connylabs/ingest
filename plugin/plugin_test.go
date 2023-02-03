package plugin

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/connylabs/ingest"
)

var noopPath string = fmt.Sprintf("../bin/plugin/%s/%s/noop", runtime.GOOS, runtime.GOARCH)

const expected = `# HELP noop show that the noop plugin can add its own collectors
# TYPE noop gauge
noop{noop="noop"} 1
`

func TestNewPluginSource(t *testing.T) {
	t.Run("Next and Reset methods", func(t *testing.T) {
		pm := NewPluginManager(0, nil)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(pm.Stop)
		t.Cleanup(cancel)

		p, err := pm.NewSource(noopPath, nil, nil)
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
		pm := NewPluginManager(0, nil)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(pm.Stop)
		t.Cleanup(cancel)

		p, err := pm.NewSource(noopPath, nil, nil)
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

		require.Error(t, p.CleanUp(ctx, ingest.NewCodec("unknown", "nobody", nil)))
	})

	t.Run("Configure", func(t *testing.T) {
		pm := NewPluginManager(0, nil)
		t.Cleanup(pm.Stop)

		p, err := pm.NewSource(noopPath, nil, nil)
		require.NoError(t, err)

		assert.Error(t, p.Configure(map[string]any{"error": "an error"}))
		assert.Equal(t, "an error", p.Configure(map[string]any{"error": "an error"}).Error())
		assert.NoError(t, p.Configure(nil))
	})

	t.Run("Gather", func(t *testing.T) {
		pm := NewPluginManager(0, nil)
		t.Cleanup(pm.Stop)

		p, err := pm.NewSource(noopPath, nil, nil)
		require.NoError(t, err)

		g, ok := p.(prometheus.Gatherer)
		require.True(t, ok)

		m, err := g.Gather()
		assert.NoError(t, err)
		assert.NotEmpty(t, m)
		err = testutil.GatherAndCompare(g, strings.NewReader(expected), "noop")
		assert.NoError(t, err)

		pr, err := testutil.GatherAndLint(g)
		assert.NoError(t, err)
		assert.Empty(t, pr)
	})
}

func TestPluginDestination(t *testing.T) {
	t.Run("Configure", func(t *testing.T) {
		pm := NewPluginManager(0, nil)
		t.Cleanup(pm.Stop)

		p, err := pm.NewDestination(noopPath, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, p)

		assert.NoError(t, p.Configure(nil))
		assert.Error(t, p.Configure(map[string]any{"error": "config error"}))
	})

	t.Run("Stat", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		pm := NewPluginManager(0, nil)
		t.Cleanup(pm.Stop)
		t.Cleanup(cancel)

		p, err := pm.NewDestination(noopPath, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, p)

		require.NoError(t, p.Configure(map[string]any{}))

		u, err := p.Stat(ctx, defaultCodec)
		require.NoError(t, err)
		assert.Equal(t, defaultObjURL, u.URI)

		u, err = p.Stat(ctx, ingest.NewCodec("fake id", "unknown", nil))
		assert.Nil(t, u)
		assert.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("Store", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		pm := NewPluginManager(0, nil)
		t.Cleanup(pm.Stop)
		t.Cleanup(cancel)

		p, err := pm.NewDestination(noopPath, nil, nil)
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

	t.Run("Gather", func(t *testing.T) {
		pm := NewPluginManager(0, nil)
		t.Cleanup(pm.Stop)

		p, err := pm.NewDestination(noopPath, nil, nil)
		require.NoError(t, err)

		g, ok := p.(prometheus.Gatherer)
		require.True(t, ok)

		m, err := g.Gather()
		assert.NoError(t, err)
		assert.NotEmpty(t, m)
		err = testutil.GatherAndCompare(g, strings.NewReader(expected), "noop")
		assert.NoError(t, err)

		pr, err := testutil.GatherAndLint(g)
		assert.NoError(t, err)
		assert.Empty(t, pr)
	})
}

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
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		_, p, err := NewPlugin(ctx, noopPath)
		require.Nil(t, err)

		err = p.Configure(nil)
		require.Nil(t, err)

		n, err := p.Next(ctx)
		assert.Nil(t, err)
		assert.Equal(t, DefaultCodec, *n)

		n, err = p.Next(ctx)
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, io.EOF)
		assert.Nil(t, n)

		err = p.Reset(ctx)
		assert.Nil(t, err)

		n, err = p.Next(ctx)
		assert.Nil(t, err)
		assert.Equal(t, DefaultCodec, *n)

		require.Nil(t, p.Configure(map[string]any{"resetErr": true}))

		assert.Error(t, p.Reset(ctx))
	})

	t.Run("Download and CleanUp", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		_, p, err := NewPlugin(ctx, noopPath)
		require.Nil(t, err)

		err = p.Configure(nil)
		require.Nil(t, err)

		n, err := p.Next(ctx)
		require.Nil(t, err)

		obj, err := p.Download(ctx, *n)
		require.Nil(t, err)

		b, err := io.ReadAll(obj.Reader)
		require.Nil(t, err)
		assert.Equal(t, DefaultObjContent, string(b))

		require.Nil(t, p.CleanUp(ctx, *n))

		require.Error(t, p.CleanUp(ctx, ingest.SimpleCodec{XID: "unknown", XName: "nobody"}))
	})

	t.Run("Configure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		_, p, err := NewPlugin(ctx, noopPath)
		require.Nil(t, err)

		assert.Error(t, p.Configure(map[string]any{"error": "an error"}))
		assert.Equal(t, "an error", p.Configure(map[string]any{"error": "an error"}).Error())
		assert.Nil(t, p.Configure(nil))
	})
}

func TestPluginStore(t *testing.T) {
	t.Run("Configure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		p, _, err := NewPlugin(ctx, noopPath)
		require.Nil(t, err)
		require.NotNil(t, p)

		assert.Nil(t, p.Configure(nil))
		assert.Error(t, p.Configure(map[string]any{"error": "config error"}))
	})

	t.Run("Stat", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		p, _, err := NewPlugin(ctx, noopPath)
		require.Nil(t, err)
		require.NotNil(t, p)

		u, err := p.Stat(ctx, DefaultCodec)
		require.Nil(t, err)
		assert.Equal(t, DefaultObjURL, u.URI)

		u, err = p.Stat(ctx, *ingest.NewCodec("fake id", "unknown"))
		assert.Nil(t, u)
		assert.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("Store", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		p, _, err := NewPlugin(ctx, noopPath)
		require.Nil(t, err)
		require.NotNil(t, p)

		u, err := p.Store(ctx, DefaultCodec, ingest.Object{
			Reader: strings.NewReader(DefaultObjContent),
		})
		require.Nil(t, err)
		assert.Equal(t, DefaultObjURL, u.String())

		u, err = p.Store(ctx, DefaultCodec, ingest.Object{
			Reader: strings.NewReader("other content"),
		})
		require.Error(t, err)
		assert.Nil(t, u)
	})
}

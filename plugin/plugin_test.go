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

		p, err := NewSource(noopPath)
		require.Nil(t, err)

		_, err = p.Next(ctx)
		assert.ErrorIs(t, err, ErrNotConfigured)

		assert.ErrorIs(t, p.Reset(ctx), ErrNotConfigured)

		err = p.Configure(nil)
		require.Nil(t, err)

		n, err := p.Next(ctx)
		assert.Nil(t, err)
		assert.Equal(t, defaultCodec, *n)

		n, err = p.Next(ctx)
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, io.EOF)
		assert.Nil(t, n)

		err = p.Reset(ctx)
		assert.Nil(t, err)

		n, err = p.Next(ctx)
		assert.Nil(t, err)
		assert.Equal(t, defaultCodec, *n)

		require.Nil(t, p.Configure(map[string]any{"resetErr": true}))

		assert.Error(t, p.Reset(ctx))
	})

	t.Run("Download and CleanUp", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		p, err := NewSource(noopPath)
		require.Nil(t, err)

		_, err = p.Download(ctx, defaultCodec)
		assert.ErrorIs(t, err, ErrNotConfigured)

		assert.ErrorIs(t, p.CleanUp(ctx, defaultCodec), ErrNotConfigured)

		err = p.Configure(nil)
		require.Nil(t, err)

		n, err := p.Next(ctx)
		require.Nil(t, err)

		obj, err := p.Download(ctx, *n)
		require.Nil(t, err)

		b, err := io.ReadAll(obj.Reader)
		require.Nil(t, err)
		assert.Equal(t, defaultObjContent, string(b))

		require.Nil(t, p.CleanUp(ctx, *n))

		require.Error(t, p.CleanUp(ctx, ingest.NewCodec("unknown", "nobody")))
	})

	t.Run("Configure", func(t *testing.T) {
		p, err := NewSource(noopPath)
		require.Nil(t, err)

		assert.Error(t, p.Configure(map[string]any{"error": "an error"}))
		assert.Equal(t, "an error", p.Configure(map[string]any{"error": "an error"}).Error())
		assert.Nil(t, p.Configure(nil))
	})
}

func TestPluginDestination(t *testing.T) {
	t.Run("Configure", func(t *testing.T) {
		p, err := NewDestination(noopPath)
		require.Nil(t, err)
		require.NotNil(t, p)

		assert.Nil(t, p.Configure(nil))
		assert.Error(t, p.Configure(map[string]any{"error": "config error"}))
	})

	t.Run("Stat", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		p, err := NewDestination(noopPath)
		require.Nil(t, err)
		require.NotNil(t, p)

		_, err = p.Stat(ctx, defaultCodec)
		assert.ErrorIs(t, err, ErrNotConfigured)

		require.Nil(t, p.Configure(map[string]any{}))

		u, err := p.Stat(ctx, defaultCodec)
		require.Nil(t, err)
		assert.Equal(t, defaultObjURL, u.URI)

		u, err = p.Stat(ctx, ingest.NewCodec("fake id", "unknown"))
		assert.Nil(t, u)
		assert.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("Store", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		p, err := NewDestination(noopPath)
		require.Nil(t, err)
		require.NotNil(t, p)

		_, err = p.Store(ctx, defaultCodec, ingest.Object{
			Reader: strings.NewReader(defaultObjContent),
		})

		assert.ErrorIs(t, err, ErrNotConfigured)

		require.Nil(t, p.Configure(nil))

		u, err := p.Store(ctx, defaultCodec, ingest.Object{
			Reader: strings.NewReader(defaultObjContent),
		})

		require.Nil(t, err)
		assert.Equal(t, defaultObjURL, u.String())

		u, err = p.Store(ctx, defaultCodec, ingest.Object{
			Reader: strings.NewReader("other content"),
		})
		require.Error(t, err)
		assert.Nil(t, u)
	})
}

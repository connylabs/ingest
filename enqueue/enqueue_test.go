package enqueue

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/mocks"
)

func TestEnqueue(t *testing.T) {
	for _, tc := range []struct {
		name   string
		expect func() (*mocks.Queue, *mocks.Nexter, *ingest.Codec)
	}{
		{
			name: "nexter returns EOF",
			expect: func() (*mocks.Queue, *mocks.Nexter, *ingest.Codec) {
				q := new(mocks.Queue)
				n := new(mocks.Nexter)
				n.On("Reset", mock.Anything).Return(nil).Once()
				n.On("Next", mock.Anything).Return(nil, io.EOF).Once()
				return q, n, nil
			},
		},
		{
			name: "nexter returns nil",
			expect: func() (*mocks.Queue, *mocks.Nexter, *ingest.Codec) {
				q := new(mocks.Queue)
				n := new(mocks.Nexter)
				n.On("Reset", mock.Anything).Return(nil).Once()
				n.On("Next", mock.Anything).Return(nil, io.EOF).Once()
				return q, n, nil
			},
		},
		{
			name: "one entry",
			expect: func() (*mocks.Queue, *mocks.Nexter, *ingest.Codec) {
				t := ingest.NewCodec("foo", "foo", []byte(`{"key":"value"}`))
				data, _ := t.Marshal()
				q := new(mocks.Queue)
				q.On("Publish", "sub", data).Return(nil).Once()
				n := new(mocks.Nexter)
				n.On("Reset", mock.Anything).Return(nil).
					On("Next", mock.Anything).Once().Return(&t, nil).
					On("Next", mock.Anything).Return(nil, io.EOF)
				return q, n, &t
			},
		},
		{
			name: "two entries",
			expect: func() (*mocks.Queue, *mocks.Nexter, *ingest.Codec) {
				t := ingest.NewCodec("foo", "foo", nil)
				data, _ := t.Marshal()
				t2 := ingest.NewCodec("foo2", "foo2", nil)
				data2, _ := t2.Marshal()
				q := new(mocks.Queue)
				q.
					On("Publish", "sub", data).Return(nil).Once().
					On("Publish", "sub", data2).Return(nil).Once()
				n := new(mocks.Nexter)
				n.
					On("Reset", mock.Anything).Return(nil).Once().
					On("Next", mock.Anything).Return(&t, nil).Once().
					On("Next", mock.Anything).Return(&t2, nil).Once().
					On("Next", mock.Anything).Return(nil, io.EOF).Once()
				return q, n, &t
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
			ctx := context.Background()

			q, n, _ := tc.expect()

			e, err := New(n, "sub", q, reg, logger)
			assert.NoError(t, err)
			assert.NoError(t, e.Enqueue(ctx))

			n.AssertExpectations(t)
			q.AssertExpectations(t)

			lps, err := testutil.GatherAndLint(reg)
			require.Nil(t, err)
			assert.Empty(t, lps)

			c, err := testutil.GatherAndCount(reg, "ingest_enqueue_attempts_total")
			require.Nil(t, err)
			assert.Equal(t, 2, c)
		})
	}
	t.Run("failed to reset", func(t *testing.T) {
		assert := assert.New(t)
		reg := prometheus.NewRegistry()
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		ctx := context.Background()

		q := new(mocks.Queue)
		n := new(mocks.Nexter)
		eerr := errors.New("some error")
		n.On("Reset", mock.Anything).Return(eerr).Once()

		e, err := New(n, "sub", q, reg, logger)
		if err != nil {
			t.Error(err)
		}
		assert.ErrorIs(e.Enqueue(ctx), eerr)

		n.AssertExpectations(t)
		q.AssertExpectations(t)
		c, err := testutil.GatherAndCount(reg, "ingest_enqueue_attempts_total")
		require.Nil(t, err)
		assert.Equal(2, c)
	})
}

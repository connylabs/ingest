package enqueue

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/connylabs/ingest/mocks"
)

func TestEnqueue(t *testing.T) {
	for _, tc := range []struct {
		name   string
		expect func() (*mocks.Queue, *mocks.Nexter, *mocks.T)
	}{
		{
			name: "nexter returns EOF",
			expect: func() (*mocks.Queue, *mocks.Nexter, *mocks.T) {
				q := new(mocks.Queue)
				n := new(mocks.Nexter)
				n.On("Reset", mock.Anything).Return(nil).Once()
				n.On("Next", mock.Anything).Return(nil, io.EOF).Once()
				return q, n, nil
			},
		},
		{
			name: "nexter returns nil",
			expect: func() (*mocks.Queue, *mocks.Nexter, *mocks.T) {
				q := new(mocks.Queue)
				n := new(mocks.Nexter)
				n.On("Reset", mock.Anything).Return(nil).Once()
				n.On("Next", mock.Anything).Return(nil, io.EOF).Once()
				return q, n, nil
			},
		},
		{
			name: "one entry",
			expect: func() (*mocks.Queue, *mocks.Nexter, *mocks.T) {
				t := &mocks.T{MockID: "foo"}
				data, _ := json.Marshal(t)
				q := new(mocks.Queue)
				q.On("Publish", "sub", data).Return(nil).Once()
				n := new(mocks.Nexter)
				n.On("Reset", mock.Anything).Return(nil).
					On("Next", mock.Anything).Once().Return(t, nil).
					On("Next", mock.Anything).Return(nil, io.EOF)
				return q, n, t
			},
		},
		{
			name: "two entries",
			expect: func() (*mocks.Queue, *mocks.Nexter, *mocks.T) {
				t := &mocks.T{MockID: "foo"}
				data, _ := json.Marshal(t)
				t2 := &mocks.T{MockID: "foo2"}
				data2, _ := json.Marshal(t2)
				q := new(mocks.Queue)
				q.
					On("Publish", "sub", data).Return(nil).Once().
					On("Publish", "sub", data2).Return(nil).Once()
				n := new(mocks.Nexter)
				n.
					On("Reset", mock.Anything).Return(nil).Once().
					On("Next", mock.Anything).Return(t, nil).Once().
					On("Next", mock.Anything).Return(t2, nil).Once().
					On("Next", mock.Anything).Return(nil, io.EOF).Once()
				return q, n, t
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
			ctx := context.Background()

			q, n, _ := tc.expect()

			e, err := New[mocks.T](n, "sub", q, reg, logger)
			if err != nil {
				t.Error(err)
			}
			if err := e.Enqueue(ctx); err != nil {
				t.Error(err)
			}
			n.AssertExpectations(t)
			q.AssertExpectations(t)
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

		e, err := New[mocks.T](n, "sub", q, reg, logger)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(eerr, e.Enqueue(ctx))

		n.AssertExpectations(t)
		q.AssertExpectations(t)
	})
}

package dequeue

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"

	"github.com/connylabs/ingest/mocks"
)

func TestDequeue(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		c := new(mocks.Client[*mocks.T])
		q := new(mocks.Queue)
		s := new(mocks.Storage[*mocks.T])
		sub := new(mocks.Subscription)

		q.On("PullSubscribe", "sub", "con", mock.Anything).Return(sub, nil).Once()

		sub.On("Pop", 1, mock.Anything).Return([]*nats.Msg{}, nil).Once().
			On("Pop", 1, mock.Anything).Return([]*nats.Msg{}, nil).After(1 * time.Millisecond)

		sub.On("Close").Return(nil).Once()

		d := New[*mocks.T]("bucket", "prefix", "meta", "", c, s, q, "str", "con", "sub", 1, true, logger, reg)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		if err := d.Dequeue(ctx); err != nil {
			t.Error(err)
		}

		q.AssertExpectations(t)
		s.AssertExpectations(t)
		sub.AssertExpectations(t)
	})
	t.Run("one object", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		c := new(mocks.Client[*mocks.T])
		q := new(mocks.Queue)
		s := new(mocks.Storage[*mocks.T])
		sub := new(mocks.Subscription)
		_t := &mocks.T{MockID: "foo"}
		data, _ := json.Marshal(_t)
		msg := &nats.Msg{Data: data}
		obj := new(mocks.Object)

		q.On("PullSubscribe", "sub", "con", mock.Anything).Return(sub, nil).Once()

		sub.On("Pop", 1, mock.Anything).Return([]*nats.Msg{msg}, nil).Once()
		sub.On("Pop", 1, mock.Anything).Return([]*nats.Msg{}, nil)
		sub.On("Close").Return(nil).Once()

		c.On("CleanUp", mock.Anything, mock.Anything).Return(nil).Once()

		s.On("Store", mock.Anything, _t, mock.Anything).Return(&url.URL{Scheme: "s3", Host: "bucket", Path: "prefix/foo"}, nil).Once()

		d := New[*mocks.T]("bucket", "prefix", "meta", "", c, s, q, "str", "con", "sub", 1, true, logger, reg)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		if err := d.Dequeue(ctx); err != nil {
			t.Error(err)
		}

		q.AssertExpectations(t)
		sub.AssertExpectations(t)
		s.AssertExpectations(t)
		obj.AssertExpectations(t)
		c.AssertExpectations(t)
	})
}

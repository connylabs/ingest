package dequeue

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/minio/minio-go/v7"
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
		mc := new(mocks.MinioClient)
		sub := new(mocks.Subscription)

		q.On("PullSubscribe", "sub", "con", mock.Anything).Return(sub, nil).Once()

		sub.On("Pop", 1, mock.Anything).Return([]*nats.Msg{}, nil).Once().
			On("Pop", 1, mock.Anything).Return([]*nats.Msg{}, nil).After(1 * time.Millisecond)

		sub.On("Close").Return(nil).Once()

		d := New[*mocks.T]("bucket", "prefix", "meta", "", c, mc, q, "str", "con", "sub", 1, true, logger, reg)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		if err := d.Dequeue(ctx); err != nil {
			t.Error(err)
		}

		q.AssertExpectations(t)
		sub.AssertExpectations(t)
	})
	t.Run("one object", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		c := new(mocks.Client[*mocks.T])
		q := new(mocks.Queue)
		mc := new(mocks.MinioClient)
		sub := new(mocks.Subscription)
		_t := &mocks.T{MockID: "foo"}
		data, _ := json.Marshal(_t)
		msg := &nats.Msg{Data: data}
		obj := new(mocks.Object)

		q.On("PullSubscribe", "sub", "con", mock.Anything).Return(sub, nil).Once()

		sub.On("Pop", 1, mock.Anything).Return([]*nats.Msg{msg}, nil).Once()
		sub.On("Pop", 1, mock.Anything).Return([]*nats.Msg{}, nil)
		sub.On("Close").Return(nil).Once()

		c.On("Download", mock.Anything, mock.Anything).Return(obj, nil).Once()
		c.On("CleanUp", mock.Anything, mock.Anything).Return(nil).Once()

		obj.On("Len").Return(int64(64)).Once()
		obj.On("MimeType").Return("plain/text").Once()

		mc.On("PutObject", mock.Anything, "bucket", "prefix/foo", mock.Anything, int64(64), mock.Anything).Return(minio.UploadInfo{}, nil).Once().
			On("PutObject", mock.Anything, "bucket", "meta/foo.done", mock.Anything, int64(0), mock.Anything).Return(minio.UploadInfo{}, nil).Once()
		mc.On("StatObject", mock.Anything, "bucket", "prefix/foo", mock.Anything).Return(minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey"}).Once().
			On("StatObject", mock.Anything, "bucket", "meta/foo.done", mock.Anything).Return(minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey"}).Once()

		d := New[*mocks.T]("bucket", "prefix", "meta", "", c, mc, q, "str", "con", "sub", 1, true, logger, reg)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		if err := d.Dequeue(ctx); err != nil {
			t.Error(err)
		}

		q.AssertExpectations(t)
		sub.AssertExpectations(t)
		mc.AssertExpectations(t)
		obj.AssertExpectations(t)
		c.AssertExpectations(t)
	})
	t.Run("object exists", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		c := new(mocks.Client[*mocks.T])
		q := new(mocks.Queue)
		mc := new(mocks.MinioClient)
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

		mc.On("StatObject", mock.Anything, "bucket", "meta/foo.done", mock.Anything).Return(minio.ObjectInfo{}, nil).Once()

		d := New[*mocks.T]("bucket", "prefix", "meta", "", c, mc, q, "str", "con", "sub", 1, true, logger, reg)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		if err := d.Dequeue(ctx); err != nil {
			t.Error(err)
		}

		q.AssertExpectations(t)
		sub.AssertExpectations(t)
		mc.AssertExpectations(t)
		obj.AssertExpectations(t)
		c.AssertExpectations(t)
	})
}

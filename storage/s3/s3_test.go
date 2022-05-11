package s3

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/mock"

	"github.com/connylabs/ingest/mocks"
)

func TestNew(t *testing.T) {
	t.Run("one object", func(t *testing.T) {
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		c := new(mocks.Client[*mocks.T])
		mc := new(mocks.MinioClient)
		_t := &mocks.T{MockID: "foo"}
		obj := new(mocks.Object)

		c.On("Download", mock.Anything, mock.Anything).Return(obj, nil).Once()

		obj.On("Len").Return(int64(64)).Once()
		obj.On("MimeType").Return("plain/text").Once()

		mc.On("PutObject", mock.Anything, "bucket", "prefix/foo", mock.Anything, int64(64), mock.Anything).Return(minio.UploadInfo{}, nil).Once().
			On("PutObject", mock.Anything, "bucket", "meta/foo.done", mock.Anything, int64(0), mock.Anything).Return(minio.UploadInfo{}, nil).Once()
		mc.On("StatObject", mock.Anything, "bucket", "prefix/foo", mock.Anything).Return(minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey"}).Once().
			On("StatObject", mock.Anything, "bucket", "meta/foo.done", mock.Anything).Return(minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey"}).Once()

		s := New[*mocks.T]("bucket", "prefix", "meta", mc, logger)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		u, err := s.Store(ctx, _t, c.Download)
		if err != nil {
			t.Error(err)
		}

		key := "s3://bucket/prefix/foo"
		if u.String() != key {
			t.Errorf("expected %q, got %q", key, u.String())
		}

		mc.AssertExpectations(t)
		obj.AssertExpectations(t)
		c.AssertExpectations(t)
	})
	t.Run("object exists", func(t *testing.T) {
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		c := new(mocks.Client[*mocks.T])
		mc := new(mocks.MinioClient)
		_t := &mocks.T{MockID: "foo"}
		obj := new(mocks.Object)

		mc.On("PutObject", mock.Anything, "bucket", "meta/foo.done", mock.Anything, int64(0), mock.Anything).Return(minio.UploadInfo{}, nil).Once()
		mc.On("StatObject", mock.Anything, "bucket", "prefix/foo", mock.Anything).Return(minio.ObjectInfo{}, nil).Once().
			On("StatObject", mock.Anything, "bucket", "meta/foo.done", mock.Anything).Return(minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey"}).Once()

		s := New[*mocks.T]("bucket", "prefix", "meta", mc, logger)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		u, err := s.Store(ctx, _t, c.Download)
		if err != nil {
			t.Error(err)
		}

		key := "s3://bucket/prefix/foo"
		if u.String() != key {
			t.Errorf("expected %q, got %q", key, u.String())
		}

		mc.AssertExpectations(t)
		obj.AssertExpectations(t)
		c.AssertExpectations(t)
	})
	t.Run("meta object exists", func(t *testing.T) {
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		c := new(mocks.Client[*mocks.T])
		mc := new(mocks.MinioClient)
		_t := &mocks.T{MockID: "foo"}
		obj := new(mocks.Object)

		mc.On("StatObject", mock.Anything, "bucket", "meta/foo.done", mock.Anything).Return(minio.ObjectInfo{}, nil).Once()

		s := New[*mocks.T]("bucket", "prefix", "meta", mc, logger)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		u, err := s.Store(ctx, _t, c.Download)
		if err != nil {
			t.Error(err)
		}

		key := "s3://bucket/prefix/foo"
		if u.String() != key {
			t.Errorf("expected %q, got %q", key, u.String())
		}

		mc.AssertExpectations(t)
		obj.AssertExpectations(t)
		c.AssertExpectations(t)
	})
	t.Run("one object no done", func(t *testing.T) {
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		c := new(mocks.Client[*mocks.T])
		mc := new(mocks.MinioClient)
		_t := &mocks.T{MockID: "foo"}
		obj := new(mocks.Object)

		c.On("Download", mock.Anything, mock.Anything).Return(obj, nil).Once()

		obj.On("Len").Return(int64(64)).Once()
		obj.On("MimeType").Return("plain/text").Once()

		mc.On("PutObject", mock.Anything, "bucket", "prefix/foo", mock.Anything, int64(64), mock.Anything).Return(minio.UploadInfo{}, nil).Once()
		mc.On("StatObject", mock.Anything, "bucket", "prefix/foo", mock.Anything).Return(minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey"}).Once()

		s := New[*mocks.T]("bucket", "prefix", "", mc, logger)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		u, err := s.Store(ctx, _t, c.Download)
		if err != nil {
			t.Error(err)
		}

		key := "s3://bucket/prefix/foo"
		if u.String() != key {
			t.Errorf("expected %q, got %q", key, u.String())
		}

		mc.AssertExpectations(t)
		obj.AssertExpectations(t)
		c.AssertExpectations(t)
	})
	t.Run("one object no done", func(t *testing.T) {
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		c := new(mocks.Client[*mocks.T])
		mc := new(mocks.MinioClient)
		_t := &mocks.T{MockID: "foo"}
		obj := new(mocks.Object)

		mc.On("StatObject", mock.Anything, "bucket", "prefix/foo", mock.Anything).Return(minio.ObjectInfo{}, nil).Once()

		s := New[*mocks.T]("bucket", "prefix", "", mc, logger)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		u, err := s.Store(ctx, _t, c.Download)
		if err != nil {
			t.Error(err)
		}

		key := "s3://bucket/prefix/foo"
		if u.String() != key {
			t.Errorf("expected %q, got %q", key, u.String())
		}

		mc.AssertExpectations(t)
		obj.AssertExpectations(t)
		c.AssertExpectations(t)
	})
}

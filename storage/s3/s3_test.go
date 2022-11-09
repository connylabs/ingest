package s3

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/mock"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/mocks"
)

func TestStore(t *testing.T) {
	t.Run("using meta objects", func(t *testing.T) {
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		c := new(mocks.Client)
		mc := new(mocks.MinioClient)

		_t := ingest.NewCodec("foo", "bar")
		obj := new(mocks.Object)

		c.On("Download", mock.Anything, mock.Anything).Return(obj, nil).Once()

		obj.On("Len").Return(int64(64)).Once()
		obj.On("MimeType").Return("plain/text").Once()

		mc.On("PutObject", mock.Anything, "bucket", "prefix/bar", mock.Anything, int64(64), mock.Anything).Return(minio.UploadInfo{}, nil).Once().
			On("PutObject", mock.Anything, "bucket", "meta/bar.done", mock.Anything, int64(0), mock.Anything).Return(minio.UploadInfo{}, nil).Once()

		s := New("bucket", "prefix", "meta", mc, logger)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		u, err := s.Store(ctx, *_t, c.Download)
		if err != nil {
			t.Error(err)
		}

		key := "s3://bucket/prefix/bar"
		if u.String() != key {
			t.Errorf("expected %q, got %q", key, u.String())
		}

		mc.AssertExpectations(t)
		obj.AssertExpectations(t)
		c.AssertExpectations(t)
	})
	t.Run("not using meta objects", func(t *testing.T) {
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		c := new(mocks.Client)
		mc := new(mocks.MinioClient)
		_t := ingest.NewCodec("foo", "bar")
		obj := new(mocks.Object)

		c.On("Download", mock.Anything, mock.Anything).Return(obj, nil).Once()

		obj.On("Len").Return(int64(64)).Once()
		obj.On("MimeType").Return("plain/text").Once()

		mc.On("PutObject", mock.Anything, "bucket", "prefix/bar", mock.Anything, int64(64), mock.Anything).Return(minio.UploadInfo{}, nil).Once()

		s := New("bucket", "prefix", "", mc, logger)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		u, err := s.Store(ctx, *_t, c.Download)
		if err != nil {
			t.Error(err)
		}

		key := "s3://bucket/prefix/bar"
		if u.String() != key {
			t.Errorf("expected %q, got %q", key, u.String())
		}

		mc.AssertExpectations(t)
		obj.AssertExpectations(t)
		c.AssertExpectations(t)
	})
}

func TestStat(t *testing.T) {
	t.Run("no object, no meta object", func(t *testing.T) {
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		c := new(mocks.Client)
		mc := new(mocks.MinioClient)
		_t := ingest.NewCodec("foo", "bar")
		obj := new(mocks.Object)

		mc.On("StatObject", mock.Anything, "bucket", "prefix/bar", mock.Anything).Return(minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey"}).Once().
			On("StatObject", mock.Anything, "bucket", "meta/bar.done", mock.Anything).Return(minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey"}).Once()

		s := New("bucket", "prefix", "meta", mc, logger)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		o, err := s.Stat(ctx, *_t)
		if !os.IsNotExist(err) {
			t.Errorf("expected error to satisfy os.IsNotExist, got %v", err)
		}

		if o != nil {
			t.Errorf("expected nil, got %v", o)
		}

		mc.AssertExpectations(t)
		obj.AssertExpectations(t)
		c.AssertExpectations(t)
	})
	t.Run("object, no meta object", func(t *testing.T) {
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		c := new(mocks.Client)
		mc := new(mocks.MinioClient)
		_t := ingest.NewCodec("foo", "bar")
		obj := new(mocks.Object)

		mc.On("PutObject", mock.Anything, "bucket", "meta/bar.done", mock.Anything, int64(0), mock.Anything).Return(minio.UploadInfo{}, nil).Once()
		mc.On("StatObject", mock.Anything, "bucket", "prefix/bar", mock.Anything).Return(minio.ObjectInfo{}, nil).Once().
			On("StatObject", mock.Anything, "bucket", "meta/bar.done", mock.Anything).Return(minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey"}).Once()

		s := New("bucket", "prefix", "meta", mc, logger)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		o, err := s.Stat(ctx, *_t)
		if err != nil {
			t.Error(err)
		}

		key := "s3://bucket/prefix/bar"
		if o.URI != key {
			t.Errorf("expected %q, got %q", key, o.URI)
		}

		mc.AssertExpectations(t)
		obj.AssertExpectations(t)
		c.AssertExpectations(t)
	})
	t.Run("no object, meta object", func(t *testing.T) {
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		c := new(mocks.Client)
		mc := new(mocks.MinioClient)
		_t := ingest.NewCodec("foo", "bar")
		obj := new(mocks.Object)

		mc.On("StatObject", mock.Anything, "bucket", "meta/bar.done", mock.Anything).Return(minio.ObjectInfo{}, nil).Once()

		s := New("bucket", "prefix", "meta", mc, logger)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		o, err := s.Stat(ctx, *_t)
		if err != nil {
			t.Error(err)
		}

		key := "s3://bucket/prefix/bar"
		if o.URI != key {
			t.Errorf("expected %q, got %q", key, o.URI)
		}

		mc.AssertExpectations(t)
		obj.AssertExpectations(t)
		c.AssertExpectations(t)
	})
	t.Run("object exists", func(t *testing.T) {
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		c := new(mocks.Client)
		mc := new(mocks.MinioClient)
		_t := ingest.NewCodec("foo", "bar")
		obj := new(mocks.Object)

		mc.On("StatObject", mock.Anything, "bucket", "prefix/bar", mock.Anything).Return(minio.ObjectInfo{}, nil).Once()

		s := New("bucket", "prefix", "", mc, logger)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		o, err := s.Stat(ctx, *_t)
		if err != nil {
			t.Error(err)
		}

		key := "s3://bucket/prefix/bar"
		if o.URI != key {
			t.Errorf("expected %q, got %q", key, o.URI)
		}

		mc.AssertExpectations(t)
		obj.AssertExpectations(t)
		c.AssertExpectations(t)
	})
	t.Run("no object", func(t *testing.T) {
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		c := new(mocks.Client)
		mc := new(mocks.MinioClient)
		_t := ingest.NewCodec("foo", "bar")
		obj := new(mocks.Object)

		mc.On("StatObject", mock.Anything, "bucket", "prefix/bar", mock.Anything).Return(minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey"}).Once()

		s := New("bucket", "prefix", "", mc, logger)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		o, err := s.Stat(ctx, *_t)
		if !os.IsNotExist(err) {
			t.Errorf("expected error to satisfy os.IsNotExist, got %v", err)
		}

		if o != nil {
			t.Errorf("expected nil, got %v", o)
		}

		mc.AssertExpectations(t)
		obj.AssertExpectations(t)
		c.AssertExpectations(t)
	})
}

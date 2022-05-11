package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"path"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/log/level"
	"github.com/minio/minio-go/v7"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

// MinioClient must be implemented by the storage client.
// The minio.Client implements this interface.
type MinioClient interface {
	PutObject(context.Context, string, string, io.Reader, int64, minio.PutObjectOptions) (minio.UploadInfo, error)
	StatObject(context.Context, string, string, minio.StatObjectOptions) (minio.ObjectInfo, error)
}

type minioStorage[T ingest.Identifiable] struct {
	bucket          string
	mc              MinioClient
	l               log.Logger
	prefix          string
	metafilesPrefix string
	useDone         bool
}

// New returns a new Storage that can store objects to S3.
func New[T ingest.Identifiable](bucket string, prefix string, metafilesPrefix string, mc MinioClient, l log.Logger) storage.Storage[T] {
	return &minioStorage[T]{
		bucket:          bucket,
		mc:              mc,
		l:               l,
		prefix:          prefix,
		metafilesPrefix: metafilesPrefix,
		useDone:         metafilesPrefix != "",
	}
}

func (ms *minioStorage[T]) Stat(ctx context.Context, element T) (*storage.ObjectInfo, error) {
	synced, done, err := ms.isObjectSynced(ctx, element.ID(), ms.useDone)
	if err != nil {
		return nil, err
	}

	if !synced {
		return nil, fs.ErrNotExist
	}

	// If the file exists but the done file does not,
	// let's patch this up.
	if !done && ms.useDone {
		if _, err := ms.mc.PutObject(ctx, ms.bucket, path.Join(ms.metafilesPrefix, doneKey(element.ID())), bytes.NewReader(make([]byte, 0)), 0, minio.PutObjectOptions{ContentType: "text/plain"}); err != nil {
			return nil, fmt.Errorf("failed to create missing meta object for existing file: %w", err)
		}
	}

	return &storage.ObjectInfo{URI: ms.url(element).String()}, nil
}

func (ms *minioStorage[T]) Store(ctx context.Context, element T, download func(context.Context, T) (ingest.Object, error)) (*url.URL, error) {
	u := ms.url(element)

	object, err := download(ctx, element)
	if err != nil {
		return nil, fmt.Errorf("failed to get message %s: %w", element.ID(), err)
	}

	if _, err := ms.mc.PutObject(
		ctx,
		ms.bucket,
		u.Path,
		object,
		object.Len(),
		minio.PutObjectOptions{ContentType: object.MimeType()}, // I guess we can remove the mime type detection because we always use tar.gz files.
	); err != nil {
		return nil, err
	}

	if ms.useDone {
		if _, err := ms.mc.PutObject(ctx, ms.bucket, path.Join(ms.metafilesPrefix, doneKey(element.ID())), bytes.NewReader(make([]byte, 0)), 0, minio.PutObjectOptions{ContentType: "text/plain"}); err != nil {
			return nil, fmt.Errorf("failed to create matching meta object for uploaded file: %w", err)
		}
	}

	return u, nil
}

func (ms *minioStorage[T]) url(element T) *url.URL {
	return &url.URL{
		Scheme: "s3",
		Host:   ms.bucket,
		Path:   path.Join(ms.prefix, element.ID()),
	}
}

func (ms *minioStorage[T]) isObjectSynced(ctx context.Context, name string, checkDone bool) (bool, bool, error) {
	nameToCheck := path.Join(ms.prefix, name)
	if checkDone {
		nameToCheck = path.Join(ms.metafilesPrefix, doneKey(name))
	}

	_, err := ms.mc.StatObject(ctx, ms.bucket, nameToCheck, minio.StatObjectOptions{})
	if err == nil {
		level.Debug(ms.l).Log("msg", "object exists in object storage", "object", nameToCheck)
		return true, checkDone, nil
	}

	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		level.Debug(ms.l).Log("msg", "object does not exist in object storage", "object", nameToCheck)
		if checkDone {
			return ms.isObjectSynced(ctx, name, false)
		}
		return false, checkDone, nil
	}

	level.Error(ms.l).Log("msg", "failed to check for object in object storage", "bucket", ms.bucket, "object", nameToCheck, "err", err.Error())
	return false, checkDone, err
}

func doneKey(name string) string {
	return fmt.Sprintf("%s.done", name)
}

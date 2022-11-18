package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"path"

	"github.com/go-kit/log"
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

type minioStorage struct {
	bucket          string
	mc              MinioClient
	l               log.Logger
	prefix          string
	metafilesPrefix string
	useDone         bool
}

// New returns a new Storage that can store objects to S3.
func New(bucket, prefix, metafilesPrefix string, mc MinioClient, useDone bool, l log.Logger) storage.Storage {
	return &minioStorage{
		bucket:          bucket,
		mc:              mc,
		l:               l,
		prefix:          prefix,
		metafilesPrefix: metafilesPrefix,
		useDone:         useDone,
	}
}

func (ms *minioStorage) Stat(ctx context.Context, element ingest.Codec) (*storage.ObjectInfo, error) {
	synced, done, err := ms.isObjectSynced(ctx, element.Name, ms.useDone)
	if err != nil {
		return nil, err
	}

	if !synced {
		return nil, fs.ErrNotExist
	}

	// If the file exists but the done file does not,
	// let's patch this up.
	if !done && ms.useDone {
		if _, err := ms.mc.PutObject(ctx, ms.bucket, path.Join(ms.metafilesPrefix, doneKey(element.Name)), bytes.NewReader(make([]byte, 0)), 0, minio.PutObjectOptions{ContentType: "text/plain"}); err != nil {
			return nil, fmt.Errorf("failed to create missing meta object for existing file: %w", err)
		}
	}

	return &storage.ObjectInfo{URI: ms.url(element).String()}, nil
}

func (ms *minioStorage) Store(ctx context.Context, element ingest.Codec, obj ingest.Object) (*url.URL, error) {
	u := ms.url(element)

	if _, err := ms.mc.PutObject(
		ctx,
		ms.bucket,
		u.Path,
		obj.Reader,
		obj.Len,
		minio.PutObjectOptions{ContentType: obj.MimeType}, // I guess we can remove the mime type detection because we always use tar.gz files.
	); err != nil {
		return nil, err
	}

	if ms.useDone {
		if _, err := ms.mc.PutObject(ctx, ms.bucket, path.Join(ms.metafilesPrefix, doneKey(element.Name)), bytes.NewReader(make([]byte, 0)), 0, minio.PutObjectOptions{ContentType: "text/plain"}); err != nil {
			return nil, fmt.Errorf("failed to create matching meta object for uploaded file: %w", err)
		}
	}

	return u, nil
}

func (ms *minioStorage) url(element ingest.Codec) *url.URL {
	return &url.URL{
		Scheme: "s3",
		Host:   ms.bucket,
		Path:   path.Join(ms.prefix, element.Name),
	}
}

func (ms *minioStorage) isObjectSynced(ctx context.Context, name string, checkDone bool) (bool, bool, error) {
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

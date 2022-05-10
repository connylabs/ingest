package dequeue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/minio/minio-go/v7"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/connylabs/ingest"
)

// Make sure that the minio Client implements the MinioClient interface.
var _ MinioClient = &minio.Client{}

// MinioClient must be implemented by the storage client.
// The minio.Client implements this interface.
type MinioClient interface {
	PutObject(context.Context, string, string, io.Reader, int64, minio.PutObjectOptions) (minio.UploadInfo, error)
	StatObject(context.Context, string, string, minio.StatObjectOptions) (minio.ObjectInfo, error)
}

type dequeuer[T ingest.Identifiable] struct {
	bucket                      string
	c                           ingest.Client[T]
	mc                          MinioClient
	l                           log.Logger
	r                           prometheus.Registerer
	q                           ingest.Queue
	cleanUp                     bool
	useDone                     bool
	webhookURL                  string
	batchSize                   int
	streamName                  string
	consumerName                string
	subjectName                 string
	dequeuerErrorCounter        prometheus.Counter
	dequeuerAttemptCounter      prometheus.Counter
	bucketFilesPrefix           string
	bucketMetafilesPrefix       string
	webhookRequestsTotalCounter *prometheus.CounterVec
}

// New creates a new ingest.Dequeuer.
func New[T ingest.Identifiable](bucket, bucketFilesPrefix, bucketMetafilesPrefix, webhookURL string, c ingest.Client[T], mc MinioClient,
	q ingest.Queue, streamName, consumerName, subjectName string, batchSize int, cleanUp bool, l log.Logger, r prometheus.Registerer,
) ingest.Dequeuer {
	if l == nil {
		l = log.NewNopLogger()
	}
	return &dequeuer[T]{
		bucket:                bucket,
		c:                     c,
		mc:                    mc,
		l:                     l,
		r:                     r,
		q:                     q,
		cleanUp:               cleanUp,
		useDone:               bucketMetafilesPrefix != "",
		webhookURL:            webhookURL,
		batchSize:             batchSize,
		streamName:            streamName,
		consumerName:          consumerName,
		subjectName:           subjectName,
		bucketFilesPrefix:     bucketFilesPrefix,
		bucketMetafilesPrefix: bucketMetafilesPrefix,

		dequeuerErrorCounter: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "dequeuer_errors_total",
			Help: "Number of errors that occured while syncing items.",
		}),
		dequeuerAttemptCounter: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "dequeuer_attempts_total",
			Help: "Number of item sync attempts.",
		}),
		webhookRequestsTotalCounter: promauto.With(r).NewCounterVec(
			prometheus.CounterOpts{
				Name: "webhook_http_client_requests_total",
				Help: "The number webhook HTTP requests.",
			}, []string{"result"}),
	}
}

func (d *dequeuer[T]) Dequeue(ctx context.Context) error {
	sub, err := d.q.PullSubscribe(d.subjectName, d.consumerName, nats.Bind(d.streamName, d.consumerName))
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return sub.Close()
		default:
		}

		tctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		msgs, err := sub.Pop(d.batchSize, nats.Context(tctx))
		cancel()
		if err != nil {
			level.Error(d.l).Log("msg", "failed to dequeue messages from queue", "err", err.Error())
			continue
		}
		level.Info(d.l).Log("msg", fmt.Sprintf("dequeued %d messages from queue", len(msgs)))

		uris := make([]string, 0, d.batchSize)
		for _, raw := range msgs {
			var job T
			if err := json.Unmarshal(raw.Data, &job); err != nil {
				level.Error(d.l).Log("msg", "failed to marshal message", "err", err.Error())
				continue
			}
			k, err := d.processMsgData(ctx, job)
			if err != nil {
				level.Error(d.l).Log("msg", "failed to process message", "err", err.Error())
				continue
			}
			level.Info(d.l).Log("msg", "successfully processed message", "data", job)
			if err := raw.AckSync(); err != nil {
				level.Error(d.l).Log("msg", "failed to ack message", "err", err.Error())
				continue
			}
			level.Debug(d.l).Log("msg", "acked message", "data", job)
			uris = append(uris, fmt.Sprintf("s3://%s/%s", d.bucket, k))
		}

		if d.webhookURL != "" {
			if err := d.callWebhook(ctx, uris); err != nil {
				d.webhookRequestsTotalCounter.WithLabelValues("error").Inc()
				level.Warn(d.l).Log("warn", "failed to call a webhook", "msg", err.Error())
				continue
			}
			d.webhookRequestsTotalCounter.WithLabelValues("success").Inc()
		}
	}
}

func (d *dequeuer[T]) processMsgData(ctx context.Context, job T) (string, error) {
	d.dequeuerAttemptCounter.Inc()

	s3Key := path.Join(
		d.bucketFilesPrefix,
		job.ID(),
	)

	operation := func() error {
		synced, done, err := d.isObjectSynced(ctx, job.ID(), d.useDone)
		if err != nil {
			return err
		}

		if !synced {
			obj, err := d.c.Download(ctx, job)
			if err != nil {
				return fmt.Errorf("failed to get message %s: %w", job.ID(), err)
			}
			if _, err := d.mc.PutObject(
				ctx,
				d.bucket,
				s3Key,
				obj,
				obj.Len(),
				minio.PutObjectOptions{ContentType: obj.MimeType()}, // I guess we can remove the mime type detection because we always use tar.gz files.
			); err != nil {
				return err
			}
		}

		if !done && d.useDone {
			if _, err := d.mc.PutObject(ctx, d.bucket, path.Join(d.bucketMetafilesPrefix, doneKey(job.ID())), bytes.NewReader(make([]byte, 0)), 0, minio.PutObjectOptions{ContentType: "text/plain"}); err != nil {
				return err
			}
		}

		if d.cleanUp {
			return d.c.CleanUp(ctx, job)
		}

		return nil
	}

	bctx := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	if err := backoff.Retry(operation, bctx); err != nil {
		d.dequeuerErrorCounter.Inc()
		return "", err
	}

	return s3Key, nil
}

func (d *dequeuer[T]) isObjectSynced(ctx context.Context, name string, checkDone bool) (bool, bool, error) {
	nameToCheck := path.Join(d.bucketFilesPrefix, name)
	if checkDone {
		nameToCheck = path.Join(d.bucketMetafilesPrefix, doneKey(name))
	}

	_, err := d.mc.StatObject(ctx, d.bucket, nameToCheck, minio.StatObjectOptions{})
	if err == nil {
		level.Debug(d.l).Log("msg", "object exists in object storage", "object", nameToCheck)
		return true, checkDone, nil
	}

	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		level.Debug(d.l).Log("msg", "object does not exist in object storage", "object", nameToCheck)
		if checkDone {
			return d.isObjectSynced(ctx, name, false)
		}
		return false, checkDone, nil
	}

	level.Error(d.l).Log("msg", "failed to check for object in object storage", "bucket", d.bucket, "object", nameToCheck, "err", err.Error())
	return false, checkDone, err
}

func (d *dequeuer[Client]) callWebhook(ctx context.Context, data []string) error {
	requestData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", d.webhookURL, bytes.NewBuffer(requestData))
	if err != nil {
		return err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	defer io.Copy(ioutil.Discard, res.Body)

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("webhook request failed with status code: %d", res.StatusCode)
	}

	return nil
}

func doneKey(name string) string {
	return fmt.Sprintf("%s.done", name)
}

package dequeue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/minio/minio-go/v7"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/mietright/ingest"
)

type dequeuer[T ingest.Identifiable] struct {
	bucket                      string
	c                           ingest.Client[T]
	mc                          *minio.Client
	l                           log.Logger
	r                           prometheus.Registerer
	q                           ingest.Queue
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
func New[T ingest.Identifiable](bucket, bucketFilesPrefix, bucketMetafilesPrefix, webhookURL string, c ingest.Client[T], mc *minio.Client,
	q ingest.Queue, streamName, consumerName, subjectName string, batchSize int, l log.Logger, r prometheus.Registerer,
) ingest.Dequeuer {
	return &dequeuer[T]{
		bucket:                bucket,
		c:                     c,
		mc:                    mc,
		l:                     l,
		r:                     r,
		q:                     q,
		webhookURL:            webhookURL,
		batchSize:             batchSize,
		streamName:            streamName,
		consumerName:          consumerName,
		subjectName:           subjectName,
		bucketFilesPrefix:     bucketFilesPrefix,
		bucketMetafilesPrefix: bucketMetafilesPrefix,

		dequeuerErrorCounter: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "dequeuer_errors_total",
			Help: "Number of errors that occured while syncing documents.",
		}),
		dequeuerAttemptCounter: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "dequeuer_attempts_total",
			Help: "Number of document sync attempts.",
		}),
		webhookRequestsTotalCounter: promauto.With(r).NewCounterVec(
			prometheus.CounterOpts{
				Name: "webhook_http_client_requests_total",
				Help: "The number webhook HTTP requests.",
			}, []string{"result"}),
	}
}

func (d dequeuer[T]) Runner(ctx context.Context) func() error {
	return func() error {
		level.Info(d.l).Log("msg", "starting the dequeuer")
		if err := d.Dequeue(ctx); err != nil {
			return fmt.Errorf("dequeuer exited unexpectedly: %w", err)
		}
		return nil
	}
}

func (s *dequeuer[T]) Dequeue(ctx context.Context) error {
	sub, err := s.q.PullSubscribe(s.subjectName, s.consumerName, nats.Bind(s.streamName, s.consumerName))
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
		msgs, err := sub.Pop(s.batchSize, nats.Context(tctx))
		cancel()
		if err != nil {
			level.Error(s.l).Log("msg", "failed to dequeue messages from queue", "err", err.Error())
			continue
		}
		level.Info(s.l).Log("msg", fmt.Sprintf("dequeued %d messages from queue", len(msgs)))

		uris := make([]string, 0, s.batchSize)
		for _, raw := range msgs {
			var job T
			if err := json.Unmarshal(raw.Data, &job); err != nil {
				level.Error(s.l).Log("msg", "failed to marshal message", "err", err.Error())
				continue
			}
			k, err := s.processMsgData(ctx, job)
			if err != nil {
				level.Error(s.l).Log("msg", "failed to process message", "err", err.Error())
				continue
			}
			level.Info(s.l).Log("msg", "successfully processed message", "data", job)
			if err := raw.AckSync(); err != nil {
				level.Error(s.l).Log("msg", "failed to ack message", "err", err.Error())
				continue
			}
			level.Debug(s.l).Log("msg", "acked message", "data", job)
			uris = append(uris, fmt.Sprintf("s3://%s/%s", s.bucket, k))
		}

		if s.webhookURL != "" {
			if err := s.callWebhook(ctx, uris); err != nil {
				s.webhookRequestsTotalCounter.WithLabelValues("error").Inc()
				level.Warn(s.l).Log("warn", "failed to call a webhook", "msg", err.Error())
				continue
			}
			s.webhookRequestsTotalCounter.WithLabelValues("success").Inc()
		}
	}
}

func (s *dequeuer[T]) processMsgData(ctx context.Context, job T) (string, error) {
	s.dequeuerAttemptCounter.Inc()

	s3Key := prefixedObjectName(
		s.bucketFilesPrefix,
		job.ID(),
	)

	operation := func() error {
		if synced, marked, err := s.isObjectSynced(ctx, job.ID(), true); err != nil {
			return err
		} else if synced && !marked {
			return s.markObjectAsSynced(ctx, job.ID())
		} else if synced {
			return nil
		}

		obj, err := s.c.Download(ctx, job)
		if err != nil {
			return fmt.Errorf("failed to get message %s: %w", job.ID(), err)
		}
		if _, err := s.mc.PutObject(
			ctx,
			s.bucket,
			s3Key,
			obj,
			obj.Len(),
			minio.PutObjectOptions{ContentType: obj.MimeType()}, // I guess we can remove the mime type detection because we always use tar.gz files.
		); err != nil {
			return err
		}
		if err := s.markObjectAsSynced(ctx, s3Key); err != nil {
			return err
		}

		return nil
	}

	bctx := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	if err := backoff.Retry(operation, bctx); err != nil {
		s.dequeuerErrorCounter.Inc()
		return "", err
	}

	return s3Key, nil
}

func (s *dequeuer[T]) isObjectSynced(ctx context.Context, name string, checkMarked bool) (bool, bool, error) {
	nameToCheck := prefixedObjectName(s.bucketFilesPrefix, name)
	if checkMarked {
		nameToCheck = prefixedObjectName(s.bucketMetafilesPrefix, markedObjectName(name))
	}

	_, err := s.mc.StatObject(ctx, s.bucket, nameToCheck, minio.StatObjectOptions{})
	if err == nil {
		level.Debug(s.l).Log("msg", "object exists in object storage", "object", nameToCheck)
		return true, checkMarked, nil
	}

	noSuchKeyErr := minio.ToErrorResponse(err).Code == "NoSuchKey"
	if noSuchKeyErr {
		level.Debug(s.l).Log("msg", "object does not exist in object storage", "object", nameToCheck)
		if checkMarked {
			return s.isObjectSynced(ctx, name, false)
		}
		return false, checkMarked, nil
	}
	level.Error(s.l).Log("msg", "failed to check for object in object storage", "bucket", s.bucket, "object", nameToCheck, "err", err.Error())
	return false, checkMarked, err
}

func (s *dequeuer[Client]) markObjectAsSynced(ctx context.Context, name string) error {
	name = prefixedObjectName(s.bucketMetafilesPrefix, markedObjectName(name))
	_, err := s.mc.PutObject(ctx, s.bucket, name, bytes.NewReader(make([]byte, 0)), 0, minio.PutObjectOptions{ContentType: "text/plain"})

	return err
}

func (s *dequeuer[Client]) callWebhook(ctx context.Context, data []string) error {
	requestData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", s.webhookURL, bytes.NewBuffer(requestData))
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

func markedObjectName(name string) string {
	return fmt.Sprintf("%s.done", name)
}

func prefixedObjectName(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return fmt.Sprintf("%s/%s", prefix, name)
}

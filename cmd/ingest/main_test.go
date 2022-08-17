package main

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	"github.com/go-kit/log"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/nats-io/nats.go"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/connylabs/ingest/config"
	"github.com/connylabs/ingest/queue"
)

var (
	accessKeyID     = "AKIAIOSFODNN7EXAMPLE"
	secretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	natsInstance   e2e.Runnable
	minioInstance1 e2e.Runnable
	minioInstance2 e2e.Runnable

	mc1 *minio.Client
	mc2 *minio.Client
	js  nats.JetStreamContext

	l log.Logger
)

func setUp(t *testing.T) {
	requ := require.New(t)
	l = log.NewJSONLogger(os.Stdout)
	e, err := e2e.NewDockerEnvironment("main_e2e")
	requ.Nil(err)

	natsInstance = e.Runnable("nats").WithPorts(
		map[string]int{
			"nats": 4222,
			"http": 8222,
		}).Init(e2e.StartOptions{
		Image:     "nats:2.6.1",
		Command:   e2e.NewCommand("", "-js", "--http_port", "8222"),
		Readiness: e2e.NewHTTPReadinessProbe("http", "/", 200, 299),
	})
	minioInstance1 = e.Runnable("minio_1").WithPorts(
		map[string]int{
			"minio":   9000,
			"console": 9001,
		}).Init(e2e.StartOptions{
		Image:     "quay.io/minio/minio:RELEASE.2021-10-23T03-28-24Z",
		Command:   e2e.NewCommand("", "server", "/data", "--console-address", ":9001"),
		Readiness: e2e.NewHTTPReadinessProbe("minio", "/minio/health/ready", 200, 299),
		EnvVars: map[string]string{
			"MINIO_ROOT_USER":     accessKeyID,
			"MINIO_ROOT_PASSWORD": secretAccessKey,
		},
	})
	minioInstance2 = e.Runnable("minio_2").WithPorts(
		map[string]int{
			"minio":   9000,
			"console": 9001,
		}).Init(e2e.StartOptions{
		Image:     "quay.io/minio/minio:RELEASE.2021-10-23T03-28-24Z",
		Command:   e2e.NewCommand("", "server", "/data", "--console-address", ":9001"),
		Readiness: e2e.NewHTTPReadinessProbe("minio", "/minio/health/ready", 200, 299),
		EnvVars: map[string]string{
			"MINIO_ROOT_USER":     accessKeyID,
			"MINIO_ROOT_PASSWORD": secretAccessKey,
		},
	})

	requ.Nil(e2e.StartAndWaitReady(natsInstance, minioInstance1, minioInstance2))

	mc1, err = minio.New(minioInstance1.Endpoint("minio"), &minio.Options{
		Secure: false,
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
	})
	requ.Nil(err)
	mc2, err = minio.New(minioInstance2.Endpoint("minio"), &minio.Options{
		Secure: false,
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
	})
	requ.Nil(err)

	nc, err := nats.Connect(natsInstance.Endpoint("nats"))
	requ.Nil(err)

	js, err = nc.JetStream()
	requ.Nil(err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "nats-str",
		Subjects: []string{"ingest.*"},
	})
	requ.Nil(err)

	_, err = js.AddConsumer("nats-str", &nats.ConsumerConfig{
		Durable:   "nats-con",
		AckPolicy: nats.AckExplicitPolicy,
	})
	requ.Nil(err)

	t.Cleanup(
		func() {
			nc.Close()
			requ.Nil(natsInstance.Stop())
			requ.Nil(minioInstance1.Stop())
			requ.Nil(minioInstance2.Stop())
		})
}

func TestRunGroup(t *testing.T) {
	if v, ok := os.LookupEnv("E2E"); !ok || !(v == "1" || v == "true") {
		t.Skip("To enable this test, set the E2E environment variable to 1 or true")
	}
	setUp(t)

	tmpl, err := template.New("config").Parse(`sources:
- name: foo_1
  type: s3
  endpoint: {{ .Foo1Endpoint }}
  insecure: true
  bucket: source
  prefix: prefix/
  accessKeyID: {{ .AccessKeyID }}
  secretAccessKey: {{ .SecretAccessKey }}
- name: foo_2
  type: s3
  endpoint: {{ .Foo2Endpoint }}
  insecure: true
  bucket: source
  prefix: prefix/
  accessKeyID: {{ .AccessKeyID }}
  secretAccessKey: {{ .SecretAccessKey }}
destinations:
- name: bar_1
  type: s3
  endpoint: {{ .Foo2Endpoint }}
  insecure: true
  bucket: destination
  prefix: target_prefix/
  metafilesPrefix: meta/
  accessKeyID: {{ .AccessKeyID }}
  secretAccessKey: {{ .SecretAccessKey }}
- name: bar_2
  type: s3
  endpoint: {{ .Foo2Endpoint }}
  insecure: true
  bucket: destination
  prefix: target_prefix/
  metafilesPrefix: meta/
  accessKeyID: {{ .AccessKeyID }}
  secretAccessKey: {{ .SecretAccessKey }}
workflows:
- name: foo_1-bar_1
  source: foo_1
  destinations:
  - bar_1
  interval: 0
  cleanUp: true
  webhook: http://localhost:8080 
- name: foo_2-bar_1-bar_2
  source: foo_2
  destinations:
  - bar_1
  - bar_2
  interval: 0
  webhook: http://localhost:8080 
`)
	require.Nil(t, err)
	b := bytes.NewBuffer(nil)
	err = tmpl.Execute(b, struct {
		Foo2Endpoint    string
		Foo1Endpoint    string
		AccessKeyID     string
		SecretAccessKey string
	}{
		Foo1Endpoint:    minioInstance1.Endpoint("minio"),
		Foo2Endpoint:    minioInstance2.Endpoint("minio"),
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
	})
	require.Nil(t, err)

	rawConfig, err := io.ReadAll(b)
	require.Nil(t, err)

	c, err := config.New(rawConfig)
	require.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := prometheus.NewRegistry()

	q, err := queue.New(natsInstance.Endpoint("nats"), reg)
	require.Nil(t, err)

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		if err := q.Close(ctx); err != nil {
			assert.Nil(t, err)
		}
	}()

	mc1.MakeBucket(ctx, "source", minio.MakeBucketOptions{})
	mc2.MakeBucket(ctx, "source", minio.MakeBucketOptions{})

	mc1.MakeBucket(ctx, "destination", minio.MakeBucketOptions{})
	mc2.MakeBucket(ctx, "destination", minio.MakeBucketOptions{})

	content := "a file"
	buf := strings.NewReader(content)
	_, err = mc1.PutObject(ctx, "source", "prefix/file_1", buf, buf.Size(), minio.PutObjectOptions{})
	require.Nil(t, err)
	buf = strings.NewReader(content)
	_, err = mc2.PutObject(ctx, "source", "prefix/file_2", buf, buf.Size(), minio.PutObjectOptions{})
	require.Nil(t, err)
	{
		// enqueue
		var g run.Group
		appFlags := &flags{
			mode:            toPtr(enqueueMode),
			queueSubject:    toPtr("ingest"),
			pluginDirectory: toPtr(fmt.Sprintf("../../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)),
		}
		require.Nil(t, runGroup(ctx, &g, q, appFlags, c, l, reg))

		require.Nil(t, g.Run())
	}
	{
		// dequeue
		var g run.Group
		appFlags := &flags{
			mode:            toPtr(dequeueMode),
			queueSubject:    toPtr("ingest"),
			streamName:      toPtr("nats-str"),
			consumerName:    toPtr("nats-con"),
			pluginDirectory: toPtr(fmt.Sprintf("../../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)),
		}
		tctx, tcancel := context.WithTimeout(ctx, 10*time.Second)
		defer tcancel()
		require.Nil(t, runGroup(tctx, &g, q, appFlags, c, l, reg))

		go require.Nil(t, g.Run())

		for {
			err := func() error {
				_, err = mc1.GetObject(tctx, "destination", "target_prefix/file_1", minio.GetObjectOptions{})
				if err != nil {
					return err
				}
				_, err = mc1.GetObject(tctx, "destination", "target_prefix/file_2", minio.GetObjectOptions{})
				if err != nil {
					return err
				}
				_, err = mc2.GetObject(tctx, "destination", "target_prefix/file_2", minio.GetObjectOptions{})
				if err != nil {
					return err
				}
				if ok, _ := mc1.BucketExists(tctx, "source"); ok {
					// This bucket should be empty and we can delete it.
					err := mc1.RemoveBucket(tctx, "source")
					if err != nil {
						fmt.Println(err.Error())
						return err
					}
				}
				return nil
			}()

			if err == nil {
				break
			}

			ticker := time.NewTicker(time.Second)
			select {
			case <-ticker.C:
			case <-tctx.Done():
				t.Error(err)
				t.FailNow()
			}

		}
	}
}

func toPtr[T any](t T) *T {
	return &t
}

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
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

const (
	accessKeyID     = "AKIAIOSFODNN7EXAMPLE"
	secretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	subject         = "subject"
	stream          = "stream"
	consumer        = "consumer"
	minioImage      = "quay.io/minio/minio:RELEASE.2021-10-23T03-28-24Z"
	natsImage       = "nats:2.6.1"
)

func newMinioRunnable(e e2e.Environment, name string) e2e.Runnable {
	return e.Runnable(name).WithPorts(
		map[string]int{
			"minio":   9000,
			"console": 9001,
		}).Init(e2e.StartOptions{
		Image:     minioImage,
		Command:   e2e.NewCommand("", "server", "/data", "--console-address", ":9001"),
		Readiness: e2e.NewHTTPReadinessProbe("minio", "/minio/health/ready", 200, 299),
		EnvVars: map[string]string{
			"MINIO_ROOT_USER":     accessKeyID,
			"MINIO_ROOT_PASSWORD": secretAccessKey,
		},
	})
}

func newNATSRunnable(e e2e.Environment, name string) e2e.Runnable {
	return e.Runnable(name).WithPorts(
		map[string]int{
			"nats": 4222,
			"http": 8222,
		}).Init(e2e.StartOptions{
		Image:     natsImage,
		Command:   e2e.NewCommand("", "-js", "--http_port", "8222"),
		Readiness: e2e.NewHTTPReadinessProbe("http", "/", 200, 299),
	})
}

type s3File struct {
	data   []byte
	name   string
	prefix string
}

func setUpMinios(t *testing.T, e e2e.Environment, files map[string]map[string][]s3File) (map[string]e2e.Runnable, map[string]*minio.Client) {
	requ := require.New(t)
	clients := make(map[string]*minio.Client)
	runnables := make(map[string]e2e.Runnable)
	var rs []e2e.Runnable
	var err error
	for m := range files {
		rs = append(rs, newMinioRunnable(e, m))
		runnables[m] = rs[len(rs)-1]
	}
	requ.Nil(e2e.StartAndWaitReady(rs...))
	for m := range files {
		clients[m], err = minio.New(runnables[m].Endpoint("minio"), &minio.Options{
			Secure: false,
			Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		})
		requ.Nil(err)
	}
	for m, bs := range files {
		for b, fs := range bs {
			err = clients[m].MakeBucket(context.Background(), b, minio.MakeBucketOptions{})
			require.Nil(t, err)
			for _, f := range fs {
				buf := bytes.NewReader(f.data)
				_, err = clients[m].PutObject(context.Background(), b, path.Join(f.prefix, f.name), buf, buf.Size(), minio.PutObjectOptions{})
				require.Nil(t, err)

			}
		}
	}
	t.Cleanup(
		func() {
			for _, r := range rs {
				requ.Nil(r.Stop())
			}
		})

	return runnables, clients
}

func setUp(t *testing.T, files map[string]map[string][]s3File) (string, map[string]e2e.Runnable, map[string]*minio.Client) {
	requ := require.New(t)
	e, err := e2e.NewDockerEnvironment("main_e2e")
	requ.Nil(err)

	natsInstance := newNATSRunnable(e, "nats")
	requ.Nil(e2e.StartAndWaitReady(natsInstance))
	rs, mcs := setUpMinios(t, e, files)

	nc, err := nats.Connect(natsInstance.Endpoint("nats"))
	requ.Nil(err)

	js, err := nc.JetStream()
	requ.Nil(err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:      stream,
		Subjects:  []string{strings.Join([]string{subject, "*"}, ".")},
		Retention: nats.InterestPolicy,
	})
	requ.Nil(err)

	t.Cleanup(
		func() {
			nc.Close()
			requ.Nil(natsInstance.Stop())
		})
	return natsInstance.Endpoint("nats"), rs, mcs
}

func TestRunGroup(t *testing.T) {
	if v, ok := os.LookupEnv("E2E"); !ok || !(v == "1" || v == "true") {
		t.Skip("To enable this test, set the E2E environment variable to 1 or true")
	}
	files := map[string]map[string][]s3File{
		"minio_1": {
			"source": []s3File{
				{
					data:   []byte("a file"),
					name:   "file_1",
					prefix: "prefix",
				},
			},
		},
		"minio_2": {
			"destination": []s3File{},
			"source": []s3File{
				{
					data:   []byte("a file"),
					name:   "file_2",
					prefix: "prefix",
				},
			},
		},
	}
	natsEndpoint, rs, mcs := setUp(t, files)
	ensureFiles := map[string]map[string][]s3File{
		"minio_2": {
			"destination": []s3File{
				{
					data:   []byte("a file"),
					name:   "file_1",
					prefix: "prefix1",
				},
				{
					data:   []byte("a file"),
					name:   "file_2",
					prefix: "prefix1",
				},
				{
					data:   []byte("a file"),
					name:   "file_2",
					prefix: "prefix2",
				},
			},
		},
	}

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
  prefix: prefix1/
  metafilesPrefix: meta/
  accessKeyID: {{ .AccessKeyID }}
  secretAccessKey: {{ .SecretAccessKey }}
- name: bar_2
  type: s3
  endpoint: {{ .Foo2Endpoint }}
  insecure: true
  bucket: destination
  prefix: prefix2/
  metafilesPrefix: meta/
  accessKeyID: {{ .AccessKeyID }}
  secretAccessKey: {{ .SecretAccessKey }}
workflows:
- name: foo_1-bar_1
  source: foo_1
  destinations:
  - bar_1
  batchSize: 1
  interval: 300s
  cleanUp: true
  webhook: http://localhost:8080 
- name: foo_2-bar_1-bar_2
  source: foo_2
  destinations:
  - bar_1
  - bar_2
  batchSize: 1
  interval: 300s
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
		Foo1Endpoint:    rs["minio_1"].Endpoint("minio"),
		Foo2Endpoint:    rs["minio_2"].Endpoint("minio"),
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

	q, err := queue.New(natsEndpoint, reg)
	require.Nil(t, err)

	l := log.NewJSONLogger(os.Stdout)
	l = log.With(l, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		if err := q.Close(ctx); err != nil {
			assert.Nil(t, err)
		}
	}()

	var wg sync.WaitGroup
	var g run.Group
	tctx, tcancel := context.WithTimeout(ctx, 10*time.Second)
	defer tcancel()
	{
		// enqueue
		appFlags := &flags{
			logLevel:        toPtr(logLevelAll),
			mode:            toPtr(enqueueMode),
			subject:         toPtr(subject),
			pluginDirectory: toPtr(fmt.Sprintf("../../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)),
		}
		require.Nil(t, runGroup(tctx, &g, q, appFlags, c, l, reg))

		wg.Add(1)
		go func() {
			require.Nil(t, g.Run())
			wg.Done()
		}()
	}
	{
		// dequeue
		appFlags := &flags{
			mode:            toPtr(dequeueMode),
			subject:         toPtr(subject),
			stream:          toPtr(stream),
			consumer:        toPtr(consumer),
			pluginDirectory: toPtr(fmt.Sprintf("../../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)),
		}
		require.Nil(t, runGroup(tctx, &g, q, appFlags, c, l, reg))

		wg.Add(1)
		go func() {
			require.Nil(t, g.Run())
			wg.Done()
		}()

		for {
			err := func() error {
				for m, bs := range ensureFiles {
					for b, fs := range bs {
						for _, f := range fs {
							obj, err := mcs[m].GetObject(tctx, b, path.Join(f.prefix, f.name), minio.GetObjectOptions{})
							if err != nil {
								return err
							}
							if obj == nil {
								return errors.New("not found")
							}
							oi, err := obj.Stat()
							if err != nil {
								return err
							}
							if oi.Err != nil {
								return oi.Err
							}
						}
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
		tcancel()
		wg.Wait()
	}
}

func toPtr[T any](t T) *T {
	return &t
}

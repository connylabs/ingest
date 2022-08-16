package enqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/efficientgo/e2e"
	"github.com/go-kit/log"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/config"
	"github.com/connylabs/ingest/mocks"
	"github.com/connylabs/ingest/queue"
)

var (
	accessKeyID     = "AKIAIOSFODNN7EXAMPLE"
	secretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	prefix = "prefix/"

	natsInstance  e2e.Runnable
	minioInstance e2e.Runnable

	mc         *minio.Client
	bucketName string
	js         nats.JetStreamContext

	q   ingest.Queue
	en  ingest.Enqueuer
	reg *prometheus.Registry

	nexter ingest.Nexter
	l      log.Logger
)

func setUp(t *testing.T) {
	requ := require.New(t)
	e, err := e2e.NewDockerEnvironment("enqueue_e2e")
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
	minioInstance = e.Runnable("minio").WithPorts(
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

	requ.Nil(e2e.StartAndWaitReady(natsInstance, minioInstance))

	mc, err = minio.New(minioInstance.Endpoint("minio"), &minio.Options{
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
		Subjects: []string{"*"},
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
			requ.Nil(minioInstance.Stop())
		})
}

func TestEnqueueE2E(t *testing.T) {
	requ := require.New(t)
	if v, ok := os.LookupEnv("E2E"); !ok || !(v == "1" || v == "true") {
		t.Skip("To enable this test, set the E2E environment variable to 1 or true")
	}
	setUp(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	before := func(t *testing.T) {
		reg = prometheus.NewRegistry()

		err := mc.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		requ.Nil(err)

		q, err = queue.New(natsInstance.Endpoint("nats"), reg)
		requ.Nil(err)

		source := config.Source{
			Name: "foo",
			Type: "s3",
			Config: map[string]interface{}{
				"endpoint":        minioInstance.Endpoint("minio"),
				"insecure":        true,
				"bucket":          bucketName,
				"prefix":          prefix,
				"accessKeyID":     accessKeyID,
				"secretAccessKey": secretAccessKey,
			},
		}

		c := config.Config{
			Sources: []config.Source{source},
		}

		sources, _, err := c.ConfigurePlugins(ctx, "../bin/plugin/linux/amd64")
		requ.Nil(err)

		var ok bool
		nexter, ok = sources["foo"]
		requ.True(ok)

		l = log.NewJSONLogger(os.Stdout)
	}

	after := func(t *testing.T) {
		requ.Nil(mc.RemoveBucketWithOptions(ctx, bucketName, minio.RemoveBucketOptions{ForceDelete: true}))
		requ.Nil(js.PurgeStream("nats-str"))
	}
	for _, tc := range []struct {
		name   string
		runner func(t *testing.T)
		config func()
	}{
		{
			name: "one file",
			config: func() {
				bucketName = "bar"
			},
			runner: func(t *testing.T) {
				content := "a file"
				buf := strings.NewReader(content)
				_, err := mc.PutObject(ctx, bucketName, fmt.Sprintf("%sfile", prefix), buf, buf.Size(), minio.PutObjectOptions{})
				requ.Nil(err)
				require.Nil(t, en.Enqueue(ctx))

				{
					// test prom metrics
					ps, err := testutil.GatherAndLint(reg)
					require.Nil(t, err)
					assert.Len(t, ps, 0, ps)
					c, err := testutil.GatherAndCount(reg, "ingest_enqueue_attempts_total")
					require.Nil(t, err)
					assert.Equal(t, 1, c)

					c, err = testutil.GatherAndCount(reg, "ingest_queue_operations_total")
					require.Nil(t, err)
					assert.Equal(t, 1, c)
				}

				sub, err := q.PullSubscribe("sub", "nats-con", nats.Bind("nats-str", "nats-con"))
				require.Nil(t, err)

				msgs, err := sub.Pop(10)
				require.Nil(t, err)

				assert.Len(t, msgs, 1)
				var sc ingest.SimpleCodec
				err = json.NewDecoder(bytes.NewBuffer(msgs[0].Data)).Decode(&sc)
				assert.Nil(t, err)
				assert.Equal(t, ingest.SimpleCodec{XID: fmt.Sprintf("%sfile", prefix), XName: "file"}, sc)
			},
		},
		{
			name: "empty prefix",
			config: func() {
				bucketName = "foo"
				prefix = ""
			},
			runner: func(t *testing.T) {
				content := "a file"
				buf := strings.NewReader(content)
				_, err := mc.PutObject(ctx, bucketName, fmt.Sprintf("%sfile", prefix), buf, buf.Size(), minio.PutObjectOptions{})
				requ.Nil(err)
				require.Nil(t, en.Enqueue(ctx))
				sub, err := q.PullSubscribe("sub", "nats-con", nats.Bind("nats-str", "nats-con"))
				require.Nil(t, err)

				msgs, err := sub.Pop(10)
				require.Nil(t, err)

				assert.Len(t, msgs, 1)
				var sc ingest.SimpleCodec
				err = json.NewDecoder(bytes.NewBuffer(msgs[0].Data)).Decode(&sc)
				assert.Nil(t, err)
				assert.Equal(t, ingest.SimpleCodec{XID: "file", XName: "file"}, sc)
			},
		},
		{
			name: "two files",
			config: func() {
				bucketName = "abc"
				prefix = "prefixx/"
			},
			runner: func(t *testing.T) {
				for i := 0; i < 2; i++ {
					content := "a file"
					buf := strings.NewReader(content)
					_, err := mc.PutObject(ctx, bucketName, fmt.Sprintf("%sfile%d", prefix, i), buf, buf.Size(), minio.PutObjectOptions{})
					requ.Nil(err)
				}

				require.Nil(t, en.Enqueue(ctx))
				{
					// test prom metrics
					ps, err := testutil.GatherAndLint(reg)
					require.Nil(t, err)
					assert.Len(t, ps, 0, ps)
					c, err := testutil.GatherAndCount(reg, "ingest_enqueue_attempts_total")
					require.Nil(t, err)
					assert.Equal(t, 1, c)

					c, err = testutil.GatherAndCount(reg, "ingest_queue_operations_total")
					require.Nil(t, err)
					// This also is 1 because it counts the cardinality of the counter vec.
					assert.Equal(t, 1, c)

					err = testutil.GatherAndCompare(reg, strings.NewReader(
						`# HELP ingest_queue_operations_total The total number of queue operations.
# TYPE ingest_queue_operations_total counter
ingest_queue_operations_total{operation="publish",result="success"} 2
`,
					), "ingest_queue_operations_total")
					require.Nil(t, err)
					// This also is 1 because it counts the cardinality of the counter vec.
					assert.Equal(t, 1, c)

				}
				sub, err := q.PullSubscribe("sub", "nats-con", nats.Bind("nats-str", "nats-con"))
				require.Nil(t, err)

				msgs, err := sub.Pop(10)
				require.Nil(t, err)

				assert.Len(t, msgs, 2)
			},
		},
	} {
		tc.config()
		// Run before after config so, it uses the updated globals from config().
		before(t)
		var err error
		en, err = New(nexter, "sub", q, reg, l, WithBackoff(false))
		requ.Nil(err)

		tc.runner(t)

		after(t)
	}
}

func TestEnqueue(t *testing.T) {
	for _, tc := range []struct {
		name   string
		expect func() (*mocks.Queue, *mocks.Nexter, *mocks.T)
	}{
		{
			name: "nexter returns EOF",
			expect: func() (*mocks.Queue, *mocks.Nexter, *mocks.T) {
				q := new(mocks.Queue)
				n := new(mocks.Nexter)
				n.On("Reset", mock.Anything).Return(nil).Once()
				n.On("Next", mock.Anything).Return(nil, io.EOF).Once()
				return q, n, nil
			},
		},
		{
			name: "nexter returns nil",
			expect: func() (*mocks.Queue, *mocks.Nexter, *mocks.T) {
				q := new(mocks.Queue)
				n := new(mocks.Nexter)
				n.On("Reset", mock.Anything).Return(nil).Once()
				n.On("Next", mock.Anything).Return(nil, io.EOF).Once()
				return q, n, nil
			},
		},
		{
			name: "one entry",
			expect: func() (*mocks.Queue, *mocks.Nexter, *mocks.T) {
				t := &mocks.T{MockID: "foo"}
				data, _ := ingest.NewCodec(t).Marshal()
				q := new(mocks.Queue)
				q.On("Publish", "sub", data).Return(nil).Once()
				n := new(mocks.Nexter)
				n.On("Reset", mock.Anything).Return(nil).
					On("Next", mock.Anything).Once().Return(t, nil).
					On("Next", mock.Anything).Return(nil, io.EOF)
				return q, n, t
			},
		},
		{
			name: "two entries",
			expect: func() (*mocks.Queue, *mocks.Nexter, *mocks.T) {
				t := &mocks.T{MockID: "foo"}
				data, _ := ingest.NewCodec(t).Marshal()
				t2 := &mocks.T{MockID: "foo2"}
				data2, _ := ingest.NewCodec(t2).Marshal()
				q := new(mocks.Queue)
				q.
					On("Publish", "sub", data).Return(nil).Once().
					On("Publish", "sub", data2).Return(nil).Once()
				n := new(mocks.Nexter)
				n.
					On("Reset", mock.Anything).Return(nil).Once().
					On("Next", mock.Anything).Return(t, nil).Once().
					On("Next", mock.Anything).Return(t2, nil).Once().
					On("Next", mock.Anything).Return(nil, io.EOF).Once()
				return q, n, t
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
			ctx := context.Background()

			q, n, _ := tc.expect()

			e, err := New(n, "sub", q, reg, logger)
			if err != nil {
				t.Error(err)
			}
			if err := e.Enqueue(ctx); err != nil {
				t.Error(err)
			}
			n.AssertExpectations(t)
			q.AssertExpectations(t)

			lps, err := testutil.GatherAndLint(reg)
			require.Nil(t, err)
			assert.Empty(t, lps)

			c, err := testutil.GatherAndCount(reg, "ingest_enqueue_attempts_total")
			require.Nil(t, err)
			assert.Equal(t, 1, c)
		})
	}
	t.Run("failed to reset", func(t *testing.T) {
		assert := assert.New(t)
		reg := prometheus.NewRegistry()
		logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		ctx := context.Background()

		q := new(mocks.Queue)
		n := new(mocks.Nexter)
		eerr := errors.New("some error")
		n.On("Reset", mock.Anything).Return(eerr).Once()

		e, err := New(n, "sub", q, reg, logger)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(eerr, e.Enqueue(ctx))

		n.AssertExpectations(t)
		q.AssertExpectations(t)
		c, err := testutil.GatherAndCount(reg, "ingest_enqueue_attempts_total")
		require.Nil(t, err)
		assert.Equal(1, c)
	})
}

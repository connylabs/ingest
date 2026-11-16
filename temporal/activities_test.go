package temporal

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/mocks"
	"github.com/connylabs/ingest/storage"
)

type testSource struct {
	*mocks.Nexter
	*mocks.Client
}

func newTestActivities(t *testing.T, s Source, st storage.Storage, webhook string) (*testsuite.TestActivityEnvironment, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	a, err := NewActivities([]Workflow{{Name: "w", SourceName: "src", Source: s, Storage: st, Webhook: webhook}}, reg, nil)
	require.NoError(t, err)
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(a)
	return env, reg
}

func TestListItems(t *testing.T) {
	n := mocks.NewNexter(t)
	n.On("Reset", mock.Anything).Return(nil).Once()
	n.On("Next", mock.Anything).Return(&ingest.Codec{ID: "1"}, nil).Once()
	n.On("Next", mock.Anything).Return(&ingest.Codec{ID: "2"}, nil).Once()
	n.On("Next", mock.Anything).Return(nil, io.EOF).Once()
	env, reg := newTestActivities(t, testSource{Nexter: n, Client: mocks.NewClient(t)}, mocks.NewStorage(t), "")

	v, err := env.ExecuteActivity(ListItemsActivityName, "w")
	require.NoError(t, err)
	var items []ingest.Codec
	require.NoError(t, v.Get(&items))
	require.Equal(t, []string{"1", "2"}, []string{items[0].ID, items[1].ID})
	require.Equal(t, 1.0, testutil.ToFloat64(counter(t, reg, "ingest_enqueue_attempts_total", "success")))
}

func TestTransfer(t *testing.T) {
	item := ingest.Codec{ID: "1", Name: "1.tar"}

	t.Run("downloads and stores a new item", func(t *testing.T) {
		c := mocks.NewClient(t)
		s := mocks.NewStorage(t)
		obj := &ingest.Object{MimeType: "application/x-tar", Len: 3, Reader: bytes.NewBufferString("tar")}
		u, _ := url.Parse("s3://b/1.tar")
		s.On("Stat", mock.Anything, item).Return(nil, os.ErrNotExist).Once()
		c.On("Download", mock.Anything, item).Return(obj, nil).Once()
		s.On("Store", mock.Anything, item, *obj).Return(u, nil).Once()
		env, reg := newTestActivities(t, testSource{Nexter: mocks.NewNexter(t), Client: c}, s, "")

		v, err := env.ExecuteActivity(TransferActivityName, "w", item)
		require.NoError(t, err)
		var res TransferResult
		require.NoError(t, v.Get(&res))
		require.Equal(t, "s3://b/1.tar", res.URI)
		require.Equal(t, 1.0, testutil.ToFloat64(counter(t, reg, "ingest_dequeue_attempts_total", "success")))
	})

	t.Run("skips an item that is already stored", func(t *testing.T) {
		s := mocks.NewStorage(t)
		s.On("Stat", mock.Anything, item).Return(&storage.ObjectInfo{}, nil).Once()
		env, _ := newTestActivities(t, testSource{Nexter: mocks.NewNexter(t), Client: mocks.NewClient(t)}, s, "")

		v, err := env.ExecuteActivity(TransferActivityName, "w", item)
		require.NoError(t, err)
		var res TransferResult
		require.NoError(t, v.Get(&res))
		require.Empty(t, res.URI)
	})

	t.Run("an unknown workflow is not retried", func(t *testing.T) {
		env, _ := newTestActivities(t, testSource{Nexter: mocks.NewNexter(t), Client: mocks.NewClient(t)}, mocks.NewStorage(t), "")

		_, err := env.ExecuteActivity(TransferActivityName, "unknown", item)
		require.ErrorContains(t, err, `unknown ingest workflow "unknown"`)
	})
}

func TestCallWebhook(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
	}))
	defer srv.Close()
	env, reg := newTestActivities(t, testSource{Nexter: mocks.NewNexter(t), Client: mocks.NewClient(t)}, mocks.NewStorage(t), srv.URL)

	_, err := env.ExecuteActivity(CallWebhookActivityName, "w", []string{"s3://b/1.tar"})
	require.NoError(t, err)
	require.Equal(t, []string{"s3://b/1.tar"}, got)
	require.Equal(t, 1.0, testutil.ToFloat64(counter(t, reg, "ingest_webhook_http_client_requests_total", "success")))
}

func counter(t *testing.T, reg *prometheus.Registry, name, result string) prometheus.Collector {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "result" && l.GetValue() == result {
					return prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name}, func() float64 { return m.GetCounter().GetValue() })
				}
			}
		}
	}
	t.Fatalf("metric %s{result=%q} not found", name, result)
	return nil
}

package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"os"

	"github.com/efficientgo/tools/core/pkg/merrors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/connylabs/ingest"
)

// ObjectInfo describes an object in storage.
type ObjectInfo struct {
	// URI is the location under which the object can be found.
	URI string
}

// Storage must know how to store and stat objects.
// it allows applications to plug different storage systems.
type Storage interface {
	// Stat can be used to find information about the object corresponding to the given element.
	// If the object does not exist, then Stat returns an error satisfied by os.IsNotExist.
	Stat(ctx context.Context, element ingest.Codec) (*ObjectInfo, error)
	Store(ctx context.Context, element ingest.Codec, obj ingest.Object) (*url.URL, error)
}

type instrumentedStorage struct {
	Storage
	operationsTotal *prometheus.CounterVec
}

func (i instrumentedStorage) Stat(ctx context.Context, element ingest.Codec) (*ObjectInfo, error) {
	oi, err := i.Storage.Stat(ctx, element)
	if err == nil || os.IsNotExist(err) {
		i.operationsTotal.WithLabelValues("stat", "success").Inc()
	} else {
		i.operationsTotal.WithLabelValues("stat", "error").Inc()
	}
	return oi, err
}

func (i instrumentedStorage) Store(ctx context.Context, element ingest.Codec, obj ingest.Object) (*url.URL, error) {
	u, err := i.Storage.Store(ctx, element, obj)
	if err == nil || os.IsNotExist(err) {
		i.operationsTotal.WithLabelValues("store", "success").Inc()
	} else {
		i.operationsTotal.WithLabelValues("store", "error").Inc()
	}
	return u, err
}

// NewInstrumentedStorage adds Prometheus metrics to any Storage.
func NewInstrumentedStorage(s Storage, r prometheus.Registerer) Storage {
	operationsTotal := promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_storage_operations_total",
		Help: "Number of storage operations.",
	}, []string{"operation", "result"})

	for _, o := range []string{"stat", "store"} {
		for _, r := range []string{"error", "success"} {
			operationsTotal.WithLabelValues(o, r).Add(0)
		}
	}

	return &instrumentedStorage{
		s,
		operationsTotal,
	}
}

type multiStorage []Storage

func (m multiStorage) Stat(ctx context.Context, element ingest.Codec) (*ObjectInfo, error) {
	var o0 *ObjectInfo
	ch := make(chan error, len(m))
	for i := range m {
		go func(i int) {
			o, err := m[i].Stat(ctx, element)
			if i == 0 {
				o0 = o
			}
			ch <- err
		}(i)
	}
	var i int
	var err merrors.NilOrMultiError
	var isNotExist bool
	for e := range ch {
		if e != nil {
			err.Add(e)
			if os.IsNotExist(e) {
				isNotExist = true
			}
		}
		i++
		if i == len(m) {
			close(ch)
		}
	}
	if isNotExist {
		return o0, os.ErrNotExist
	}
	return o0, err.Err()
}

func (m multiStorage) Configure(map[string]any) error {
	return errors.New("not implemented")
}

func (m multiStorage) Store(ctx context.Context, element ingest.Codec, obj ingest.Object) (*url.URL, error) {
	var u0 *url.URL
	ch := make(chan error, len(m))
	// TODO: the whole copying could be improved, too many copies of the same data.
	buf, err := io.ReadAll(obj.Reader)
	if err != nil {
		return nil, err
	}
	rs := bytes.NewReader(buf)
	for i := range m {
		tmp := &bytes.Buffer{}
		_, err := rs.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}
		_, err = io.Copy(tmp, rs)
		if err != nil {
			return nil, err
		}

		obj := ingest.Object{
			Len:      obj.Len,
			MimeType: obj.MimeType,
			Reader:   tmp,
		}
		go func(i int) {
			u, err := m[i].Store(ctx, element, obj)
			if i == 0 {
				u0 = u
			}
			ch <- err
		}(i)
	}
	var i int
	var merr merrors.NilOrMultiError
	for e := range ch {
		if e != nil {
			merr.Add(e)
		}
		i++
		if i == len(m) {
			close(ch)
		}
	}
	return u0, merr.Err()
}

// NewMultiStorage creates a composite storage that combines many storages.
// Elements are stored across all storages.
// Whenever multiple values are expected, the first storage's result is always given.
func NewMultiStorage(s ...Storage) Storage {
	return multiStorage(s)
}

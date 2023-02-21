package multi

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"os"

	"github.com/efficientgo/tools/core/pkg/merrors"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

type multiStorage []storage.Storage

func (m multiStorage) Stat(ctx context.Context, element ingest.Codec) (*storage.ObjectInfo, error) {
	if len(m) == 0 {
		return nil, os.ErrNotExist
	}
	var o0 *storage.ObjectInfo
	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan error)
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
			cancel()
		}
		i++
		if i == len(m) {
			close(ch)
		}
	}
	cancel()
	if isNotExist {
		return nil, os.ErrNotExist
	}
	return o0, err.Err()
}

func (m multiStorage) Store(ctx context.Context, element ingest.Codec, obj ingest.Object) (*url.URL, error) {
	if len(m) == 0 {
		return nil, os.ErrNotExist
	}
	var u0 *url.URL
	ch := make(chan error, len(m))
	// TODO: the whole copying could be improved, too many copies of the same data.
	buf, err := io.ReadAll(obj.Reader)
	if err != nil {
		return nil, err
	}
	for i := range m {
		go func(i int) {
			// Store may be called just because the Stat on one single underlying
			// storage returned false. Example, an object exists in storage A but
			// not in storage B. We want to avoid uploading to A just because the
			// object does not exist in B.
			if _, err := m[i].Stat(ctx, element); !os.IsNotExist(err) {
				ch <- err
				return
			}
			obj := ingest.Object{
				Len:      obj.Len,
				MimeType: obj.MimeType,
				Reader:   bytes.NewReader(buf),
			}
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
func NewMultiStorage(s ...storage.Storage) storage.Storage {
	if len(s) == 1 {
		return s[0]
	}
	return multiStorage(s)
}

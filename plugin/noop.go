package plugin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/hashicorp/go-hclog"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

var (
	// defaultCodec can be used for testing against this noop plugin.
	defaultCodec  = ingest.NewCodec("id", "name")
	DefaultLogger = hclog.New(&hclog.LoggerOptions{
		Level:      hclog.Trace,
		Output:     os.Stderr,
		JSONFormat: true,
	})
)

const (
	defaultObjContent = "content of the default object"
	defaultObjURL     = "http://host:9090/path"
)

func NewNoopSource(l hclog.Logger) *NoopSource {
	return &NoopSource{
		l: l,
	}
}

// NoopSource returns elements from a buffer of elements.
type NoopSource struct {
	ptr      int
	buf      []ingest.Codec
	resetErr bool
	m        sync.Mutex
	l        hclog.Logger
}

// NewSource implements the Plugin interface.
func (p *NoopSource) Configure(config map[string]interface{}) error {
	p.l.Debug("configuring plugin")

	if v, ok := config["error"]; ok {
		if v, ok := v.(string); ok {
			return errors.New(v)
		}
		return errors.New("unknow error")
	}

	if v, ok := config["resetErr"]; ok {
		if v, ok := v.(bool); ok {
			p.resetErr = v
		}
	}
	p.ptr = 0
	p.buf = []ingest.Codec{defaultCodec}
	return nil
}

// Reset resets the Nexter as if it was newly created.
func (p *NoopSource) Reset(ctx context.Context) error {
	p.l.Debug("reset nexter")
	if p.resetErr {
		return errors.New("reset error")
	}
	p.m.Lock()
	defer p.m.Unlock()
	p.ptr = 0

	return nil
}

// Next ignores the context in this implementation.
func (p *NoopSource) Next(_ context.Context) (*ingest.Codec, error) {
	p.l.Debug("call next", "counter", p.ptr)
	p.m.Lock()
	defer p.m.Unlock()
	if p.ptr >= len(p.buf) {
		return nil, io.EOF
	}

	defer func() {
		p.ptr++
	}()
	return &p.buf[p.ptr], nil
}

func (p *NoopSource) CleanUp(ctx context.Context, i ingest.Codec) error {
	p.l.Debug("call cleanup")
	if i != defaultCodec {
		return fmt.Errorf("id %q not found", i.ID)
	}

	return nil
}

// Download will take an Element and download it from S3
func (s *NoopSource) Download(ctx context.Context, i ingest.Codec) (*ingest.Object, error) {
	if i != defaultCodec {
		return nil, fmt.Errorf("id %q not found", i.ID)
	}
	return &ingest.Object{
		Len:      int64(len(defaultObjContent)),
		MimeType: "plain/text",
		Reader:   strings.NewReader(defaultObjContent),
	}, nil
}

func NewNoopStore(l hclog.Logger) *NoopStore {
	return &NoopStore{
		l: l,
	}
}

type NoopStore struct {
	l hclog.Logger
}

func (p *NoopStore) Configure(config map[string]interface{}) error {
	p.l.Debug("configuring plugin")

	if v, ok := config["error"]; ok {
		if v, ok := v.(string); ok {
			return errors.New(v)
		}
		return errors.New("unknow error")
	}

	return nil
}

func (ms *NoopStore) Stat(ctx context.Context, element ingest.Codec) (*storage.ObjectInfo, error) {
	if element != defaultCodec {
		return nil, os.ErrNotExist
	}
	return &storage.ObjectInfo{
		URI: defaultObjURL,
	}, nil
}

func (ms *NoopStore) Store(ctx context.Context, element ingest.Codec, obj ingest.Object) (*url.URL, error) {
	b, err := io.ReadAll(obj.Reader)
	if err != nil {
		return nil, err
	}
	if string(b) != defaultObjContent {
		return nil, errors.New("wrong object content")
	}

	u, err := url.Parse(defaultObjURL)
	if err != nil {
		panic(err)
	}

	return u, nil
}

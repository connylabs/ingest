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
	// DefaultCodec can be used for testing against this noop plugin.
	DefaultCodec  = ingest.SimpleCodec{XID: "id", XName: "name"}
	DefaultLogger = hclog.New(&hclog.LoggerOptions{
		Level:      hclog.Trace,
		Output:     os.Stderr,
		JSONFormat: true,
	})
)

const (
	DefaultObjContent = "content of the default object"
	DefaultObjURL     = "http://host:9090/path"
)

func NewNoopSource(l hclog.Logger) *NoopSource {
	return &NoopSource{
		l: l,
	}
}

// NoopSource can fetch elements from the S3 API.
type NoopSource struct {
	ptr      int
	buf      []ingest.SimpleCodec
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
	p.buf = []ingest.SimpleCodec{DefaultCodec}
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

// Next ignores the context in this implementation
func (p *NoopSource) Next(_ context.Context) (*ingest.SimpleCodec, error) {
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

func (p *NoopSource) CleanUp(ctx context.Context, i ingest.SimpleCodec) error {
	p.l.Debug("call cleanup")
	if i != DefaultCodec {
		return fmt.Errorf("id %q not found", i.XID)
	}

	return nil
}

// Download will take an Element and download it from S3
func (s *NoopSource) Download(ctx context.Context, i ingest.SimpleCodec) (*ingest.Object, error) {
	if i != DefaultCodec {
		return nil, fmt.Errorf("id %q not found", i.XID)
	}
	return &ingest.Object{
		Len:      int64(len(DefaultObjContent)),
		MimeType: "plain/text",
		Reader:   strings.NewReader(DefaultObjContent),
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

func (ms *NoopStore) Stat(ctx context.Context, element ingest.SimpleCodec) (*storage.ObjectInfo, error) {
	if element != DefaultCodec {
		return nil, os.ErrNotExist
	}
	return &storage.ObjectInfo{
		URI: DefaultObjURL,
	}, nil
}

func (ms *NoopStore) Store(ctx context.Context, element ingest.SimpleCodec, obj ingest.Object) (*url.URL, error) {
	b, err := io.ReadAll(obj.Reader)
	if err != nil {
		return nil, err
	}
	if string(b) != DefaultObjContent {
		return nil, errors.New("wrong object content")
	}

	u, err := url.Parse(DefaultObjURL)
	if err != nil {
		panic(err)
	}

	return u, nil
}

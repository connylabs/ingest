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

func NewNoopSource(l hclog.Logger) *noopSource {
	return &noopSource{
		l: l,
	}
}

// noopSource returns elements from a buffer of elements.
type noopSource struct {
	ptr      int
	buf      []ingest.Codec
	resetErr bool
	m        sync.Mutex
	l        hclog.Logger
}

// NewSource implements the Plugin interface.
func (s *noopSource) Configure(config map[string]interface{}) error {
	s.l.Debug("configuring plugin")

	if v, ok := config["error"]; ok {
		if v, ok := v.(string); ok {
			return errors.New(v)
		}
		return errors.New("unknow error")
	}

	if v, ok := config["resetErr"]; ok {
		if v, ok := v.(bool); ok {
			s.resetErr = v
		}
	}
	s.ptr = 0
	s.buf = []ingest.Codec{defaultCodec}
	return nil
}

// Reset resets the Nexter as if it was newly created.
func (s *noopSource) Reset(ctx context.Context) error {
	s.l.Debug("reset nexter")
	if s.resetErr {
		return errors.New("reset error")
	}
	s.m.Lock()
	defer s.m.Unlock()
	s.ptr = 0

	return nil
}

// Next ignores the context in this implementation.
func (s *noopSource) Next(_ context.Context) (*ingest.Codec, error) {
	s.l.Debug("call next", "counter", s.ptr)
	s.m.Lock()
	defer s.m.Unlock()
	if s.ptr >= len(s.buf) {
		return nil, io.EOF
	}

	defer func() {
		s.ptr++
	}()
	return &s.buf[s.ptr], nil
}

func (s *noopSource) CleanUp(ctx context.Context, i ingest.Codec) error {
	s.l.Debug("call cleanup")
	if i != defaultCodec {
		return fmt.Errorf("id %q not found", i.ID)
	}

	return nil
}

// Download will take an Element and download it from S3
func (s *noopSource) Download(ctx context.Context, i ingest.Codec) (*ingest.Object, error) {
	if i != defaultCodec {
		return nil, fmt.Errorf("id %q not found", i.ID)
	}
	return &ingest.Object{
		Len:      int64(len(defaultObjContent)),
		MimeType: "plain/text",
		Reader:   strings.NewReader(defaultObjContent),
	}, nil
}

func NewNoopDestination(l hclog.Logger) *noopDestination {
	return &noopDestination{
		l: l,
	}
}

type noopDestination struct {
	l hclog.Logger
}

func (d *noopDestination) Configure(config map[string]interface{}) error {
	d.l.Debug("configuring plugin")

	if v, ok := config["error"]; ok {
		if v, ok := v.(string); ok {
			return errors.New(v)
		}
		return errors.New("unknow error")
	}

	return nil
}

func (d *noopDestination) Stat(ctx context.Context, element ingest.Codec) (*storage.ObjectInfo, error) {
	if element != defaultCodec {
		return nil, os.ErrNotExist
	}
	return &storage.ObjectInfo{
		URI: defaultObjURL,
	}, nil
}

func (d *noopDestination) Store(ctx context.Context, element ingest.Codec, obj ingest.Object) (*url.URL, error) {
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

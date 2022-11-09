package main

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mitchellh/mapstructure"

	"github.com/connylabs/ingest"
	iplugin "github.com/connylabs/ingest/plugin"
	s3storage "github.com/connylabs/ingest/storage/s3"
)

const defaultEndpoint = "s3.amazonaws.com"

type sourceConfig struct {
	Endpoint        string
	Insecure        bool
	AccessKeyID     string `json:"accessKeyID"`
	SecretAccessKey string
	Bucket          string
	Prefix          string
	Recursive       bool
}

type destinationConfig struct {
	sourceConfig    `mapstructure:",squash"`
	MetafilesPrefix string
}

type plugin struct{}

// NewSource implements the Plugin interface.
func (p *plugin) NewSource(_ context.Context, config map[string]interface{}) (iplugin.Source, error) {
	sc := new(sourceConfig)
	err := mapstructure.Decode(config, sc)
	if err != nil {
		return nil, err
	}
	if sc.Endpoint == "" {
		sc.Endpoint = defaultEndpoint
	}
	mc, err := minio.New(sc.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(sc.AccessKeyID, sc.SecretAccessKey, ""),
		Secure: !sc.Insecure,
	})
	if err != nil {
		return nil, err
	}
	return newSource(mc, sc.Bucket, sc.Prefix, sc.Recursive), nil
}

// NewDestination implements the Plugin interface.
func (p *plugin) NewDestination(_ context.Context, config map[string]interface{}) (iplugin.Destination, error) {
	dc := new(destinationConfig)
	err := mapstructure.Decode(config, dc)
	if err != nil {
		return nil, err
	}
	if dc.Endpoint == "" {
		dc.Endpoint = defaultEndpoint
	}
	mc, err := minio.New(dc.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(dc.AccessKeyID, dc.SecretAccessKey, ""),
		Secure: !dc.Insecure,
	})
	if err != nil {
		return nil, err
	}
	return s3storage.New(dc.Bucket, dc.Prefix, dc.MetafilesPrefix, mc, log.NewNopLogger()), nil
}

// Register allows ingest to register this plugin.
var Register iplugin.Register = func() (iplugin.Plugin, error) {
	return &plugin{}, nil
}

// An Element is pushed and popped from the queue.
type Element struct {
	bucket string
	prefix string
	name   string
}

// ID returns a unique ID for the Element.
func (e Element) ID() string {
	return path.Join(e.prefix, e.name)
}

// Name returns the name to use when storing the Element.
func (e Element) Name() string {
	return e.name
}

// source can fetch elements from the S3 API.
type source struct {
	mu sync.Mutex
	// TODO: instrument later
	mc        *minio.Client
	c         <-chan minio.ObjectInfo
	bucket    string
	prefix    string
	recursive bool
}

// newSource return a new source.
func newSource(mc *minio.Client, bucket, prefix string, recursive bool) iplugin.Source {
	return &source{
		mc:        mc,
		bucket:    bucket,
		prefix:    prefix,
		recursive: recursive,
	}
}

// Reset resets the Nexter as if it was newly created.
func (s *source) Reset(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.c = s.mc.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix:    s.prefix,
		Recursive: s.recursive,
	})

	return nil
}

// Next ignores the context in this implementation
func (s *source) Next(_ context.Context) (*ingest.SimpleCodec, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	oi, ok := <-s.c
	if ok {
		if oi.Err != nil {
			return nil, oi.Err
		}
		e := Element{
			bucket: s.bucket,
			prefix: s.prefix,
			name:   strings.TrimPrefix(oi.Key, s.prefix),
		}
		return ingest.NewCodec(e.ID(), e.Name()), nil
	}

	return nil, io.EOF
}

type object struct {
	mo       *minio.Object
	size     int64
	mimeType string
}

func (o *object) MimeType() string {
	return o.mimeType
}

func (o *object) Len() int64 {
	return o.size
}

func (o *object) Read(p []byte) (int, error) {
	return o.mo.Read(p)
}

func (s *source) CleanUp(ctx context.Context, i ingest.SimpleCodec) error {
	return s.mc.RemoveObject(ctx, s.bucket, i.ID(), minio.RemoveObjectOptions{})
}

// Download will take an Element and download it from S3
func (s *source) Download(ctx context.Context, i ingest.SimpleCodec) (ingest.Object, error) {
	o, err := s.mc.GetObject(ctx, s.bucket, i.ID(), minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	stat, err := o.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get stat: %w", err)
	}

	return &object{o, stat.Size, stat.ContentType}, nil
}

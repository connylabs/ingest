package main

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"

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

// NewSource implements the Plugin interface.
func (p *source) Configure(config map[string]interface{}) error {
	sc := new(sourceConfig)
	err := mapstructure.Decode(config, sc)
	if err != nil {
		return err
	}
	if sc.Endpoint == "" {
		sc.Endpoint = defaultEndpoint
	}
	mc, err := minio.New(sc.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(sc.AccessKeyID, sc.SecretAccessKey, ""),
		Secure: !sc.Insecure,
	})
	if err != nil {
		return err
	}
	p.bucket = sc.Bucket
	p.mc = mc
	p.prefix = sc.Prefix
	p.recursive = sc.Recursive

	return nil
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

func (s *source) CleanUp(ctx context.Context, i ingest.SimpleCodec) error {
	return s.mc.RemoveObject(ctx, s.bucket, i.ID(), minio.RemoveObjectOptions{})
}

// Download will take an Element and download it from S3
func (s *source) Download(ctx context.Context, i ingest.SimpleCodec) (*ingest.Object, error) {
	o, err := s.mc.GetObject(ctx, s.bucket, i.ID(), minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	stat, err := o.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get stat: %w", err)
	}

	return &ingest.Object{
		Reader:   o,
		Len:      stat.Size,
		MimeType: stat.ContentType,
	}, nil
}

func main() {
	iplugin.RunPluginServer(&source{}, s3storage.New())
}

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
	"github.com/connylabs/ingest/plugin"
	iplugin "github.com/connylabs/ingest/plugin"
	"github.com/connylabs/ingest/storage"
	s3storage "github.com/connylabs/ingest/storage/s3"
)

const defaultEndpoint = "s3.amazonaws.com"

type destinationConfig struct {
	sourceConfig    `mapstructure:",squash"`
	MetafilesPrefix string
}

type sourceConfig struct {
	Endpoint        string
	Insecure        bool
	AccessKeyID     string `json:"accessKeyID"`
	SecretAccessKey string
	Bucket          string
	Prefix          string
	Recursive       bool
}

var _ plugin.Destination = &destination{}

type destination struct {
	storage.Storage
}

func (d *destination) Configure(config map[string]interface{}) error {
	dc := new(destinationConfig)
	err := mapstructure.Decode(config, dc)
	if err != nil {
		return err
	}
	if dc.Endpoint == "" {
		dc.Endpoint = defaultEndpoint
	}
	mc, err := minio.New(dc.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(dc.AccessKeyID, dc.SecretAccessKey, ""),
		Secure: !dc.Insecure,
	})
	if err != nil {
		return fmt.Errorf("failed to create minio client:% w", err)
	}

	d.Storage = s3storage.New(dc.Bucket, dc.Prefix, dc.MetafilesPrefix, mc, log.NewNopLogger())

	return nil
}

// Configure will configure the source with the values given by config.
func (s *source) Configure(config map[string]interface{}) error {
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
	s.bucket = sc.Bucket
	s.mc = mc
	s.prefix = sc.Prefix
	s.recursive = sc.Recursive

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
func (s *source) Next(_ context.Context) (*ingest.Codec, error) {
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
		c := ingest.NewCodec(e.ID(), e.Name(), nil)
		return &c, nil
	}

	return nil, io.EOF
}

func (s *source) CleanUp(ctx context.Context, i ingest.Codec) error {
	return s.mc.RemoveObject(ctx, s.bucket, i.ID, minio.RemoveObjectOptions{})
}

// Download will take an Element and download it from S3
func (s *source) Download(ctx context.Context, i ingest.Codec) (*ingest.Object, error) {
	o, err := s.mc.GetObject(ctx, s.bucket, i.ID, minio.GetObjectOptions{})
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
	iplugin.RunPluginServer(&source{}, &destination{}, nil)
}

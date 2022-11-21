package plugin

import (
	"context"
	"errors"
	"io"
	"log"
	"net/rpc"
	"net/url"
	"os"
	"time"

	hplugin "github.com/hashicorp/go-plugin"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

const DefaultTimeOut = 5 * time.Second

type pluginSourceRPCServer struct {
	Impl Source

	mb      *hplugin.MuxBroker
	ctx     context.Context
	cancel  context.CancelFunc
	timeOut time.Duration
}

func (s *pluginSourceRPCServer) CleanUp(c *ingest.Codec, resp *any) error {
	if s.ctx == nil {
		return ErrNotConfigured
	}

	ctx, cancel := context.WithTimeout(s.ctx, s.timeOut)
	defer cancel()

	return s.Impl.CleanUp(ctx, *c)
}

func (s *pluginSourceRPCServer) Configure(c *map[string]any, resp *any) error {
	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.timeOut = DefaultTimeOut

	return s.Impl.Configure(*c)
}

func (s *pluginSourceRPCServer) Download(c *ingest.Codec, resp *DownloadResponse) error {
	if s.ctx == nil {
		return ErrNotConfigured
	}

	ctx, cancel := context.WithTimeout(s.ctx, s.timeOut)
	defer cancel()

	obj, err := s.Impl.Download(ctx, *c)
	if err != nil {
		return err
	}

	id := s.mb.NextId()
	*resp = DownloadResponse{
		MimeType: obj.MimeType,
		Len:      obj.Len,
		Reader:   id,
	}

	go func() {
		con, err := s.mb.Accept(id)
		if err != nil {
			// TODO: log error
			return
		}
		defer con.Close()

		if _, err := io.Copy(con, obj.Reader); err != nil {
			// TODO Log error with better logger
			log.Println(err)
		}
	}()

	return nil
}

func (s *pluginSourceRPCServer) Next(args any, resp *NextResponse) error {
	if s.ctx == nil {
		return ErrNotConfigured
	}

	ctx, cancel := context.WithTimeout(s.ctx, s.timeOut)
	defer cancel()

	c, err := s.Impl.Next(ctx)
	resp.S = c
	if err != nil {
		resp.Err = err.Error()
	}
	return nil
}

func (s *pluginSourceRPCServer) Reset(args any, resp *any) error {
	if s.ctx == nil {
		return ErrNotConfigured
	}

	ctx, cancel := context.WithTimeout(s.ctx, s.timeOut)
	defer cancel()

	return s.Impl.Reset(ctx)
}

var _ Source = &pluginSourceRPC{}

type pluginSourceRPC struct {
	client *rpc.Client
	mb     *hplugin.MuxBroker
}

func (p *pluginSourceRPC) call(serviceMethod string, args any, reply any) (err error) {
	err = p.client.Call(serviceMethod, args, reply)
	if err != nil && err.Error() == ErrNotConfigured.Error() {
		err = ErrNotConfigured
	}

	return
}

func (c *pluginSourceRPC) CleanUp(ctx context.Context, s ingest.Codec) error {
	return c.call("Plugin.CleanUp", s, new(any))
}

func (c *pluginSourceRPC) Configure(conf map[string]any) error {
	if conf == nil {
		conf = map[string]any{}
	}
	return c.call("Plugin.Configure", &conf, new(any))
}

func (c *pluginSourceRPC) Download(ctx context.Context, s ingest.Codec) (*ingest.Object, error) {
	var resp DownloadResponse
	if err := c.call("Plugin.Download", s, &resp); err != nil {
		return nil, err
	}
	con, err := c.mb.Dial(resp.Reader)
	if err != nil {
		return nil, err
	}
	obj := &ingest.Object{
		MimeType: resp.MimeType,
		Len:      resp.Len,
		Reader:   con, // TODO: do we need to io.Copy here?
	}

	return obj, nil
}

func (c *pluginSourceRPC) Next(ctx context.Context) (*ingest.Codec, error) {
	var resp NextResponse
	if err := c.call("Plugin.Next", new(any), &resp); err != nil {
		return nil, err
	}
	var err error
	if resp.Err != "" {
		err = errors.New(resp.Err)
		if resp.Err == io.EOF.Error() {
			err = io.EOF
		}
	}

	return resp.S, err
}

func (c *pluginSourceRPC) Reset(ctx context.Context) error {
	if err := c.call("Plugin.Reset", new(any), new(any)); err != nil {
		return err
	}
	return nil
}

type DownloadResponse struct {
	MimeType string
	Len      int64
	Reader   uint32
}

type NextResponse struct {
	S   *ingest.Codec
	Err string
}

type pluginDestinationRPCServer struct {
	Impl Destination

	mb      *hplugin.MuxBroker
	ctx     context.Context
	cancel  context.CancelFunc
	timeOut time.Duration
}

var ErrNotConfigured = errors.New("not configured")

func (s *pluginDestinationRPCServer) Configure(c *map[string]any, resp *any) error {
	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.timeOut = DefaultTimeOut

	return s.Impl.Configure(*c)
}

func (s *pluginDestinationRPCServer) Stat(args *ingest.Codec, resp *storage.ObjectInfo) error {
	if s.ctx == nil {
		return ErrNotConfigured
	}

	ctx, cancel := context.WithTimeout(s.ctx, s.timeOut)
	defer cancel()

	c, err := s.Impl.Stat(ctx, *args)
	if err != nil {
		return err
	}
	*resp = *c

	return nil
}

func (s *pluginDestinationRPCServer) Store(args *StoreRequest, resp *url.URL) error {
	if s.ctx == nil {
		return ErrNotConfigured
	}

	con, err := s.mb.Dial(args.Obj.Reader)
	if err != nil {
		return err
	}
	obj := ingest.Object{
		Len:      args.Obj.Len,
		MimeType: args.Obj.MimeType,
		Reader:   con,
	}

	ctx, cancel := context.WithTimeout(s.ctx, s.timeOut)
	defer cancel()

	u, err := s.Impl.Store(ctx, args.C, obj)
	if err != nil {
		return err
	}

	*resp = *u

	return nil
}

var _ Destination = &pluginDestinationRPC{}

type pluginDestinationRPC struct {
	client *rpc.Client
	mb     *hplugin.MuxBroker
}

func (p *pluginDestinationRPC) call(serviceMethod string, args any, reply any) (err error) {
	err = p.client.Call(serviceMethod, args, reply)
	if err != nil && err.Error() == ErrNotConfigured.Error() {
		err = ErrNotConfigured
	}

	return
}

func (c *pluginDestinationRPC) Configure(conf map[string]any) error {
	if conf == nil {
		conf = map[string]any{}
	}
	return c.call("Plugin.Configure", &conf, new(any))
}

func (c *pluginDestinationRPC) Stat(ctx context.Context, s ingest.Codec) (*storage.ObjectInfo, error) {
	var resp storage.ObjectInfo
	if err := c.call("Plugin.Stat", s, &resp); err != nil {
		if err.Error() == os.ErrNotExist.Error() {
			err = os.ErrNotExist
		}
		return nil, err
	}
	return &resp, nil
}

func (c *pluginDestinationRPC) Store(ctx context.Context, s ingest.Codec, obj ingest.Object) (*url.URL, error) {
	var resp url.URL
	id := c.mb.NextId()
	req := &StoreRequest{
		C: s,
		Obj: struct {
			Len      int64
			MimeType string
			Reader   uint32
		}{
			Len:      obj.Len,
			MimeType: obj.MimeType,
			Reader:   id,
		},
	}
	go func() {
		con, err := c.mb.Accept(id)
		if err != nil {
			return
		}
		defer con.Close()
		if _, err := io.Copy(con, obj.Reader); err != nil {
			return
		}
	}()

	if err := c.call("Plugin.Store", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

type StoreRequest struct {
	C   ingest.Codec
	Obj struct {
		Len      int64
		MimeType string
		Reader   uint32
	}
}

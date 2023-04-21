package plugin

import (
	"context"
	"errors"
	"io"
	"net/rpc"
	"net/url"
	"os"
	"time"

	"github.com/hashicorp/go-hclog"
	hplugin "github.com/hashicorp/go-plugin"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

const DefaultTimeOut = 5 * time.Second

type pluginSourceRPCServer struct {
	Impl Source

	g          prometheus.Gatherer
	l          hclog.Logger
	mb         *hplugin.MuxBroker
	ctx        context.Context
	configured bool
	timeOut    time.Duration
}

func (s *pluginSourceRPCServer) Gather(c *any, resp *[]*dto.MetricFamily) error {
	m, err := s.g.Gather()

	*resp = m

	return err
}

func (s *pluginSourceRPCServer) CleanUp(c *ingest.Codec, resp *any) error {
	if !s.configured {
		return ErrNotConfigured
	}

	return s.Impl.CleanUp(s.ctx, *c)
}

func (s *pluginSourceRPCServer) Configure(c *map[string]any, resp *any) error {
	// override timeout from conf?
	s.timeOut = DefaultTimeOut

	if err := s.Impl.Configure(*c); err != nil {
		return err
	}

	s.configured = true

	return nil
}

func (s *pluginSourceRPCServer) Download(c *ingest.Codec, resp *DownloadResponse) error {
	if !s.configured {
		return ErrNotConfigured
	}

	obj, err := s.Impl.Download(s.ctx, *c)
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
			s.l.Error("failed to accept connection", "id", id, "error", err.Error())
			return
		}
		defer con.Close()

		if _, err := io.Copy(con, obj.Reader); err != nil {
			s.l.Error("failed copy from connection", "id", id, "error", err.Error())
		}
	}()

	return nil
}

func (s *pluginSourceRPCServer) Next(args any, resp *ingest.Codec) error {
	if !s.configured {
		return ErrNotConfigured
	}

	c, err := s.Impl.Next(s.ctx)
	if err != nil {
		return err
	}

	*resp = *c

	return nil
}

func (s *pluginSourceRPCServer) Reset(args any, resp *any) error {
	if !s.configured {
		return ErrNotConfigured
	}

	return s.Impl.Reset(s.ctx)
}

var (
	_ Source              = &pluginSourceRPC{}
	_ prometheus.Gatherer = &pluginSourceRPC{}
)

type pluginSourceRPC struct {
	client *rpc.Client
	mb     *hplugin.MuxBroker
}

func (p *pluginSourceRPC) call(serviceMethod string, args any, reply any) (err error) {
	return mapErrMsg(p.client.Call(serviceMethod, args, reply))
}

func (c *pluginSourceRPC) Gather() (resp []*dto.MetricFamily, err error) {
	err = c.call("Plugin.Gather", new(any), &resp)

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

func (c *pluginSourceRPC) Next(context.Context) (*ingest.Codec, error) {
	var resp ingest.Codec

	if err := mapErrMsg(c.call("Plugin.Next", new(any), &resp)); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (c *pluginSourceRPC) Reset(ctx context.Context) error {
	return mapErrMsg(c.call("Plugin.Reset", new(any), new(any)))
}

type DownloadResponse struct {
	MimeType string
	Len      int64
	Reader   uint32
}

type pluginDestinationRPCServer struct {
	Impl Destination

	g          prometheus.Gatherer
	l          hclog.Logger
	mb         *hplugin.MuxBroker
	ctx        context.Context
	configured bool
	timeOut    time.Duration
}

var ErrNotConfigured = errors.New("not configured")

func (s *pluginDestinationRPCServer) Gather(c *any, resp *[]*dto.MetricFamily) error {
	m, err := s.g.Gather()

	*resp = m

	return err
}

func (s *pluginDestinationRPCServer) Configure(c *map[string]any, resp *any) error {
	s.timeOut = DefaultTimeOut

	if err := s.Impl.Configure(*c); err != nil {
		return err
	}

	s.configured = true

	return nil
}

func (s *pluginDestinationRPCServer) Stat(args *ingest.Codec, resp *storage.ObjectInfo) error {
	if !s.configured {
		return ErrNotConfigured
	}

	c, err := s.Impl.Stat(s.ctx, *args)
	if err != nil {
		return err
	}
	*resp = *c

	return nil
}

func (s *pluginDestinationRPCServer) Store(args *StoreRequest, resp *url.URL) error {
	if !s.configured {
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

	u, err := s.Impl.Store(s.ctx, args.C, obj)
	if err != nil {
		return err
	}

	*resp = *u

	return nil
}

var (
	_ Destination         = &pluginDestinationRPC{}
	_ prometheus.Gatherer = &pluginDestinationRPC{}
)

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

func (c *pluginDestinationRPC) Gather() (resp []*dto.MetricFamily, err error) {
	err = c.call("Plugin.Gather", new(any), &resp)

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

// mapErrMsg returns the original error if err's message matches one of a popular error.
// Like io.EOF, context.ErrCancel
func mapErrMsg(err error) error {
	if err == nil {
		return nil
	}

	switch err.Error() {
	case io.EOF.Error():
		return io.EOF
	case context.Canceled.Error():
		return context.Canceled
	case os.ErrNotExist.Error():
		return os.ErrNotExist
	case ErrNotConfigured.Error():
		return ErrNotConfigured
	case ErrNotImplemented.Error():
		return ErrNotImplemented
	default:
		return err

	}
}

package plugin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"net/url"
	"os"
	"os/exec"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-multierror"
	hplugin "github.com/hashicorp/go-plugin"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

// ErrNotImplemented can be returned by a plugin to indicate
// that it does not support acting as a source or destination.
var ErrNotImplemented = errors.New("not implemented")

// A Source represents an API from which objects should be downloaded.
type Source interface {
	ingest.Nexter
	ingest.Client
	Configure(map[string]any) error
}

// A Destination represents an API to which objects should be uploaded.
type Destination interface {
	storage.Storage
	Configure(map[string]any) error
}

type pluginSourceRPCServer struct {
	Impl Source
	mb   *hplugin.MuxBroker
}

type NextResponse struct {
	S   *ingest.SimpleCodec
	Err string
}

func (s *pluginSourceRPCServer) Next(args any, resp *NextResponse) error {
	c, err := s.Impl.Next(context.TODO())
	resp.S = c
	if err != nil {
		resp.Err = err.Error()
	}
	return nil
}

func (s *pluginSourceRPCServer) Reset(args any, resp *error) error {
	err := s.Impl.Reset(context.TODO())
	*resp = err
	return nil
}

type DownloadResponse struct {
	MimeType string
	Len      int64
	Reader   uint32
}

func (s *pluginSourceRPCServer) Download(c *ingest.SimpleCodec, resp *DownloadResponse) error {
	obj, err := s.Impl.Download(context.TODO(), *c)
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

func (s *pluginSourceRPCServer) CleanUp(c *ingest.SimpleCodec, resp *any) error {
	return s.Impl.CleanUp(context.TODO(), *c)
}

func (s *pluginSourceRPCServer) Configure(c *map[string]any, resp *any) error {
	return s.Impl.Configure(*c)
}

type PluginSource struct {
	Impl Source
}

func (p *PluginSource) Server(mb *hplugin.MuxBroker) (interface{}, error) {
	return &pluginSourceRPCServer{Impl: p.Impl, mb: mb}, nil
}

type pluginSourceRPC struct {
	client *rpc.Client
	mb     *hplugin.MuxBroker
}

func (c *pluginSourceRPC) Next(ctx context.Context) (*ingest.SimpleCodec, error) {
	fmt.Println("heloo from client")
	var resp NextResponse
	if err := c.client.Call("Plugin.Next", new(any), &resp); err != nil {
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
	fmt.Println("hello form client reset")
	var err error
	if err := c.client.Call("Plugin.Reset", new(any), &err); err != nil {
		fmt.Printf("hello form client reset error: %q\n", err.Error())
		return err
	}
	return err
}

func (c *pluginSourceRPC) Download(ctx context.Context, s ingest.SimpleCodec) (*ingest.Object, error) {
	var resp DownloadResponse
	if err := c.client.Call("Plugin.Download", s, &resp); err != nil {
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

func (c *pluginSourceRPC) CleanUp(ctx context.Context, s ingest.SimpleCodec) error {
	return c.client.Call("Plugin.CleanUp", s, new(any))
}

func (c *pluginSourceRPC) Configure(conf map[string]any) error {
	return c.client.Call("Plugin.Configure", &conf, new(any))
}

func (p *PluginSource) Client(mb *hplugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &pluginSourceRPC{client: c, mb: mb}, nil
}

type pluginDestinationRPCServer struct {
	Impl Destination
	mb   *hplugin.MuxBroker
}

type StatResponse struct {
	ObjInfo *storage.ObjectInfo
	Err     string
}

func (s *pluginDestinationRPCServer) Stat(args *ingest.SimpleCodec, resp *StatResponse) error {
	c, err := s.Impl.Stat(context.TODO(), *args)
	resp.ObjInfo = c
	return err
}

type StoreRequest struct {
	C   ingest.SimpleCodec
	Obj struct {
		Len      int64
		MimeType string
		Reader   uint32
	}
}

type StoreResponse struct {
	Err error
	URL *url.URL
}

func (s *pluginDestinationRPCServer) Store(args *StoreRequest, resp *StoreResponse) error {
	con, err := s.mb.Dial(args.Obj.Reader)
	if err != nil {
		return err
	}
	obj := ingest.Object{
		Len:      args.Obj.Len,
		MimeType: args.Obj.MimeType,
		Reader:   con,
	}
	url, err := s.Impl.Store(context.TODO(), args.C, obj)
	resp.Err = err
	resp.URL = url

	return nil
}

func (c *pluginDestinationRPC) Store(ctx context.Context, s ingest.SimpleCodec, obj ingest.Object) (*url.URL, error) {
	var resp StoreResponse
	id := c.mb.NextId()
	req := &StoreRequest{
		C: s,
		Obj: struct {
			Len      int64
			MimeType string
			Reader   uint32
		}{
			Len:      0,
			MimeType: obj.MimeType,
			Reader:   id,
		},
	}
	go func() {
		con, err := c.mb.Accept(id)
		if err != nil {
			return
		}
		if _, err := io.Copy(con, obj.Reader); err != nil {
			return
		}
		con.Close()
	}()

	if err := c.client.Call("Plugin.Store", req, &resp); err != nil {
		return nil, err
	}
	return resp.URL, nil
}

var (
	_ Destination = &pluginDestinationRPC{}
	_ Source      = &pluginSourceRPC{}
)

func (c *pluginDestinationRPC) Configure(conf map[string]any) error {
	return c.client.Call("Plugin.Configure", &conf, new(any))
}

func (s *pluginDestinationRPCServer) Configure(c *map[string]any, resp *any) error {
	return s.Impl.Configure(*c)
}

func (c *pluginDestinationRPC) Stat(ctx context.Context, s ingest.SimpleCodec) (*storage.ObjectInfo, error) {
	var resp StatResponse
	if err := c.client.Call("Plugin.Stat", s, &resp); err != nil {
		if err.Error() == os.ErrNotExist.Error() {
			err = os.ErrNotExist
		}
		return nil, err
	}
	return resp.ObjInfo, nil
}

type PluginDestination struct {
	Impl Destination
}

func (p *PluginDestination) Server(mb *hplugin.MuxBroker) (interface{}, error) {
	return &pluginDestinationRPCServer{Impl: p.Impl, mb: mb}, nil
}

type pluginDestinationRPC struct {
	client *rpc.Client
	mb     *hplugin.MuxBroker
}

func (p *PluginDestination) Client(mb *hplugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &pluginDestinationRPC{client: c, mb: mb}, nil
}

func NewPlugin(ctx context.Context, path string) (Destination, Source, error) {
	handshakeConfig := hplugin.HandshakeConfig{
		ProtocolVersion:  PluginMagicProtocalVersion,
		MagicCookieKey:   PluginMagicCookieKey,
		MagicCookieValue: PluginCookieValue,
	}

	// pluginMap is the map of plugins we can dispense.
	pluginMap := map[string]hplugin.Plugin{
		"destination": &PluginDestination{},
		"source":      &PluginSource{},
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Name:       "plugin",
		JSONFormat: true,
		Output:     os.Stdout,
		Level:      hclog.Debug,
	})

	// We're a host! Start by launching the plugin process.
	client := hplugin.NewClient(&hplugin.ClientConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
		Cmd:             exec.Command(path),
		Logger:          logger,
	})

	// Connect via RPC
	rpcClient, err := client.Client()
	if err != nil {
		client.Kill()
		return nil, nil, err
	}
	go func() {
		// TODO find out how to exit gracefully
		// Sometime the RPC server keeps running usings lots of CPU.
		<-ctx.Done()
		logger.Info("closing rpc client")
		if err := rpcClient.Close(); err != nil {
			logger.Error("failed to close rpc client", "err", err.Error())
		}
		client.Kill()
	}()

	mErr := &multierror.Error{}
	rawD, errD := rpcClient.Dispense("destination")
	if errD != nil {
		mErr = multierror.Append(mErr, errD)
	}
	rawS, errS := rpcClient.Dispense("source")
	if errS != nil {
		mErr = multierror.Append(mErr, errS)
	}
	if mErr.Len() == 2 {
		rpcClient.Close()
		client.Kill()
		return nil, nil, fmt.Errorf("failed to dispense any rpc client: %w", mErr)
	}
	return rawD.(Destination), rawS.(Source), mErr.ErrorOrNil()
}

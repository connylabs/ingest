package plugin

import (
	"net/url"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

type instrumentedPluginSourceRPCServer struct {
	*pluginSourceRPCServer
	cv *prometheus.CounterVec
}

func (s *instrumentedPluginSourceRPCServer) Gather(c *any, resp *[]*dto.MetricFamily) error {
	err := s.pluginSourceRPCServer.Gather(c, resp)
	if err != nil {
		s.cv.With(prometheus.Labels{"rpc_method": "Gather", "result": "error"}).Inc()
		return err
	}
	s.cv.With(prometheus.Labels{"rpc_method": "Gather", "result": "success"}).Inc()
	return nil
}

func (s *instrumentedPluginSourceRPCServer) Configure(c *map[string]any, resp *any) error {
	err := s.pluginSourceRPCServer.Configure(c, resp)
	if err != nil {
		s.cv.With(prometheus.Labels{"rpc_method": "Configure", "result": "error"}).Inc()
		return err
	}
	s.cv.With(prometheus.Labels{"rpc_method": "Configure", "result": "success"}).Inc()
	return nil
}

func (s *instrumentedPluginSourceRPCServer) CleanUp(c *ingest.Codec, resp *any) error {
	err := s.pluginSourceRPCServer.CleanUp(c, resp)
	if err != nil {
		s.cv.With(prometheus.Labels{"rpc_method": "CleanUp", "result": "error"}).Inc()
		return err
	}
	s.cv.With(prometheus.Labels{"rpc_method": "CleanUp", "result": "success"}).Inc()
	return nil
}

func (s *instrumentedPluginSourceRPCServer) Download(c *ingest.Codec, resp *DownloadResponse) (err error) {
	err = s.pluginSourceRPCServer.Download(c, resp)
	if err != nil {
		s.cv.With(prometheus.Labels{"rpc_method": "Download", "result": "error"}).Inc()
		return
	}
	s.cv.With(prometheus.Labels{"rpc_method": "Download", "result": "success"}).Inc()
	return
}

func (s *instrumentedPluginSourceRPCServer) Next(args any, resp *ingest.Codec) (err error) {
	err = s.pluginSourceRPCServer.Next(args, resp)
	if err != nil {
		s.cv.With(prometheus.Labels{"rpc_method": "Next", "result": "error"}).Inc()
		return
	}
	s.cv.With(prometheus.Labels{"rpc_method": "Next", "result": "success"}).Inc()
	return
}

func (s *instrumentedPluginSourceRPCServer) Reset(args any, resp *any) (err error) {
	err = s.pluginSourceRPCServer.Reset(args, resp)
	if err != nil {
		s.cv.With(prometheus.Labels{"rpc_method": "Reset", "result": "error"}).Inc()
		return
	}
	s.cv.With(prometheus.Labels{"rpc_method": "Reset", "result": "success"}).Inc()
	return
}

type instrumentedPluginDestinationRPCServer struct {
	*pluginDestinationRPCServer
	cv *prometheus.CounterVec
}

func (s *instrumentedPluginDestinationRPCServer) Gather(c *any, resp *[]*dto.MetricFamily) (err error) {
	err = s.pluginDestinationRPCServer.Gather(c, resp)
	if err != nil {
		s.cv.With(prometheus.Labels{"rpc_method": "Gather", "result": "error"}).Inc()
		return
	}
	s.cv.With(prometheus.Labels{"rpc_method": "Gather", "result": "success"}).Inc()
	return
}

func (s *instrumentedPluginDestinationRPCServer) Configure(c *map[string]any, resp *any) (err error) {
	err = s.pluginDestinationRPCServer.Configure(c, resp)
	if err != nil {
		s.cv.With(prometheus.Labels{"rpc_method": "Configure", "result": "error"}).Inc()
		return
	}
	s.cv.With(prometheus.Labels{"rpc_method": "Configure", "result": "success"}).Inc()
	return
}

func (s *instrumentedPluginDestinationRPCServer) Stat(args *ingest.Codec, resp *storage.ObjectInfo) (err error) {
	err = s.pluginDestinationRPCServer.Stat(args, resp)
	if err != nil {
		s.cv.With(prometheus.Labels{"rpc_method": "Stat", "result": "error"}).Inc()
		return
	}
	s.cv.With(prometheus.Labels{"rpc_method": "Stat", "result": "success"}).Inc()
	return
}

func (s *instrumentedPluginDestinationRPCServer) Store(args *StoreRequest, resp *url.URL) (err error) {
	err = s.pluginDestinationRPCServer.Store(args, resp)
	if err != nil {
		s.cv.With(prometheus.Labels{"rpc_method": "Store", "result": "error"}).Inc()
		return
	}
	s.cv.With(prometheus.Labels{"rpc_method": "Store", "result": "success"}).Inc()
	return
}

package main

import (
	"github.com/prometheus/client_golang/prometheus"

	iplugin "github.com/connylabs/ingest/plugin"
)

func main() {
	reg := prometheus.NewRegistry()
	c := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "noop",
		Help:        "show that the noop plugin can add its own collectors",
		ConstLabels: prometheus.Labels{"noop": "noop"},
	})
	reg.MustRegister(c)
	c.Set(1)
	iplugin.RunPluginServer(iplugin.NewNoopSource(iplugin.DefaultLogger), iplugin.NewNoopDestination(iplugin.DefaultLogger), iplugin.WithGatherer(reg), iplugin.WithLogger(iplugin.DefaultLogger))
}

package main

import (
	"github.com/prometheus/client_golang/prometheus"

	iplugin "github.com/connylabs/ingest/plugin"
)

func main() {
	c := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "noop",
		Help:        "show that the noop plugin can add its own collectors",
		ConstLabels: prometheus.Labels{"noop": "noop"},
	})
	c.Set(1)
	iplugin.RunPluginServer(iplugin.NewNoopSource(iplugin.DefaultLogger), iplugin.NewNoopDestination(iplugin.DefaultLogger), c)
}

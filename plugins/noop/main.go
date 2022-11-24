package main

import (
	iplugin "github.com/connylabs/ingest/plugin"
)

func main() {
	iplugin.RunPluginServer(iplugin.NewNoopSource(iplugin.DefaultLogger), iplugin.NewNoopStore(iplugin.DefaultLogger))
}

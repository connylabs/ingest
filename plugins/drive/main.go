package main

import (
	iplugin "github.com/connylabs/ingest/plugin"
	dstorage "github.com/connylabs/ingest/storage/drive"
)

func main() {
	iplugin.RunPluginServer(nil, &dstorage.DriveStorage{})
}

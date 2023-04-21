package main

import (
	"context"
	"fmt"

	"github.com/mitchellh/mapstructure"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"

	"github.com/connylabs/ingest/plugin"
	"github.com/connylabs/ingest/storage"
	dstorage "github.com/connylabs/ingest/storage/drive"
)

type destinationConfig struct {
	APIKey          string
	CredentialsFile string
	Folder          string
}

var _ plugin.Destination = &destination{}

type destination struct {
	storage.Storage
	reg prometheus.Registerer
}

// NewDestination implements the Plugin interface.
func (d *destination) Configure(config map[string]interface{}) error {
	dc := new(destinationConfig)
	err := mapstructure.Decode(config, dc)
	if err != nil {
		return err
	}
	var o []option.ClientOption
	if dc.APIKey != "" {
		o = append(o, option.WithAPIKey(dc.APIKey))
	}
	if dc.CredentialsFile != "" {
		o = append(o, option.WithCredentialsFile(dc.CredentialsFile))
	}
	ds, err := drive.NewService(context.TODO(), o...)
	if err != nil {
		return fmt.Errorf("failed to create drive service: %w", err)
	}

	drS, err := dstorage.New(dc.Folder, ds, plugin.DefaultLogger, d.reg)
	if err != nil {
		return fmt.Errorf("failed to create drive storage: %w", err)
	}
	d.Storage = drS
	return nil
}

func main() {
	reg := prometheus.NewRegistry()
	plugin.RunPluginServer(nil, &destination{reg: reg}, plugin.WithGatherer(reg))
}

package main

import (
	"context"

	"github.com/go-kit/log"
	"github.com/mitchellh/mapstructure"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"

	iplugin "github.com/connylabs/ingest/plugin"
	dstorage "github.com/connylabs/ingest/storage/drive"
)

type destinationConfig struct {
	APIKey          string
	CredentialsFile string
	Folder          string
}

type plugin struct{}

// NewSource implements the Plugin interface.
func (p *plugin) NewSource(_ context.Context, config map[string]interface{}) (iplugin.Source, error) {
	return nil, iplugin.ErrNotImplemented
}

// NewDestination implements the Plugin interface.
func (p *plugin) NewDestination(ctx context.Context, config map[string]interface{}) (iplugin.Destination, error) {
	dc := new(destinationConfig)
	err := mapstructure.Decode(config, dc)
	if err != nil {
		return nil, err
	}
	var o []option.ClientOption
	if dc.APIKey != "" {
		o = append(o, option.WithAPIKey(dc.APIKey))
	}
	if dc.CredentialsFile != "" {
		o = append(o, option.WithCredentialsFile(dc.CredentialsFile))
	}
	ds, err := drive.NewService(ctx, o...)
	if err != nil {
		return nil, err
	}
	return dstorage.New(dc.Folder, ds, log.NewNopLogger())
}

// Register allows ingest to register this plugin.
var Register iplugin.Register = func() (iplugin.Plugin, error) {
	return &plugin{}, nil
}

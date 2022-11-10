package drive

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/mapstructure"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"

	"github.com/connylabs/ingest"
	iplugin "github.com/connylabs/ingest/plugin"
	"github.com/connylabs/ingest/storage"
)

type DriveStorage struct {
	s *drive.Service
	l hclog.Logger
	p string
}

type destinationConfig struct {
	APIKey          string
	CredentialsFile string
	Folder          string
}

// NewDestination implements the Plugin interface.
func (p *DriveStorage) Configure(config map[string]interface{}) error {
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
		return err
	}

	parts := strings.Split(dc.Folder, "/")
	if len(parts) < 1 {
		return errors.New("no folder was specified")
	}
	p.s = ds
	p.l = iplugin.DefaultLogger

	f, err := p.find(context.Background(), "", parts)
	if err != nil {
		return fmt.Errorf("failed to find folder: %w", err)
	}
	p.p = f.Id
	return nil
}

// New returns a new Storage that can store objects to Google Drive.
func New(folder string, service *drive.Service, l hclog.Logger) (storage.Storage, error) {
	parts := strings.Split(folder, "/")
	if len(parts) < 1 {
		return nil, errors.New("no folder was specified")
	}
	ds := &DriveStorage{s: service, l: l}
	f, err := ds.find(context.Background(), "", parts)
	if err != nil {
		return nil, fmt.Errorf("failed to find folder: %w", err)
	}
	ds.p = f.Id
	return ds, nil
}

func (ds *DriveStorage) Stat(ctx context.Context, element ingest.SimpleCodec) (*storage.ObjectInfo, error) {
	f, err := ds.find(ctx, ds.p, []string{element.Name()})
	if err != nil {
		return nil, err
	}

	return &storage.ObjectInfo{URI: f.Id}, nil
}

// find is a helper that will recursively look for a file matching the given hierarchy.
func (ds *DriveStorage) find(ctx context.Context, parent string, parts []string) (*drive.File, error) {
	query := fmt.Sprintf("name = '%s' and trashed=false", parts[0])
	if parent != "" {
		query += fmt.Sprintf(" and '%s' in parents", parent)
	}
	fileList, err := ds.s.Files.List().IncludeItemsFromAllDrives(true).SupportsAllDrives(true).Fields("files(id,parents)").Context(ctx).Q(query).Do()
	if err != nil {
		return nil, err
	}
	for i := range fileList.Files {
		if (parent == "") != (len(fileList.Files[i].Parents) == 0) {
			continue
		}
		if len(parts) == 1 {
			return fileList.Files[i], nil
		}
		f, err := ds.find(ctx, fileList.Files[i].Id, parts[1:])
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return f, nil
	}
	return nil, fs.ErrNotExist
}

func (ds *DriveStorage) Store(ctx context.Context, element ingest.SimpleCodec, obj ingest.Object) (*url.URL, error) {
	file := &drive.File{
		Name:    element.Name(),
		Parents: []string{ds.p},
	}

	f, err := ds.s.Files.Create(file).Media(obj.Reader).SupportsAllDrives(true).Context(ctx).Do()
	if err != nil {
		return nil, err
	}

	return url.Parse(fmt.Sprintf("https://drive.google.com/file/d/%s", f.Id))
}

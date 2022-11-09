package drive

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"

	"github.com/go-kit/log"
	"google.golang.org/api/drive/v3"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

type driveStorage struct {
	s *drive.Service
	l log.Logger
	p string
}

// New returns a new Storage that can store objects to Google Drive.
func New(folder string, service *drive.Service, l log.Logger) (storage.Storage, error) {
	parts := strings.Split(folder, "/")
	if len(parts) < 1 {
		return nil, errors.New("no folder was specified")
	}
	ds := &driveStorage{s: service, l: l}
	f, err := ds.find(context.Background(), "", parts)
	if err != nil {
		return nil, fmt.Errorf("failed to find folder: %w", err)
	}
	ds.p = f.Id
	return ds, nil
}

func (ds *driveStorage) Stat(ctx context.Context, element ingest.SimpleCodec) (*storage.ObjectInfo, error) {
	f, err := ds.find(ctx, ds.p, []string{element.Name()})
	if err != nil {
		return nil, err
	}

	return &storage.ObjectInfo{URI: f.Id}, nil
}

// find is a helper that will recursively look for a file matching the given hierarchy.
func (ds *driveStorage) find(ctx context.Context, parent string, parts []string) (*drive.File, error) {
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

func (ds *driveStorage) Store(ctx context.Context, element ingest.SimpleCodec, download func(context.Context, ingest.SimpleCodec) (*ingest.Object, error)) (*url.URL, error) {
	file := &drive.File{
		Name:    element.Name(),
		Parents: []string{ds.p},
	}

	object, err := download(ctx, element)
	if err != nil {
		return nil, fmt.Errorf("failed to download %s: %w", element.ID(), err)
	}

	f, err := ds.s.Files.Create(file).Media(object.Reader).SupportsAllDrives(true).Context(ctx).Do()
	if err != nil {
		return nil, err
	}

	return url.Parse(fmt.Sprintf("https://drive.google.com/file/d/%s", f.Id))
}

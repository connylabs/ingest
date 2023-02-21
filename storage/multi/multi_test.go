package multi

import (
	"context"
	url "net/url"
	"os"
	"strings"
	"testing"

	mock "github.com/stretchr/testify/mock"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/mocks"
	"github.com/connylabs/ingest/storage"
)

func TestMultiStorageStat(t *testing.T) {
	codec := ingest.Codec{ID: "foo", Name: "bar"}
	for _, tc := range []struct {
		name     string
		storages func(t *testing.T) []storage.Storage
		codec    ingest.Codec
		err      error
	}{
		{
			name:  "empty",
			codec: codec,
			err:   os.ErrNotExist,
		},
		{
			name: "one exists",
			storages: func(t *testing.T) []storage.Storage {
				return []storage.Storage{
					callToStorage(mocks.NewStorage(t).EXPECT().
						Stat(mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), nil).Once(),
					),
				}
			},
			codec: codec,
		},
		{
			name: "one doesn't exist",
			storages: func(t *testing.T) []storage.Storage {
				return []storage.Storage{
					callToStorage(mocks.NewStorage(t).EXPECT().
						Stat(mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), os.ErrNotExist).Once(),
					),
				}
			},
			codec: codec,
			err:   os.ErrNotExist,
		},
		{
			name: "multiple doesn't exist",
			storages: func(t *testing.T) []storage.Storage {
				return []storage.Storage{
					callToStorage(mocks.NewStorage(t).EXPECT().
						Stat(mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), os.ErrNotExist).Maybe(),
					),
					callToStorage(mocks.NewStorage(t).EXPECT().
						Stat(mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), os.ErrNotExist).Maybe(),
					),
					callToStorage(mocks.NewStorage(t).EXPECT().
						Stat(mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), os.ErrNotExist).Maybe(),
					),
				}
			},
			codec: codec,
			err:   os.ErrNotExist,
		},
		{
			name: "multiple doesn't exist in one",
			storages: func(t *testing.T) []storage.Storage {
				return []storage.Storage{
					callToStorage(mocks.NewStorage(t).EXPECT().
						Stat(mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), nil).Maybe(),
					),
					callToStorage(mocks.NewStorage(t).EXPECT().
						Stat(mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), nil).Maybe(),
					),
					callToStorage(mocks.NewStorage(t).EXPECT().
						Stat(mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), os.ErrNotExist).Once(),
					),
				}
			},
			codec: codec,
			err:   os.ErrNotExist,
		},
		{
			name: "multiple exists",
			storages: func(t *testing.T) []storage.Storage {
				return []storage.Storage{
					callToStorage(mocks.NewStorage(t).EXPECT().
						Stat(mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), nil).Once(),
					),
					callToStorage(mocks.NewStorage(t).EXPECT().
						Stat(mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), nil).Once(),
					),
					callToStorage(mocks.NewStorage(t).EXPECT().
						Stat(mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), nil).Once(),
					),
				}
			},
			codec: codec,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var storages []storage.Storage
			if tc.storages != nil {
				storages = tc.storages(t)
			}
			s := NewMultiStorage(storages...)
			if _, err := s.Stat(context.Background(), codec); err != tc.err {
				t.Errorf("expected %v, got %v", tc.err, err)
			}
		})
	}
}

func TestMultiStorageStore(t *testing.T) {
	codec := ingest.Codec{ID: "foo", Name: "bar"}
	for _, tc := range []struct {
		name     string
		storages func(t *testing.T) []storage.Storage
		codec    ingest.Codec
		err      error
	}{
		{
			name:  "empty",
			codec: codec,
			err:   os.ErrNotExist,
		},
		{
			name: "one exists; uses storage directly",
			storages: func(t *testing.T) []storage.Storage {
				return []storage.Storage{
					callToStorage(mocks.NewStorage(t).
						On("Store", mock.Anything, codec, mock.Anything).
						Return(&url.URL{Scheme: "s3", Host: "bucket", Path: "prefix/foo"}, nil).
						Once(),
					),
				}
			},
			codec: codec,
		},
		{
			name: "one does not exists; uses storage directly",
			storages: func(t *testing.T) []storage.Storage {
				return []storage.Storage{
					callToStorage(mocks.NewStorage(t).
						On("Store", mock.Anything, codec, mock.Anything).
						Return(&url.URL{Scheme: "s3", Host: "bucket", Path: "prefix/foo"}, nil).
						Once(),
					),
				}
			},
			codec: codec,
		},
		{
			name: "multiple does not exist in one",
			storages: func(t *testing.T) []storage.Storage {
				return []storage.Storage{
					callToStorage(mocks.NewStorage(t).
						On("Store", mock.Anything, codec, mock.Anything).
						Return(&url.URL{Scheme: "s3", Host: "bucket", Path: "prefix/foo"}, nil).
						Once().
						On("Stat", mock.Anything, mock.Anything).
						Return((*storage.ObjectInfo)(nil), os.ErrNotExist).
						Once(),
					),
					callToStorage(mocks.NewStorage(t).
						On("Stat", mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), nil).
						Once(),
					),
					callToStorage(mocks.NewStorage(t).
						On("Stat", mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), nil).
						Once(),
					),
				}
			},
			codec: codec,
		},
		{
			name: "multiple does not exist in any",
			storages: func(t *testing.T) []storage.Storage {
				return []storage.Storage{
					callToStorage(mocks.NewStorage(t).
						On("Store", mock.Anything, codec, mock.Anything).
						Return(&url.URL{Scheme: "s3", Host: "bucket", Path: "prefix/foo"}, nil).
						Once().
						On("Stat", mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), os.ErrNotExist).
						Once(),
					),
					callToStorage(mocks.NewStorage(t).
						On("Store", mock.Anything, codec, mock.Anything).
						Return(&url.URL{Scheme: "s3", Host: "bucket", Path: "prefix/foo"}, nil).
						Once().
						On("Stat", mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), os.ErrNotExist).
						Once(),
					),
					callToStorage(mocks.NewStorage(t).
						On("Store", mock.Anything, codec, mock.Anything).
						Return(&url.URL{Scheme: "s3", Host: "bucket", Path: "prefix/foo"}, nil).
						Once().
						On("Stat", mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), os.ErrNotExist).
						Once(),
					),
				}
			},
			codec: codec,
		},
		{
			name: "multiple exists in all",
			storages: func(t *testing.T) []storage.Storage {
				return []storage.Storage{
					callToStorage(mocks.NewStorage(t).
						On("Stat", mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), nil).
						Once(),
					),
					callToStorage(mocks.NewStorage(t).
						On("Stat", mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), nil).
						Once(),
					),
					callToStorage(mocks.NewStorage(t).
						On("Stat", mock.Anything, codec).
						Return((*storage.ObjectInfo)(nil), nil).
						Once(),
					),
				}
			},
			codec: codec,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var storages []storage.Storage
			if tc.storages != nil {
				storages = tc.storages(t)
			}
			s := NewMultiStorage(storages...)
			if _, err := s.Store(context.Background(), ingest.Codec{ID: "foo", Name: "bar"}, ingest.Object{Reader: strings.NewReader("hello")}); err != tc.err {
				t.Errorf("expected %v, got %v", tc.err, err)
			}
		})
	}
}

func callToStorage(m *mock.Call) storage.Storage {
	ms := new(mocks.Storage)
	ms.Mock.ExpectedCalls = m.Parent.ExpectedCalls
	return ms
}

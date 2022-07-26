// This file is hand written

package mocks

import (
	"context"
	"net/url"

	"github.com/stretchr/testify/mock"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

var _ storage.Storage = &Storage{}

// Storage is a mock.
type Storage struct {
	mock.Mock
}

// Stat is a mock function.
func (_m *Storage) Stat(ctx context.Context, element ingest.Identifiable) (*storage.ObjectInfo, error) {
	ret := _m.Called(ctx, element)

	var o *storage.ObjectInfo
	if rf, ok := ret.Get(0).(func(context.Context, ingest.Identifiable) *storage.ObjectInfo); ok {
		o = rf(ctx, element)
	} else {
		o = ret.Get(0).(*storage.ObjectInfo)
	}

	var err error
	if rf, ok := ret.Get(1).(func(context.Context, ingest.Identifiable) error); ok {
		err = rf(ctx, element)
	} else {
		err = ret.Error(1)
	}
	return o, err
}

// Store is a mock function.
func (_m *Storage) Store(ctx context.Context, element ingest.Identifiable, download func(context.Context, ingest.Identifiable) (ingest.Object, error)) (*url.URL, error) {
	ret := _m.Called(ctx, element, download)

	var u *url.URL
	if rf, ok := ret.Get(0).(func(context.Context, ingest.Identifiable, func(context.Context, ingest.Identifiable) (ingest.Object, error)) *url.URL); ok {
		u = rf(ctx, element, download)
	} else {
		u = ret.Get(0).(*url.URL)
	}

	var err error
	if rf, ok := ret.Get(1).(func(context.Context, ingest.Identifiable, func(context.Context, ingest.Identifiable) (ingest.Object, error)) error); ok {
		err = rf(ctx, element, download)
	} else {
		err = ret.Error(1)
	}
	return u, err
}

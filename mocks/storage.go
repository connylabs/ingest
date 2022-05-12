// This file is hand written

package mocks

import (
	"context"
	"net/url"

	"github.com/stretchr/testify/mock"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

var _ storage.Storage[*T] = &Storage[*T]{}

// Storage is a mock.
type Storage[T ingest.Identifiable] struct {
	mock.Mock
}

// Stat is a mock function.
func (_m *Storage[T]) Stat(ctx context.Context, element T) (*storage.ObjectInfo, error) {
	ret := _m.Called(ctx, element)

	var o *storage.ObjectInfo
	if rf, ok := ret.Get(0).(func(context.Context, T) *storage.ObjectInfo); ok {
		o = rf(ctx, element)
	} else {
		o = ret.Get(0).(*storage.ObjectInfo)
	}

	var err error
	if rf, ok := ret.Get(1).(func(context.Context, T) error); ok {
		err = rf(ctx, element)
	} else {
		err = ret.Error(1)
	}
	return o, err
}

// Store is a mock function.
func (_m *Storage[T]) Store(ctx context.Context, element T, download func(context.Context, T) (ingest.Object, error)) (*url.URL, error) {
	ret := _m.Called(ctx, element, download)

	var u *url.URL
	if rf, ok := ret.Get(0).(func(context.Context, T, func(context.Context, T) (ingest.Object, error)) *url.URL); ok {
		u = rf(ctx, element, download)
	} else {
		u = ret.Get(0).(*url.URL)
	}

	var err error
	if rf, ok := ret.Get(1).(func(context.Context, T, func(context.Context, T) (ingest.Object, error)) error); ok {
		err = rf(ctx, element, download)
	} else {
		err = ret.Error(1)
	}
	return u, err
}

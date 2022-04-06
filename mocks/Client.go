// This file is hand written

package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/mietright/ingest"
)

var _ ingest.Client[*T] = &Client[*T]{}

// Client is a mock
type Client[T ingest.Identifiable] struct {
	mock.Mock
}

// Download is a mock function.
func (_m *Client[T]) Download(ctx context.Context, t T) (ingest.Object, error) {
	ret := _m.Called(ctx, t)

	var obj *Object
	if rf, ok := ret.Get(0).(func(context.Context, T) Object); ok {
		*obj = rf(ctx, t)
	} else {
		obj = ret.Get(0).(*Object)
	}

	var err error
	if rf, ok := ret.Get(1).(func(context.Context, T) error); ok {
		err = rf(ctx, t)
	} else {
		err = ret.Error(1)
	}
	return obj, err
}

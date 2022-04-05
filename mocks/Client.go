// This file is hand written

package mocks

import (
	"context"

	"github.com/mietright/ingest"

	"github.com/stretchr/testify/mock"
)

var (
	_ ingest.Client[*T] = &Client[*T]{}
	_ ingest.Object     = &Object{}
)

type Client[T ingest.Identifiable] struct {
	mock.Mock
}

// Close provides a mock function with given fields: _a0
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

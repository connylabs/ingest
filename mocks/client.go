// This file is hand written

package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/connylabs/ingest"
)

var _ ingest.Client = &Client{}

// Client is a mock
type Client struct {
	mock.Mock
}

// Download is a mock function.
func (_m *Client) Download(ctx context.Context, t ingest.Identifiable) (ingest.Object, error) {
	ret := _m.Called(ctx, t)

	var obj *Object
	if rf, ok := ret.Get(0).(func(context.Context, ingest.Identifiable) Object); ok {
		*obj = rf(ctx, t)
	} else {
		obj = ret.Get(0).(*Object)
	}

	var err error
	if rf, ok := ret.Get(1).(func(context.Context, ingest.Identifiable) error); ok {
		err = rf(ctx, t)
	} else {
		err = ret.Error(1)
	}
	return obj, err
}

// CleanUp is a mock function.
func (_m *Client) CleanUp(ctx context.Context, t ingest.Identifiable) error {
	ret := _m.Called(ctx, t)

	var err error
	if rf, ok := ret.Get(0).(func(context.Context, ingest.Identifiable) error); ok {
		err = rf(ctx, t)
	} else {
		err = ret.Error(0)
	}
	return err
}

// Code generated by mockery v2.15.0. DO NOT EDIT.

package mocks

import (
	context "context"

	ingest "github.com/connylabs/ingest"
	mock "github.com/stretchr/testify/mock"

	storage "github.com/connylabs/ingest/storage"

	url "net/url"
)

// Storage is an autogenerated mock type for the Storage type
type Storage struct {
	mock.Mock
}

type Storage_Expecter struct {
	mock *mock.Mock
}

func (_m *Storage) EXPECT() *Storage_Expecter {
	return &Storage_Expecter{mock: &_m.Mock}
}

// Stat provides a mock function with given fields: ctx, element
func (_m *Storage) Stat(ctx context.Context, element ingest.Codec) (*storage.ObjectInfo, error) {
	ret := _m.Called(ctx, element)

	var r0 *storage.ObjectInfo
	if rf, ok := ret.Get(0).(func(context.Context, ingest.Codec) *storage.ObjectInfo); ok {
		r0 = rf(ctx, element)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*storage.ObjectInfo)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, ingest.Codec) error); ok {
		r1 = rf(ctx, element)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Storage_Stat_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Stat'
type Storage_Stat_Call struct {
	*mock.Call
}

// Stat is a helper method to define mock.On call
//  - ctx context.Context
//  - element ingest.Codec
func (_e *Storage_Expecter) Stat(ctx interface{}, element interface{}) *Storage_Stat_Call {
	return &Storage_Stat_Call{Call: _e.mock.On("Stat", ctx, element)}
}

func (_c *Storage_Stat_Call) Run(run func(ctx context.Context, element ingest.Codec)) *Storage_Stat_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(ingest.Codec))
	})
	return _c
}

func (_c *Storage_Stat_Call) Return(_a0 *storage.ObjectInfo, _a1 error) *Storage_Stat_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

// Store provides a mock function with given fields: ctx, element, obj
func (_m *Storage) Store(ctx context.Context, element ingest.Codec, obj ingest.Object) (*url.URL, error) {
	ret := _m.Called(ctx, element, obj)

	var r0 *url.URL
	if rf, ok := ret.Get(0).(func(context.Context, ingest.Codec, ingest.Object) *url.URL); ok {
		r0 = rf(ctx, element, obj)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*url.URL)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, ingest.Codec, ingest.Object) error); ok {
		r1 = rf(ctx, element, obj)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Storage_Store_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Store'
type Storage_Store_Call struct {
	*mock.Call
}

// Store is a helper method to define mock.On call
//  - ctx context.Context
//  - element ingest.Codec
//  - obj ingest.Object
func (_e *Storage_Expecter) Store(ctx interface{}, element interface{}, obj interface{}) *Storage_Store_Call {
	return &Storage_Store_Call{Call: _e.mock.On("Store", ctx, element, obj)}
}

func (_c *Storage_Store_Call) Run(run func(ctx context.Context, element ingest.Codec, obj ingest.Object)) *Storage_Store_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(ingest.Codec), args[2].(ingest.Object))
	})
	return _c
}

func (_c *Storage_Store_Call) Return(_a0 *url.URL, _a1 error) *Storage_Store_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

type mockConstructorTestingTNewStorage interface {
	mock.TestingT
	Cleanup(func())
}

// NewStorage creates a new instance of Storage. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewStorage(t mockConstructorTestingTNewStorage) *Storage {
	mock := &Storage{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// Code generated by mockery v2.15.0. DO NOT EDIT.

package mocks

import (
	context "context"

	ingest "github.com/connylabs/ingest"
	mock "github.com/stretchr/testify/mock"
)

// Nexter is an autogenerated mock type for the Nexter type
type Nexter struct {
	mock.Mock
}

// Next provides a mock function with given fields: _a0
func (_m *Nexter) Next(_a0 context.Context) (*ingest.Codec, error) {
	ret := _m.Called(_a0)

	var r0 *ingest.Codec
	if rf, ok := ret.Get(0).(func(context.Context) *ingest.Codec); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*ingest.Codec)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Reset provides a mock function with given fields: _a0
func (_m *Nexter) Reset(_a0 context.Context) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTNewNexter interface {
	mock.TestingT
	Cleanup(func())
}

// NewNexter creates a new instance of Nexter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewNexter(t mockConstructorTestingTNewNexter) *Nexter {
	mock := &Nexter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

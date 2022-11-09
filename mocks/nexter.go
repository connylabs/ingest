// Code generated by mockery v2.10.2. DO NOT EDIT.

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
func (_m *Nexter) Next(_a0 context.Context) (*ingest.SimpleCodec, error) {
	ret := _m.Called(_a0)

	var r0 *ingest.SimpleCodec
	if rf, ok := ret.Get(0).(func(context.Context) *ingest.SimpleCodec); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*ingest.SimpleCodec)
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

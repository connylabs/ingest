// Code generated by mockery v2.15.0. DO NOT EDIT.

package mocks

import (
	context "context"

	ingest "github.com/connylabs/ingest"
	mock "github.com/stretchr/testify/mock"

	nats "github.com/nats-io/nats.go"
)

// Queue is an autogenerated mock type for the Queue type
type Queue struct {
	mock.Mock
}

// Close provides a mock function with given fields: _a0
func (_m *Queue) Close(_a0 context.Context) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Publish provides a mock function with given fields: _a0, _a1
func (_m *Queue) Publish(_a0 string, _a1 []byte) error {
	ret := _m.Called(_a0, _a1)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []byte) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PullSubscribe provides a mock function with given fields: _a0, _a1, _a2
func (_m *Queue) PullSubscribe(_a0 string, _a1 string, _a2 ...nats.SubOpt) (ingest.Subscription, error) {
	_va := make([]interface{}, len(_a2))
	for _i := range _a2 {
		_va[_i] = _a2[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, _a0, _a1)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 ingest.Subscription
	if rf, ok := ret.Get(0).(func(string, string, ...nats.SubOpt) ingest.Subscription); ok {
		r0 = rf(_a0, _a1, _a2...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(ingest.Subscription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, string, ...nats.SubOpt) error); ok {
		r1 = rf(_a0, _a1, _a2...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewQueue interface {
	mock.TestingT
	Cleanup(func())
}

// NewQueue creates a new instance of Queue. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewQueue(t mockConstructorTestingTNewQueue) *Queue {
	mock := &Queue{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

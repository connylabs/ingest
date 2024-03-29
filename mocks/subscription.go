// Code generated by mockery v2.15.0. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"

	nats "github.com/nats-io/nats.go"
)

// Subscription is an autogenerated mock type for the Subscription type
type Subscription struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *Subscription) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Pop provides a mock function with given fields: _a0, _a1
func (_m *Subscription) Pop(_a0 context.Context, _a1 int) ([]*nats.Msg, error) {
	ret := _m.Called(_a0, _a1)

	var r0 []*nats.Msg
	if rf, ok := ret.Get(0).(func(context.Context, int) []*nats.Msg); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*nats.Msg)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, int) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewSubscription interface {
	mock.TestingT
	Cleanup(func())
}

// NewSubscription creates a new instance of Subscription. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewSubscription(t mockConstructorTestingTNewSubscription) *Subscription {
	mock := &Subscription{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

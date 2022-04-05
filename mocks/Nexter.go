// This file is hand written

package mocks

import (
	"context"
	"fmt"

	ingest "github.com/mietright/ingest"

	mock "github.com/stretchr/testify/mock"
)

var _ ingest.Identifiable = &T{}

type T struct {
	Id string
}

func (t *T) ID() string {
	return t.Id
}

var _ ingest.Nexter[T] = &Nexter{}

// Nexter is a mock type for the Nexter type
type Nexter struct {
	mock.Mock
}

// Close provides a mock function with given fields: _a0
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

// Close provides a mock function with given fields: _a0
func (_m *Nexter) Next(_a0 context.Context) (*T, error) {
	ret := _m.Called(_a0)

	r0 := &T{}
	if rf, ok := ret.Get(0).(func(context.Context) *T); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*T)
		} else {
			r0 = nil
		}
	}
	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	fmt.Printf("next returning\n\tt=%v\n\terr=%v\n", r0, r1)
	return r0, r1
}

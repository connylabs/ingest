// This file is hand written

package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/mietright/ingest"
)

var (
	_ ingest.Identifiable = &T{}
	_ ingest.Nexter[T]    = &Nexter{}
)

// T is a mock.
type T struct {
	Id string
}

// ID returns the Id of T.
func (t *T) ID() string {
	return t.Id
}

// Nexter is a mock type for the Nexter type
type Nexter struct {
	mock.Mock
}

// Close provides a mock function with given fields: _a0
func (n *Nexter) Reset(ctx context.Context) error {
	ret := n.Called(ctx)

	var err error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		err = rf(ctx)
	} else {
		err = ret.Error(0)
	}

	return err
}

// Close provides a mock function with given fields: _a0
func (n *Nexter) Next(ctx context.Context) (*T, error) {
	ret := n.Called(ctx)

	t := &T{}
	if rf, ok := ret.Get(0).(func(context.Context) *T); ok {
		t = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			t = ret.Get(0).(*T)
		} else {
			t = nil
		}
	}

	var err error
	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		err = rf(ctx)
	} else {
		err = ret.Error(1)
	}

	return t, err
}

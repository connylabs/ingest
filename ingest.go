package ingest

import (
	"context"
	"io"

	"github.com/nats-io/nats.go"
)

// Queue is able to publish messages and subscribe to incoming messages
type Queue interface {
	Close(context.Context) error
	Publish(string, []byte) error
	PullSubscribe(string, string, ...nats.SubOpt) (Subscription, error)
}

// Subscription is able to pull a batch of messages from a stream for a pull consumer.
type Subscription interface {
	Pop(int, ...nats.PullOpt) ([]*nats.Msg, error)
	Close() error
}

// Nexter is able to list the objects available in the external API and returns them one by one.
// A Nexter must be implemented for the specific service.
type Nexter[T any] interface {
	// Reset initializes or resets the state of the Nexter.
	// After Reset, calls of Next should retrieve all objects.
	Reset(context.Context) error
	// Next returns one T that represents an object.
	// If all objects were returned by Next io.EOF must be returned.
	Next(context.Context) (*T, error)
}

// Enqueue is able to enqueue jobs into NATS.
type Enqueuer[T any] interface {
	Enqueue(context.Context) error
	// Runner runs Enqueue in the interval passes to New.
	Runner(context.Context) func() error
}

// DefaultBatchSize default size of the batch of messages pulled from the queue
const DefaultBatchSize = 8

// Dequeuer is able to dequeue documents from the queue and upload documents to the S3.
type Dequeuer interface {
	Dequeue(context.Context) error
	Runner(context.Context) func() error
}

// Object represents an object that can be uploaded into
// the object storage.
type Object interface {
	// html Content type
	MimeType() string
	// Length of the underlying buffer of the io.Reader
	Len() int64
	io.Reader
}

// Client is able to create an Object from a T.
// Client must be implemented by the uses.
type Client[T Identifiable] interface {
	// Download converts a T into an Object.
	// In most cases it will use the ID of the T to
	// download the object from an API, or it can create
	// the Object directly from the T.
	Download(context.Context, T) (Object, error)
}

// Identifiable must be implemented by the T.
// The ID returned by ID() will be uses as a key
// in the object storage.
type Identifiable interface {
	// ID returns a unique id.
	ID() string
}

package ingest

import (
	"context"
	"io"

	"github.com/nats-io/nats.go"
)

// DefaultBatchSize default size of the batch of messages pulled from the queue
const DefaultBatchSize = 8

// Queue is able to publish messages and subscribe to incoming messages
type Queue interface {
	Close(context.Context) error
	Publish(string, []byte) error
	PullSubscribe(string, string, ...nats.SubOpt) (Subscription, error)
}

// Subscription is able to pull a batch of messages from a stream for a pull consumer.
type Subscription interface {
	Pop(context.Context, int) ([]*nats.Msg, error)
	Close() error
}

// Nexter is able to list the elements available in the external API and returns them one by one.
// A Nexter must be implemented for the specific service.
type Nexter interface {
	// Reset initializes or resets the state of the Nexter.
	// After Reset, calls of Next should retrieve all elements.
	Reset(context.Context) error
	// Next returns a *Codec that represents an element.
	// If all elements were returned by Next, io.EOF must be returned.
	Next(context.Context) (*Codec, error)
}

// Enqueuer is able to enqueue elements into NATS.
type Enqueuer interface {
	// Enqueue adds all of the elements that the Nexter will produce into the queue.
	Enqueue(context.Context) error
}

// Dequeuer is able to dequeue elements from the queue and upload objects to object storage.
type Dequeuer interface {
	// Dequeue will read relevant messages on a Queue and upload the corresponding
	// Objects to object storage until the given context is cancelled.
	Dequeue(context.Context) error
}

// Object represents an object that can be uploaded into object storage.
type Object struct {
	// MimeType is the HTTP-style Content-Type of the object.
	MimeType string
	// Len is the length of the underlying buffer of the io.Reader.
	Len    int64
	Reader io.Reader
}

// Client is able to create an Object from a Codec.
// Client must be implemented by the caller.
type Client interface {
	// Download converts a Codec into an Object.
	// In most cases it will use the ID of the Codec
	// to download the object from an API,
	Download(context.Context, Codec) (*Object, error)
	// CleanUp is called after an object is uploaded
	// to object storage. In most cases, this will
	// serve the purpose of removing the object from
	// the source API so that it is no longer returned
	// by the Nexter and as such is not resynchronized.
	// This method must be idempotent and safe to call
	// multiple times.
	CleanUp(context.Context, Codec) error
}

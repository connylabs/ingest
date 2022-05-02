# ingest

Ingest is a library that makes it easy to build worker-queue services that use a [NATS](https://nats.io/) queue to synchronize objects from any API into an S3-compliant object storage.

## Usage

The library is split into two packages:
1. `enqueue`: responsible for putting items into a queue; and
2. `dequeue`: responsible for reading items from the queue, perform the download from the API, and the upload object into object storage.

### Enqueuer

The `Enqueuer` fetches all elements from the external API and puts them into the NATS queue.
It can put either just an ID or the entire contents of the object into the queue.

For a new service you must implement the following `Nexter` interface:

```go
// Nexter is able to list the elements available in the external API and returns them one by one.
// A Nexter must be implemented for the specific service.
type Nexter[T any] interface {
	// Reset initializes or resets the state of the Nexter.
	// After Reset, calls of Next should retrieve all elements.
	Reset(context.Context) error
	// Next returns one T that represents an element.
	// If all elements were returned by Next, io.EOF must be returned.
	Next(context.Context) (*T, error)
}
```

`T` can either be an ID or the whole object.

To use the Enqueuer create, a new one with `enqueue.New`.
It implements the following interface:

```go
// Enqueuer is able to enqueue elements into NATS.
type Enqueuer interface {
	// Enqueue adds all of the elements that the Nexter will produce into the queue.
	Enqueue(context.Context) error
}
```

### Dequeuer

The `Dequeuer` reads from the queue and uploads the object into object storage.
You need implement the `Client` interface.
**Note**: if the entire object is transported over the queue, then the `Download` operation can directly convert the given `T` into an object rather than download it from the API.

```go
// Object represents an object that can be uploaded into object storage.
type Object interface {
	// MimeType is the HTTP-style Content-Type of the object.
	MimeType() string
	// Len is the length of the underlying buffer of the io.Reader.
	Len() int64
	io.Reader
}

// Client is able to create an Object from a T.
// Client must be implemented by the caller.
type Client[T Identifiable] interface {
	// Download converts a T into an Object.
	// In most cases it will use the ID of the T to
	// download the object from an API,
        // however in some cases it is possible to create
	// the Object directly from the T.
	Download(context.Context, T) (Object, error)
	// CleanUp is called after an object is uploaded
	// to object storage. In most cases, this will
	// serve the purpose of removing the object from
	// the source API so that it is no longer returned
	// by the Nexter and as such is not resynchronized.
	// This method must be idempotent and safe to call
	// multiple times.
	CleanUp(context.Context, T) error
}

// Identifiable must be implemented by the caller for their T.
// The ID returned by ID() will be uses as a key  in object storage.
type Identifiable interface {
	// ID returns a unique id.
	ID() string
}
```

To use the Dequeuer, create a new one with `dequeuer.New`.
It implements the following interface:

```go
// Dequeuer is able to dequeue elements from the queue and upload objects to object storage.
type Dequeuer interface {
	// Dequeue will read relevant messages on a Queue and upload the corresponding
	// Objects to object storage until the given context is cancelled.
	Dequeue(context.Context) error
}
```

### T

This library uses generics for the `T` type, which represents an element from the source API.
With generics, the library is able to marshal and unmarshal items when writing and reading to and from the NATS queue respectively.
Otherwise, users would have to implement a `T` interface that has `UnmarshalFrom([]byte)` and `Marshal() []byte` methods.

**Note**: the user definition of `T` must have public fields.
Private fields will not be written to the NATS queue.

## Writing Binaries

This library provides a convenience `cmd` package that can be helpful for writing binaries.
The library's `NewEnqueuerRunner` and `NewDequeuerRunner` functions return runnable functions that will execute an `Enqueuer` or a `Dequeuer` respectively forever until a given context is cancelled.

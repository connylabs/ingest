# ingest

Ingest is a library that makes it easy to build services that use a [NATS](https://nats.io/) queue to synchronize objects from any API into an S3 compliant object storage.

## Usage

The library is split into two packages.
One part puts Ts into a queue.
Another reads them from the queue and performs the download from the API and the upload into the object storage.

### Enqueuer

The Enqueuer fetches all items from the external API and puts them into the NATS queue.
It can put only an ID or the whole content of the object into the queue.

For a new service you must implement the `Nexter` interface.

```go
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
```

The T can either be an ID or the whole object.

To use the Enqueuer create a new one with `enqueue.New`.
It implements the following interface.
Use either function to start the Enqueuer.

```go
// Enqueue is able to enqueue jobs into NATS.
type Enqueue[T any] interface {
	Enqueue(context.Context) error
	// Runner runs Enqueue in the interval passes to New.
	Runner(context.Context) func() error
}
```

### Dequeuer

The Dequeuer reads from the queue and uploads the object into S3.
You need implement the Client interface.
Note that the Download operation could also just convert the T into an object when loading the whole object into the [NATS](https://nats.io/) queue.

```go
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
```

To use the Dequeuer, create a new one with `dequeuer.New`.
It implements the following interface.
Use either function to start the Dequeuer.

```go
// Dequeuer is able to dequeue element from the queue and upload documents to S3.
type Dequeuer interface {
	Dequeue(context.Context) error
	Runner(context.Context) func() error
}
```

### T

This library uses generics for the T type.
Otherwise users would have to implement an interface that has `UnmarshalFrom([]byte)` and `Marshal() []byte` methods.
With generics the library marshals and unmarshals to write and read to and from the NATS queue.

**Note**:The user definition of T must have public fields.
Private fields will not be written to the NATS queue.

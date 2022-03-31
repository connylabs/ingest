# ingest

Ingest is a library that makes it easy to build services that use a [NATS](https://nats.io/) queue to synchronize objects from any API into an S3 compliant object storage.

## Usage

The library is split into two packages.
One part puts Jobs into a queue.
Another reads them from the queue and performs the download from the API and the upload into the object storage.

### Enqueuer

The Enqueuer fetches all items from the external API and puts them into the NATS queue.
It can put only an ID or the whole content of the object into the queue.

For a new service you must implement the `Nexter` interface.

```go
// Nexter is able to list the objects available in the external API and returns them one by one.
// A Nexter must be implemented for the specific service.
type Nexter[Job any] interface {
	// Reset initializes or resets the state of the Nexter.
	// After Reset, calls of Next should retrieve all objects.
	Reset(context.Context) (Nexter[Job], error)
	// Next returns one Job that represents an object.
	// If all objects were returned by Next io.EOF must be returned.
	Next(context.Context) (Job, error)
}
```

The Job can either be an ID or the whole object.

### Dequeuer

The Dequeuer reads from the queue and uploads the object into S3.
You need implement the Client interface.
Note that the Download operation could also just convert the Job into an object when loading the whole object into the [NATS](https://nats.io/) queue.

```go
type Client[Job any] interface {
	Download(context.Context, Job) (Object, error)
}

type Identifiable interface {
	ID() string
}
```

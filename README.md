# ingest

Ingest is a pluggable tool that makes it easy to orchestrate the synchronize of objects across multiple sources.

## Concept

Ingest includes a main binary and some common plugins like s3.
Each plugin can implement a data source, target or both.
The S3 plugin implements both the source and target interface, but some plugins may only be able to act as either of them.
The plugins are loaded at runtime and enable users to implement their own custom plugins.

### Workflows

A workflow defines a data source and one or more targets.
When configured, objects from the source will be copied to all targets.

## Configuration

The exact configurations depends on the plugin.
Typically, the S3 plugin requires an URL, an access key and a secret access key.
This may differ from plugins that don't require authentication or a different kind of authentication.

The following example shows the configuration to copy object between two instances of S3:

```yaml
sources:
- name: foo_1
  type: s3
  endpoint: source.amazon.com
  bucket: source
  prefix: prefix/
  accessKeyID: key
  secretAccessKey: secret
destinations:
- name: bar_1
  type: s3
  endpoint: destination.amazon.com
  insecure: true
  bucket: destination
  prefix: prefix1/
  metafilesPrefix: meta/
  accessKeyID: key
  secretAccessKey: secret
workflows:
- name: foo_1-bar_1
  source: foo_1
  destinations:
  - bar_1
  batchSize: 1
  interval: 300s
  cleanUp: true
  webhook: http://localhost:8080
```

## Deployment

The deployment of ingest contains of two parts.

On part is called the enqueuer.
It is started with the flag `--mode=enqueue`.
It will scan the configured sources and push a message for each item in the source to its [NATS](https://nats.io/) stream.

The other part is the dequeuer.
It is started with the flag `--mode=dequeue`.
It will pop messages from the NATS stream and copy each object identified by the [NATS](https://nats.io/) message to the configured targets.



## Usage as a Library

The library is split into two packages:
1. `enqueue`: responsible for putting items into a queue; and
2. `dequeue`: responsible for reading items from the queue, perform the download from the API, and the upload object into object storage.

### Enqueuer

The `Enqueuer` fetches all elements from the external API and puts them into the NATS queue.
It can put either just an ID or the entire contents of the object into the queue.

For a new service you must implement the following `Nexter` interface:

[embedmd]:# (ingest.go /\/\/ Nexter/ /}/)
```go
// Nexter is able to list the elements available in the external API and returns them one by one.
// A Nexter must be implemented for the specific service.
type Nexter interface {
	// Reset initializes or resets the state of the Nexter.
	// After Reset, calls of Next should retrieve all elements.
	Reset(context.Context) error
	// Next returns one T that represents an element.
	// If all elements were returned by Next, io.EOF must be returned.
	Next(context.Context) (Identifiable, error)
}
```

To use the Enqueuer create, a new one with `enqueue.New`.
It implements the following interface:

[embedmd]:# (ingest.go /\/\/ Enqueuer/ /}/)
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

[embedmd]:# (ingest.go /\/\/ Object / /}/)
```go
// Object represents an object that can be uploaded into object storage.
type Object interface {
	// MimeType is the HTTP-style Content-Type of the object.
	MimeType() string
	// Len is the length of the underlying buffer of the io.Reader.
	Len() int64
	io.Reader
}
```

[embedmd]:# (ingest.go /\/\/ Client/ /}/)
```go
// Client is able to create an Object from a T.
// Client must be implemented by the caller.
type Client interface {
	// Download converts a T into an Object.
	// In most cases it will use the ID of the T to
	// download the object from an API,
	// however in some cases it is possible to create
	// the Object directly from the T.
	Download(context.Context, Identifiable) (Object, error)
	// CleanUp is called after an object is uploaded
	// to object storage. In most cases, this will
	// serve the purpose of removing the object from
	// the source API so that it is no longer returned
	// by the Nexter and as such is not resynchronized.
	// This method must be idempotent and safe to call
	// multiple times.
	CleanUp(context.Context, Identifiable) error
}
```

[embedmd]:# (ingest.go /\/\/ Identifiable/ /}/)
```go
// Identifiable must be implemented by the caller for their T.
// The ID is used to identify the T in the source API.
// The Name is used as a key in the destination object storage.
type Identifiable interface {
	// ID returns a unique id.
	ID() string
	// Nane returns a name for the destination.
	Name() string
}
```

To use the Dequeuer, create a new one with `dequeuer.New`.
It implements the following interface:

[embedmd]:# (ingest.go /\/\/ Dequeuer/ /}/)
```go
// Dequeuer is able to dequeue elements from the queue and upload objects to object storage.
type Dequeuer interface {
	// Dequeue will read relevant messages on a Queue and upload the corresponding
	// Objects to object storage until the given context is cancelled.
	Dequeue(context.Context) error
}
```

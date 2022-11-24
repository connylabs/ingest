package ingest

import "encoding/json"

// Codec implements the Identifiable and Codec interfaces
// and is used as the default marshaler/unmarshaler for Identifiables.
type Codec struct {
	// ID is the ID of the Identifiable. It should uniquely
	// identify a resource in an API.
	ID string `json:"id"`
	// Name is the name of the resource. Destinations will use this
	// as the key under which to store objects.
	Name string `json:"name"`
}

// Marshal serializes the Identifiable so it can be sent on the queue.
func (c *Codec) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

// Unmarshal deserializes a message from the qeueue back into an Identifiable.
func (c *Codec) Unmarshal(data []byte) error {
	return json.Unmarshal(data, c)
}

// NewCodec creates a Codec from an existing Identifiable.
func NewCodec(id, name string) Codec {
	return Codec{ID: id, Name: name}
}

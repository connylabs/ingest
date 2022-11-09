package ingest

import "encoding/json"

// SimpleCodec implements the Identifiable and Codec interfaces
// and is used as the default marshaler/unmarshaler for Identifiables.
type SimpleCodec struct {
	XID   string `json:"id"`
	XName string `json:"name"`
}

// ID returns the ID of the Identifiable. It should uniquely
// identify a resource in an API.
func (s *SimpleCodec) ID() string {
	return s.XID
}

// Name returns the name of the resource. Destinations will use this
// as the key under which to store objects.
func (s *SimpleCodec) Name() string {
	return s.XName
}

// Marshal serializes the Identifiable so it can be sent on the queue.
func (s *SimpleCodec) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

// Unmarshal deserializes a message from the qeueue back into an Identifiable.
func (s *SimpleCodec) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

// NewCodec creates a Codec from an existing Identifiable.
func NewCodec(id, name string) *SimpleCodec {
	return &SimpleCodec{XID: id, XName: name}
}

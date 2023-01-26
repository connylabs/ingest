package ingest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCodecMarshalling(t *testing.T) {
	c := NewCodec("id", "name", []byte(`{"meta":"value"}`))

	d, err := c.Marshal()
	assert.NoError(t, err)
	nc := new(Codec)
	assert.NoError(t, nc.Unmarshal(d))
	assert.Equal(t, &c, nc)
}

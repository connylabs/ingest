package config

import (
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/connylabs/ingest/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	for _, tc := range []struct {
		name   string
		paths  []string
		config []byte
		err    error
	}{
		{
			name:  "one path",
			paths: []string{fmt.Sprintf("../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)},
			config: []byte(`
sources:
- name: foo_1
  type: s3
- name: foo_2
  type: s3
destinations:
- name: bar_1
  type: s3
- name: bar_2
  type: s3
workflows:
- name: foo_1-bar_1
  source: foo_1
  destinations:
  - bar_1
- name: foo_2-bar_1-bar_2
  source: foo_2
  destinations:
  - bar_1
  - bar_2
`),
		},
		{
			name:  "two path",
			paths: []string{"../bin/plugin/to/nowhere", fmt.Sprintf("../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)},
			config: []byte(`
sources:
- name: foo_1
  type: s3
- name: foo_2
  type: s3
destinations:
- name: bar_1
  type: s3
- name: bar_2
  type: s3
workflows:
- name: foo_1-bar_1
  source: foo_1
  destinations:
  - bar_1
- name: foo_2-bar_1-bar_2
  source: foo_2
  destinations:
  - bar_1
  - bar_2
`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := New(tc.config)
			require.NoError(t, err)

			pm := &plugin.PluginManager{}
			t.Cleanup(pm.Stop)
			ss, ds, err := c.ConfigurePlugins(pm, tc.paths)
			for _, s := range ss {
				_, ok := s.(*SourceTyper)
				assert.True(t, ok)
			}
			for _, d := range ds {
				_, ok := d.(*DestinationTyper)
				assert.True(t, ok)
			}
			assert.ErrorIs(t, err, tc.err, errors.Unwrap(err))
		})
	}
}

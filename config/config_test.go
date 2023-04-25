package config

import (
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/connylabs/ingest/plugin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWithEnv(t *testing.T) {
	secret := "secret"
	os.Setenv("INGEST_SECRET", secret)

	c, err := New([]byte(`
sources:
- name: foo_1
  type: s3
  accessKey: $INGEST_SECRET
`), nil)
	require.NoError(t, err)
	i, ok := c.Sources[0].Config["accessKey"]
	require.True(t, ok, "accessKey not in map")
	t.Log(i)
	s, ok := i.(string)
	require.True(t, ok, "accessKey is not string")

	assert.Equal(t, secret, s)
}

func TestNew(t *testing.T) {
	for _, tc := range []struct {
		name          string
		paths         []string
		config        []byte
		err           error
		strict        bool
		nSources      int
		nDestinations int
	}{
		{
			name:          "one path",
			paths:         []string{fmt.Sprintf("../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)},
			nSources:      2,
			nDestinations: 2,
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
			name:          "two path",
			paths:         []string{"../bin/plugin/to/nowhere", fmt.Sprintf("../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)},
			nSources:      2,
			nDestinations: 2,
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
			name:          "unused sources and destinations",
			paths:         []string{fmt.Sprintf("../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)},
			nSources:      2,
			nDestinations: 2,
			config: []byte(`
sources:
- name: unused_1
  type: s3
- name: unused_2
  type: s3
- name: foo_1
  type: s3
- name: foo_2
  type: s3
destinations:
- name: unused_1
  type: s3
- name: unused_2
  type: s3
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
			name:   "strict workflow referencing non-existant source",
			paths:  []string{fmt.Sprintf("../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)},
			err:    fmt.Errorf("workflow %q references non-existent source %q", "foo_2-bar_1-bar_2", "foo_2"),
			strict: true,
			config: []byte(`
sources:
- name: foo_1
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
			name:   "strict workflow referencing invalid source",
			paths:  []string{fmt.Sprintf("../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)},
			err:    fmt.Errorf("cannot instantiate source %q", "foo_2"),
			strict: true,
			config: []byte(`
sources:
- name: foo_1
  type: s3
- name: foo_2
  type: s3
  bucket: 0
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
			name:          "workflow referencing non-existant source",
			paths:         []string{fmt.Sprintf("../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)},
			nSources:      1,
			nDestinations: 1,
			config: []byte(`
sources:
- name: foo_1
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
			name:          "workflow referencing invalid source",
			paths:         []string{fmt.Sprintf("../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)},
			nSources:      1,
			nDestinations: 1,
			config: []byte(`
sources:
- name: foo_1
  type: s3
- name: foo_2
  type: s3
  bucket: 0
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
			name:   "strict workflow referencing non-existant destination",
			paths:  []string{fmt.Sprintf("../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)},
			err:    fmt.Errorf("workflow %q references non-existent destination %q", "foo_2-bar_1-bar_2", "bar_2"),
			strict: true,
			config: []byte(`
sources:
- name: foo_1
  type: s3
- name: foo_2
  type: s3
destinations:
- name: bar_1
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
			name:   "strict workflow referencing invalid destination",
			paths:  []string{fmt.Sprintf("../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)},
			err:    fmt.Errorf("cannot instantiate destination %q", "bar_2"),
			strict: true,
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
  bucket: 0
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
			name:          "workflow referencing non-existant destination",
			paths:         []string{fmt.Sprintf("../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)},
			nSources:      2,
			nDestinations: 1,
			config: []byte(`
sources:
- name: foo_1
  type: s3
- name: foo_2
  type: s3
destinations:
- name: bar_1
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
			name:          "workflow referencing invalid destination",
			paths:         []string{fmt.Sprintf("../bin/plugin/%s/%s", runtime.GOOS, runtime.GOARCH)},
			nSources:      2,
			nDestinations: 1,
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
  bucket: 0
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
			r := prometheus.NewRegistry()
			c, err := New(tc.config, r)
			require.NoError(t, err)

			pm := plugin.NewPluginManager(0, nil)
			t.Cleanup(pm.Stop)
			ss, ds, err := c.ConfigurePlugins(pm, tc.paths, tc.strict)
			assert.Equal(t, tc.nSources, len(ss))
			assert.Equal(t, tc.nDestinations, len(ds))
			for _, s := range ss {
				_, ok := s.(*SourceTyper)
				assert.True(t, ok)
			}
			for _, d := range ds {
				_, ok := d.(*DestinationTyper)
				assert.True(t, ok)
			}
			if tc.err == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tc.err.Error())
			}
			metricFamilies, err := r.Gather()
			assert.NoError(t, err)
			assert.Equal(t, len(metricFamilies), 1)
		})
	}
}

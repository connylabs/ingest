//go:build tools
// +build tools

package ingest

import (
	_ "github.com/campoy/embedmd"
	_ "github.com/vektra/mockery/v2"
	_ "golang.org/x/lint/golint"
)

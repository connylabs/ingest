//go:build tools
// +build tools

package ingest

import (
	_ "github.com/campoy/embedmd"
	_ "github.com/minio/mc"
	_ "github.com/nats-io/natscli/nats"
	_ "github.com/vektra/mockery/v2"
)

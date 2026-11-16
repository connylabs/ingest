package temporal

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	sdklog "go.temporal.io/sdk/log"
)

type logger struct {
	l log.Logger
}

// NewLogger adapts a go-kit logger to the logger interface of the Temporal SDK.
func NewLogger(l log.Logger) sdklog.Logger {
	if l == nil {
		l = log.NewNopLogger()
	}
	return &logger{l: l}
}

func (l *logger) Debug(msg string, keyvals ...interface{}) {
	level.Debug(l.l).Log(append([]interface{}{"msg", msg}, keyvals...)...)
}

func (l *logger) Info(msg string, keyvals ...interface{}) {
	level.Info(l.l).Log(append([]interface{}{"msg", msg}, keyvals...)...)
}

func (l *logger) Warn(msg string, keyvals ...interface{}) {
	level.Warn(l.l).Log(append([]interface{}{"msg", msg}, keyvals...)...)
}

func (l *logger) Error(msg string, keyvals ...interface{}) {
	level.Error(l.l).Log(append([]interface{}{"msg", msg}, keyvals...)...)
}

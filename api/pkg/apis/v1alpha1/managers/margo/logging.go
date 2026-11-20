package margo

import (
	"fmt"
	"strings"

	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
)

// Logger is the interface used across the margo package so that
// managers and state machines don't depend on a concrete logger type.
type Logger interface {
	Debug(msg string, kv ...interface{})
	Info(msg string, kv ...interface{})
	Warn(msg string, kv ...interface{})
	Error(msg string, kv ...interface{})
}

// kvLogger wraps the Symphony logger and formats "msg", key, value, key, value...
// into a single readable line: "msg key value key value".
type kvLogger struct {
	base logger.Logger
}

func newKVLogger(name string) *kvLogger {
	return &kvLogger{base: logger.NewLogger(name)}
}

func format(msg string, kv ...interface{}) string {
	var sb strings.Builder
	sb.WriteString(msg)
	for i := 0; i < len(kv); i += 2 {
		sb.WriteString(" ")
		sb.WriteString(fmt.Sprint(kv[i]))
		if i+1 < len(kv) {
			sb.WriteString(" ")
			sb.WriteString(fmt.Sprint(kv[i+1]))
		}
	}
	return sb.String()
}

func (l *kvLogger) Debug(msg string, kv ...interface{}) { l.base.Debug(format(msg, kv...)) }
func (l *kvLogger) Info(msg string, kv ...interface{})  { l.base.Info(format(msg, kv...)) }
func (l *kvLogger) Warn(msg string, kv ...interface{})  { l.base.Warn(format(msg, kv...)) }
func (l *kvLogger) Error(msg string, kv ...interface{}) { l.base.Error(format(msg, kv...)) }

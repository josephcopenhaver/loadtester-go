package loadtester

import (
	"context"
	"os"

	"log/slog"
)

type StructuredLogger interface {
	Debug(msg string, keysAndValues ...interface{})
	DebugContext(ctx context.Context, msg string, keysAndValues ...interface{})
	Warn(msg string, keysAndValues ...interface{})
	WarnContext(ctx context.Context, msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})
	ErrorContext(ctx context.Context, msg string, keysAndValues ...interface{})
	Info(msg string, keysAndValues ...interface{})
	InfoContext(ctx context.Context, msg string, keysAndValues ...interface{})
}

func NewLogger(level slog.Level) (*slog.Logger, error) {
	return slog.New(slog.NewJSONHandler(
		os.Stdout,
		&slog.HandlerOptions{
			AddSource: true,
			Level:     level,
		},
	)), nil
}

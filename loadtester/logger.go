package loadtester

import (
	"context"
	"os"

	"log/slog"
)

type StructuredLogger interface {
	Debug(msg string, keysAndValues ...any)
	DebugContext(ctx context.Context, msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
	WarnContext(ctx context.Context, msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
	ErrorContext(ctx context.Context, msg string, keysAndValues ...any)
	Info(msg string, keysAndValues ...any)
	InfoContext(ctx context.Context, msg string, keysAndValues ...any)
}

// NewLogger should likely be followed by a call to slog.SetDefault with the logger
// returned by this function if called in the program's main context.
func NewLogger(level slog.Level) (*slog.Logger, error) {
	return slog.New(slog.NewJSONHandler(
		os.Stdout,
		&slog.HandlerOptions{
			AddSource: true,
			Level:     level,
		},
	)), nil
}

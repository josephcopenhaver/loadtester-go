package loadtester

import (
	"context"
	"os"

	"log/slog"
)

type StructuredLogger interface {
	LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr)
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

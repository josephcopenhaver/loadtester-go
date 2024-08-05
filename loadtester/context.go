package loadtester

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

// RootContext returns a context that is canceled when the
// system process receives an interrupt, sigint, or sigterm
//
// Also returns a function that can be used to cancel the context.
func RootContext(logger StructuredLogger) (context.Context, func()) {

	ctx, cancel := context.WithCancel(context.Background())

	procDone := make(chan os.Signal, 1)

	signal.Notify(procDone, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		defer cancel()

		const evtName = "shutdown requested"
		const typeName = "signaler"

		done := ctx.Done()

		select {
		case <-procDone:
			if logger == nil {
				return
			}

			logger.LogAttrs(ctx, slog.LevelWarn,
				evtName,
				slog.String(typeName, "process"),
			)
		case <-done:
			if logger == nil {
				return
			}

			logger.LogAttrs(ctx, slog.LevelWarn,
				evtName,
				slog.String(typeName, "context"),
				slog.Any("error", ctx.Err()),
				slog.Any("cause", context.Cause(ctx)),
			)
		}
	}()

	return ctx, cancel
}

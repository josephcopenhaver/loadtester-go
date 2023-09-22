package loadtester

import (
	"context"
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

		done := ctx.Done()

		var requester string
		select {
		case <-procDone:
			requester = "user"
		case <-done:
			requester = "process"
		}

		if logger == nil {
			return
		}

		logger.WarnContext(ctx,
			"shutdown requested",
			"requester", requester,
		)
	}()

	return ctx, cancel
}

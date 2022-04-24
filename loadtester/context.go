package loadtester

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
)

// RootContext returns a context that is canceled when the
// system process receives an interrupt, sigint, or sigterm
//
// Also returns a function that can be used to cancel the context.
func RootContext(logger *zap.SugaredLogger) (context.Context, func()) {

	ctx, cancel := context.WithCancel(context.Background())

	procDone := make(chan os.Signal, 1)

	signal.Notify(procDone, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		defer cancel()

		done := ctx.Done()

		requester := "unknown"
		select {
		case <-procDone:
			requester = "user"
		case <-done:
			requester = "process"
		}

		if logger == nil {
			return
		}

		logger.Warnw(
			"shutdown requested",
			"requester", requester,
		)
	}()

	return ctx, cancel
}

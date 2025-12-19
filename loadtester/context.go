package loadtester

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

const (
	shutdownLogEvtName  = "shutdown requested"
	shutdownLogTypeName = "signaler"
)

// var (
// 	// notShutdown indicates either a successful run or a panic
// 	//
// 	// not a shutdown signal
// 	errNotShutdown = errors.New("not shutdown")
// )

type processShutdownSignalErr struct {
	s os.Signal
}

func (ps processShutdownSignalErr) Error() string {
	return "process shutdown signal received: " + ps.s.String()
}

// RootContext returns a context that is canceled when the
// system process receives an interrupt, sigint, or sigterm
//
// Also returns a function that can be used to cancel the context.
func RootContext(logger StructuredLogger) (context.Context, func()) {
	procDone := make(chan os.Signal, 1)
	signal.Notify(procDone, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// signal.Stop(procDone) is intentionally not called because there is no way
	// to know when the usage context has fully completed and some other handler
	// i.e. the default handler should take over.
	//
	// If you desire that or the ability to set a cause then use
	// WithRootShutdownSignalContext instead.

	ctx, cancel := context.WithCancelCause(context.Background())

	go func() {
		done := ctx.Done()

		select {
		case s := <-procDone:
			cancel(processShutdownSignalErr{s})
		case <-done:
		}

		if logger == nil {
			return
		}

		c := context.Cause(ctx)

		if sigErr, ok := c.(processShutdownSignalErr); ok {
			logger.LogAttrs(ctx, slog.LevelWarn,
				shutdownLogEvtName,
				slog.String(shutdownLogTypeName, "process"),
				slog.String("signal", sigErr.s.String()),
			)
			return
		}

		logger.LogAttrs(ctx, slog.LevelWarn,
			shutdownLogEvtName,
			slog.String(shutdownLogTypeName, "context"),
			slog.Any("error", ctx.Err()),
			slog.Any("cause", c),
		)
	}()

	return ctx, func() { cancel(context.Canceled) }
}

// func WithRootShutdownSignalContext(f func(context.Context, context.CancelCauseFunc, func(StructuredLogger))) {
// 	procDone := make(chan os.Signal, 1)
// 	signal.Notify(procDone, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

// 	var setLogger func(StructuredLogger)
// 	var getLogger func() StructuredLogger
// 	{
// 		var logger atomic.Value

// 		setLogger = func(slog StructuredLogger) {
// 			if slog == nil {
// 				panic("nil logger")
// 			}

// 			logger.Store(slog)
// 		}
// 		getLogger = func() StructuredLogger {
// 			if v := logger.Load(); v != nil {
// 				return v.(StructuredLogger)
// 			}

// 			return nil
// 		}
// 	}

// 	ctx, cancel := context.WithCancelCause(context.Background())

// 	var wg sync.WaitGroup
// 	wg.Go(func() {
// 		const evtName = "shutdown requested"
// 		const typeName = "signaler"

// 		done := ctx.Done()

// 		select {
// 		case s := <-procDone:
// 			cancel(processShutdownSignalErr{s})
// 		case <-done:
// 		}

// 		logger := getLogger()
// 		if logger == nil {
// 			return
// 		}

// 		c := context.Cause(ctx)
// 		if c == errNotShutdown {
// 			return
// 		}

// 		if sigErr, ok := c.(processShutdownSignalErr); ok {
// 			logger.LogAttrs(ctx, slog.LevelWarn,
// 				evtName,
// 				slog.String(typeName, "process"),
// 				slog.String("signal", sigErr.s.String()),
// 			)
// 			return
// 		}

// 		logger.LogAttrs(ctx, slog.LevelWarn,
// 			evtName,
// 			slog.String(typeName, "context"),
// 			slog.Any("error", ctx.Err()),
// 			slog.Any("cause", c),
// 		)
// 	})

// 	defer wg.Wait()
// 	defer signal.Stop(procDone)
// 	defer cancel(errNotShutdown)

// 	defer func() {
// 		logger := getLogger()
// 		if logger == nil {
// 			return
// 		}

// 		r := recover()
// 		if r == nil {
// 			return
// 		}
// 		defer panic(r)

// 		logger.LogAttrs(ctx, slog.LevelError,
// 			"root context panic",
// 			slog.Any("error", r),
// 		)
// 	}()

// 	f(ctx, cancel, setLogger)
// }

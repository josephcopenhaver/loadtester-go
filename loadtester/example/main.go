package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/josephcopenhaver/loadtester-go/v5/loadtester"
)

type task struct{}

func (t *task) Do(ctx context.Context, workerID int) error {
	return nil
}

type myTaskReader struct{}

func (tr *myTaskReader) ReadTasks(p []loadtester.Doer) int {
	// make sure you only fill up to len
	// filling less than len will signal that the loadtest is over

	var i int
	for i < len(p) {
		p[i] = &task{}
		i++
	}

	return i
}

func newMyTaskReader() *myTaskReader {
	return &myTaskReader{}
}

func main() {

	var logger loadtester.StructuredLogger
	{
		level := slog.LevelInfo

		if s := os.Getenv("LOG_LEVEL"); s != "" {
			var v slog.Level
			err := v.UnmarshalText([]byte(s))
			if err != nil {
				panic(fmt.Errorf("failed to parse LOG_LEVEL environment variable: %w", err))
			}

			level = v
		}

		v, err := loadtester.NewLogger(level)
		if err != nil {
			panic(err)
		}

		slog.SetDefault(v)
		logger = v
	}

	ctx, cancel := loadtester.RootContext(logger)
	defer cancel()

	tr := newMyTaskReader()

	numWorkers := 5

	op := loadtester.NewOpts()
	lt, err := loadtester.NewLoadtest(
		op.TaskReader(tr),
		op.Logger(logger),
		op.NumWorkers(numWorkers),
		op.NumIntervalTasks(25),
		op.Interval(1*time.Second),
		op.Retry(false), // default is true; not required for this example since no tasks can be retried, plus saves some minor compute and disk io
		// op.MetricsLatencyPercentile(true), // default is false
		// op.MetricsLatencyVarianceEnabled(true),    // default is false
		// op.FlushRetriesOnShutdown(true), // default is false
		// op.MetricsCsv(false) // default is true; set to false to stop creating a metrics.csv file on loadtest run
	)
	if err != nil {
		panic(err)
	}

	// TODO: defer any loadtest cleanup here

	var wg sync.WaitGroup
	defer func() {
		// just to ensure all child workers exit before any cleanup runs
		//
		// it's duplicated on the positive path and does no harm to be called twice on the positive path
		logger.LogAttrs(ctx, slog.LevelDebug, "post-wg-decl: waiting for all goroutines to finish")
		wg.Wait()
		logger.LogAttrs(ctx, slog.LevelDebug, "post-wg-decl: all goroutines finished")
	}()

	//
	// start loadtest routine
	//
	wg.Add(1)
	go func() {
		defer wg.Done()

		// ensure the parent context is canceled when this critical goroutine ends
		defer cancel()

		// note, if you do not want to support user input then just end main by starting
		// the loadtest and don't use a wait group or goroutine for it

		if err := lt.Run(ctx); err != nil {
			logger.LogAttrs(ctx, slog.LevelError,
				"loadtest errored",
				slog.Any("error", err),
				slog.Bool("panic", true),
			)
			panic(err)
		}
	}()

	//
	// support user input to adjust the loadtest config
	//

	// define input handling channel and closer
	inputChan, closeInputChan := func() (chan string, func()) {
		c := make(chan string)

		closer := sync.OnceFunc(func() {
			close(c)
		})

		return c, closer
	}()

	//
	// start example user line input handling routines
	//

	// routine that listens for context done and closes input channel
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-ctx.Done()
		closeInputChan()
	}()

	// routine that offers user strings to input channel
	go func() {
		// note routine intentionally allowed to leak because
		// there is no way to make the reader context aware
		defer closeInputChan()

		sc := bufio.NewScanner(os.Stdin)
		sc.Split(bufio.ScanLines)

		for sc.Scan() {
			s := sc.Text()
			if s == "" {
				continue
			}

			// note it's possible for this channel
			// write to panic due to the user
			// doing things really fast and pressing control-c afterward
			//
			// but in that case we're still going through the stop procedure so meh

			inputChan <- s
		}
	}()

	// user input channel processing loop
	func() {
		for s := range inputChan {
			var cu loadtester.ConfigUpdate

			// note when calling SetNumWorkers() you likely also want to call SetNumIntervalTasks() to increase
			// the concurrent throughput for a given interval-segment of time for the change in parallelism SetNumWorkers provides
			//
			// most people will choose to keep these two values exactly the same because their goal is to
			// increase parallelism as well as concurrency and such a lock-step approach ensures that no
			// single outlier task affects the throughput of all the others in the same interval-segment of time.

			switch s {
			case "stop", "exit", "quit":
				return
			case "set workers":
				cu.SetNumWorkers(numWorkers)
			case "del worker", "remove worker":
				numWorkers -= 1

				cu.SetNumWorkers(numWorkers)
			case "add worker":
				numWorkers += 1

				cu.SetNumWorkers(numWorkers)
			}
			_ = lt.UpdateConfig(cu)
		}
	}()

	cancel()

	logger.LogAttrs(ctx, slog.LevelWarn, "waiting for all goroutines to finish")
	wg.Wait()
	logger.LogAttrs(ctx, slog.LevelWarn, "all goroutines finished")
}

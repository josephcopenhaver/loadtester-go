package main

import (
	"context"
	"time"

	"github.com/josephcopenhaver/loadtester-go/loadtester"
	"go.uber.org/zap/zapcore"
)

type task struct{}

func (t *task) Do(ctx context.Context, workerID int) error {
	return nil
}

type MyLoadtest struct{}

func (lt MyLoadtest) ReadTasks(p []loadtester.Doer) int {
	// make sure you only fill up to len

	var i int
	for i < len(p) {
		p[i] = &task{}
		i++
	}

	return i
}

func (lt MyLoadtest) UpdateConfigChan() <-chan loadtester.ConfigUpdate {
	return nil
}

func main() {
	logger, err := loadtester.NewLogger(zapcore.InfoLevel)
	if err != nil {
		panic(err)
	}

	ctx, cancel := loadtester.RootContext(logger)
	defer cancel()

	lt, err := loadtester.NewLoadtest(
		&MyLoadtest{},
		loadtester.Logger(logger),
		loadtester.NumWorkers(5),
		loadtester.NumIntervalTasks(25),
		loadtester.Interval(1*time.Second),
	)
	if err != nil {
		panic(err)
	}

	logger.Infow("running")
	defer func() {
		logger.Infow("stopped")
	}()
	if err := lt.Run(ctx); err != nil {
		logger.Panicw(
			"loadtest errored",
			"error", err,
		)
	}
}

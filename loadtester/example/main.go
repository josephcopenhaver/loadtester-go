package main

import (
	"context"
	"time"

	"github.com/josephcopenhaver/loadtester-go/loadtester"
)

type task struct{}

func (t *task) Do(ctx context.Context, workerID int) error {
	return nil
}

type MyLoadtest struct{}

func (lt MyLoadtest) NextTask() loadtester.Doer {
	return &task{}
}

func (lt MyLoadtest) UpdateChan() <-chan loadtester.ConfigUpdate {
	return nil
}

func main() {
	ctx, cancel := loadtester.RootContext()
	defer cancel()

	lt, err := loadtester.NewLoadtest(
		&MyLoadtest{},
		loadtester.NumWorkers(5),
		loadtester.NumIntervalTasks(25),
		loadtester.Interval(1*time.Second),
	)
	if err != nil {
		panic(err)
	}

	loadtester.Logger.Infow("running")
	defer func() {
		loadtester.Logger.Infow("stopped")
	}()
	if err := lt.Run(ctx); err != nil {
		loadtester.Logger.Panicw(
			"loadtest errored",
			"error", err,
		)
	}
}

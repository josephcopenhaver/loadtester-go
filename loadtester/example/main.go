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

func (lt MyLoadtest) ReadTasks(p []loadtester.Doer) int {
	// make sure you only fill up to len

	var i int
	for i < len(p) {
		p[i] = &task{}
		i++
	}

	return i
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

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

	lt := loadtester.NewLoadtest(
		&MyLoadtest{},
		loadtester.NumWorkers(1),
		loadtester.NumIntervalTasks(1),
		loadtester.Interval(1*time.Second),
	)
	loadtester.Logger.Infow("running")
	defer func() {
		loadtester.Logger.Infow("stopped")
	}()
	lt.Run(ctx)
}

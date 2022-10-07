package loadtester

import (
	"context"
	"time"
)

//
// interfaces
//

// Doer is a basic task unit
//
// If your Doer also implements Retryer
// then note that Doer can be run again in a different thread
// if your worker size is greater than one.
//
// If you want your task to have a retry upper bound then implement
// DoRetryChecker
type Doer interface {
	Do(ctx context.Context, workerID int) error
}

type Retryer interface {
	Retry(ctx context.Context, workerID int, prevErr error) error
}

// DoRetryer interface is useful for tasks that have no retry count upper bound
//
// If you need to have a retry upper bound, then have your task implement DoRetryChecker
type DoRetryer interface {
	Doer
	Retryer
}

type RetryChecker interface {
	CanRetry(ctx context.Context, workerID int, prevErr error) bool
}

type DoRetryChecker interface {
	DoRetryer
	RetryChecker
}

//
// structs
//

type retryTask struct {
	DoRetryer
	err error
}

func (rt *retryTask) Do(ctx context.Context, workerID int) error {
	return rt.DoRetryer.Retry(ctx, workerID, rt.err)
}

type taskMeta struct {
	IntervalID time.Time

	//
	// rate gauges:
	//

	NumIntervalTasks int
	Lag              time.Duration
}

type taskWithMeta struct {
	doer        Doer
	enqueueTime time.Time
	meta        taskMeta
}

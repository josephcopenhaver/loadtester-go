package loadtester

import (
	"context"
)

// Doer is a basic task unit
//
// If your Doer also implements Retryer
// then note that Doer can be run again in a different thread
// if your worker size is greater than one.
//
// If you want your task to have a retry upperbound then implement
// DoRetryChecker
type Doer interface {
	Do(ctx context.Context, workerID int) error
}

type Retryer interface {
	Retry(ctx context.Context, workerID int, prevErr error) error
}

// DoRetryer interface is useful for tasks that have no retry count upperbound
//
// If you need to have a retry upperbound, then have your task implement DoRetryChecker
type DoRetryer interface {
	Doer
	Retryer
}

type RetryChecker interface {
	CanRetry(ctx context.Context, workerID int, prevErr error) bool
}

// DoRetryChecker
type DoRetryChecker interface {
	DoRetryer
	RetryChecker
}

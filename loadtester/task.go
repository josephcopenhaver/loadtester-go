package loadtester

import (
	"context"
)

type Doer interface {
	Do(ctx context.Context, workerID int) error
}

type Retryer interface {
	Retry(ctx context.Context, workerID int, prevErr error) error
}

type DoRetryer interface {
	Doer
	Retryer
}

type DoRetryChecker interface {
	DoRetryer
	CanRetry(ctx context.Context, workerID int, prevErr error) bool
}

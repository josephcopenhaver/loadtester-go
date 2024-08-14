package loadtester

import (
	"context"
	"sync"
	"time"
)

type taskMetadataState struct {
	meta taskMeta

	enqueueTime, dequeueTime time.Time
}

type taskMetadata struct {
	taskMetadataState
}

func (tm *taskMetadata) EnqueueTime() time.Time {
	return tm.enqueueTime
}

func (tm *taskMetadata) DequeueTime() time.Time {
	return tm.dequeueTime
}

func (tm *taskMetadata) IntervalID() time.Time {
	return tm.meta.IntervalID
}

func (tm *taskMetadata) SampleSize() int {
	return tm.meta.SampleSize
}

func (tm *taskMetadata) NumIntervalTasks() int {
	return tm.meta.NumIntervalTasks
}

func (tm *taskMetadata) Lag() time.Duration {
	return tm.meta.Lag
}

var taskMetadataPool = sync.Pool{
	New: func() any {
		return &taskMetadata{}
	},
}

func newTaskMetadata() *taskMetadata {
	return taskMetadataPool.Get().(*taskMetadata)
}

func releaseTaskMetadata(tm *taskMetadata) {
	tm.taskMetadataState = taskMetadataState{}
	taskMetadataPool.Put(tm)
}

type taskMetadataCtxKey struct{}

func injectTaskMetadataProvider(ctx context.Context, tm *taskMetadata) context.Context {
	return context.WithValue(ctx, taskMetadataCtxKey{}, tm)
}

type taskMetadataProvider interface {
	IntervalID() time.Time
	EnqueueTime() time.Time
	DequeueTime() time.Time
	SampleSize() int
	NumIntervalTasks() int
	Lag() time.Duration
}

// TODO: move to unit test
var _ taskMetadataProvider = (*taskMetadata)(nil)

// GetTaskMetadataProvider returns a possibly nil value that implements:
//
// - `IntervalID() time.Time`
//
// - `EnqueueTime() time.Time`
//
// - `DequeueTime() time.Time`
//
// - `SampleSize() int`
//
// - `NumIntervalTasks() int`
//
// - `Lag() time.Duration`
func GetTaskMetadataProvider(ctx context.Context) *taskMetadata {
	return ctx.Value(taskMetadataCtxKey{}).(*taskMetadata)
}

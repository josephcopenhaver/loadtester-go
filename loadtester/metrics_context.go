package loadtester

import (
	"context"
	"sync"
	"time"
)

type metricsContextState struct {
	context.Context

	meta taskMeta

	enqueueTime time.Time
}

type metricsContext struct {
	metricsContextState
}

func (mc *metricsContext) EnqueueTime() time.Time {
	return mc.enqueueTime
}

func (mc *metricsContext) IntervalID() time.Time {
	return mc.meta.IntervalID
}

func (mc *metricsContext) SampleSize() int {
	return mc.meta.SampleSize
}

func (mc *metricsContext) NumIntervalTasks() int {
	return mc.meta.NumIntervalTasks
}

func (mc *metricsContext) Lag() time.Duration {
	return mc.meta.Lag
}

var metricContextPool = sync.Pool{
	New: func() any {
		return &metricsContext{}
	},
}

func newMetricsContext() *metricsContext {
	return metricContextPool.Get().(*metricsContext)
}

func releaseMetricsContext(mc *metricsContext) {
	mc.metricsContextState = metricsContextState{}
	metricContextPool.Put(mc)
}

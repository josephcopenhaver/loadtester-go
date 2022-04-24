package loadtester

import (
	"time"
)

type ConfigUpdate struct {
	numWorkers struct {
		set bool
		val int
	}
	numIntervalTasks struct {
		set bool
		val int
	}
	interval struct {
		set bool
		val time.Duration
	}
}

func (cu *ConfigUpdate) SetNumWorkers(n int) {
	cu.numWorkers.set = true
	cu.numWorkers.val = n
}

func (cu *ConfigUpdate) SetNumIntervalTasks(n int) {
	cu.numIntervalTasks.set = true
	cu.numIntervalTasks.val = n
}

func (cu *ConfigUpdate) SetInterval(d time.Duration) {
	cu.interval.set = true
	cu.interval.val = d
}

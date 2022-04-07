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

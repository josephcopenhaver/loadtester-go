package loadtester

import (
	"time"
)

type loadtestOptions struct {
	taskBufferingFactor     int
	maxTotalTasks           int
	maxWorkers              int
	numWorkers              int
	maxIntervalTasks        int
	numIntervalTasks        int
	interval                time.Duration
	csvOutputFilename       string
	csvOutputFlushFrequency time.Duration
	csvOutputDisabled       bool
}

type LoadtestOption func(*loadtestOptions)

func MaxTotalTasks(max int) LoadtestOption {
	return func(opt *loadtestOptions) {
		opt.maxTotalTasks = max
	}
}

func MaxWorkers(max int) LoadtestOption {
	return func(opt *loadtestOptions) {
		opt.maxWorkers = max
	}
}

func NumWorkers(n int) LoadtestOption {
	return func(opt *loadtestOptions) {
		opt.numWorkers = n
		if opt.maxWorkers < n {
			opt.maxWorkers = n
		}
	}
}

func TaskBufferingFactor(factor int) LoadtestOption {
	return func(opt *loadtestOptions) {
		opt.taskBufferingFactor = factor
	}
}

// MaxIntervalTasks could be removed as this is more of a hint
func MaxIntervalTasks(n int) LoadtestOption {
	return func(opt *loadtestOptions) {
		opt.maxIntervalTasks = n
	}
}

func NumIntervalTasks(n int) LoadtestOption {
	return func(opt *loadtestOptions) {
		opt.numIntervalTasks = n
		if opt.maxIntervalTasks < n {
			opt.maxIntervalTasks = n
		}
	}
}

func Interval(d time.Duration) LoadtestOption {
	return func(opt *loadtestOptions) {
		opt.interval = d
	}
}

func CsvFilename(s string) LoadtestOption {
	return func(opt *loadtestOptions) {
		opt.csvOutputFilename = s
	}
}

func CsvFlushFrequency(d time.Duration) LoadtestOption {
	return func(opt *loadtestOptions) {
		opt.csvOutputFlushFrequency = d
	}
}

func CsvWriterDisabled(b bool) LoadtestOption {
	return func(opt *loadtestOptions) {
		opt.csvOutputDisabled = b
	}
}

package loadtester

import (
	"time"
)

type loadtestOptions struct {
	taskBufferingFactor     int
	maxTotalTasks           int
	maxWorkers              int
	maxWorkersSet           bool
	numWorkers              int
	numWorkersSet           bool
	maxIntervalTasks        int
	maxIntervalTasksSet     bool
	numIntervalTasks        int
	numIntervalTasksSet     bool
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
		opt.maxWorkersSet = true
	}
}

func NumWorkers(n int) LoadtestOption {
	return func(opt *loadtestOptions) {
		opt.numWorkers = n
		opt.numWorkersSet = true
	}
}

func TaskBufferingFactor(factor int) LoadtestOption {
	return func(opt *loadtestOptions) {
		opt.taskBufferingFactor = factor
	}
}

func MaxIntervalTasks(n int) LoadtestOption {
	return func(opt *loadtestOptions) {
		opt.maxIntervalTasks = n
		opt.maxIntervalTasksSet = true
	}
}

func NumIntervalTasks(n int) LoadtestOption {
	return func(opt *loadtestOptions) {
		opt.numIntervalTasks = n
		opt.numIntervalTasksSet = true
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

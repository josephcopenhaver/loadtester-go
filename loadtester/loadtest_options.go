package loadtester

import (
	"time"
)

type loadtestConfig struct {
	outputBufferingFactor   int
	maxTasks                int
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
	flushRetriesTimeout     time.Duration
	flushRetriesOnShutdown  bool
	retriesDisabled         bool
	logger                  SugaredLogger
}

type LoadtestOption func(*loadtestConfig)

// MaxTasks sets an upperbound on the number of tasks the loadtest could perform
func MaxTasks(max int) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.maxTasks = max
	}
}

func MaxWorkers(max int) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.maxWorkers = max
		cfg.maxWorkersSet = true
	}
}

func NumWorkers(n int) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.numWorkers = n
		cfg.numWorkersSet = true
	}
}

func OutputBufferingFactor(factor int) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.outputBufferingFactor = factor
	}
}

func MaxIntervalTasks(n int) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.maxIntervalTasks = n
		cfg.maxIntervalTasksSet = true
	}
}

func NumIntervalTasks(n int) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.numIntervalTasks = n
		cfg.numIntervalTasksSet = true
	}
}

func Interval(d time.Duration) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.interval = d
	}
}

func CsvFilename(s string) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.csvOutputFilename = s
	}
}

func CsvFlushFrequency(d time.Duration) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.csvOutputFlushFrequency = d
	}
}

func CsvWriterDisabled(b bool) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.csvOutputDisabled = b
	}
}

// FlushRetriesOnShutdown is useful when your loadtest is more like a smoke test
// that must have all tasks flush and be succesful
func FlushRetriesOnShutdown(b bool) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.flushRetriesOnShutdown = b
	}
}

// FlushRetriesTimeout is only relevant when FlushRetriesOnShutdown(true) is used
func FlushRetriesTimeout(d time.Duration) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.flushRetriesTimeout = d
	}
}

// RetriesDisabled causes loadtester to ignore retry logic present on tasks
func RetriesDisabled(b bool) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.retriesDisabled = b
	}
}

func Logger(logger SugaredLogger) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.logger = logger
	}
}

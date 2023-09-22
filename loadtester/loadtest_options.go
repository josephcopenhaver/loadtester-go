package loadtester

import (
	"errors"
	"fmt"
	"log/slog"
	"time"
)

type loadtestConfig struct {
	outputBufferingFactor  int
	maxTasks               int
	maxWorkers             int
	maxWorkersSet          bool
	numWorkers             int
	numWorkersSet          bool
	maxIntervalTasks       int
	maxIntervalTasksSet    bool
	numIntervalTasks       int
	numIntervalTasksSet    bool
	interval               time.Duration
	csvOutputFilename      string
	csvOutputFlushInterval time.Duration
	csvOutputDisabled      bool
	flushRetriesTimeout    time.Duration
	flushRetriesOnShutdown bool
	retriesDisabled        bool
	logger                 StructuredLogger
}

func newLoadtestConfig(options ...LoadtestOption) (loadtestConfig, error) {
	var result loadtestConfig

	cfg := loadtestConfig{
		outputBufferingFactor:  4,
		maxWorkers:             1,
		numWorkers:             1,
		maxIntervalTasks:       1,
		numIntervalTasks:       1,
		interval:               time.Second,
		csvOutputFilename:      "metrics.csv",
		csvOutputFlushInterval: 5 * time.Second,
		flushRetriesTimeout:    2 * time.Minute,
	}

	for _, option := range options {
		option(&cfg)
	}

	if cfg.numWorkers < 0 {
		cfg.numWorkers = 0
	}

	if cfg.numIntervalTasks < 0 {
		cfg.numIntervalTasks = 0
	}

	if !cfg.maxWorkersSet && cfg.numWorkersSet {
		cfg.maxWorkers = cfg.numWorkers
	}

	if !cfg.maxIntervalTasksSet && cfg.numIntervalTasksSet {
		cfg.maxIntervalTasks = cfg.numIntervalTasks
	}

	if cfg.maxWorkers < cfg.numWorkers {
		return result, errors.New("loadtest misconfigured: MaxWorkers < NumWorkers")
	}

	if cfg.maxWorkers < 1 {
		return result, errors.New("loadtest misconfigured: MaxWorkers < 1")
	}

	if cfg.maxIntervalTasks < cfg.numIntervalTasks {
		return result, errors.New("loadtest misconfigured: MaxIntervalTasks < NumIntervalTasks")
	}

	if cfg.maxIntervalTasks < 1 {
		return result, errors.New("loadtest misconfigured: maxIntervalTasks < 1")
	}

	if cfg.outputBufferingFactor <= 0 {
		cfg.outputBufferingFactor = 1
	}

	if cfg.interval < 0 {
		return result, errors.New("loadtest misconfigured: interval < 0")
	}

	if cfg.csvOutputFlushInterval < 0 {
		return result, errors.New("loadtest misconfigured: csvOutputFlushInterval < 0")
	}

	if cfg.flushRetriesTimeout < 0 {
		return result, errors.New("loadtest misconfigured: flushRetriesTimeout < 0")
	}

	if cfg.logger == nil {
		logger, err := NewLogger(slog.LevelInfo)
		if err != nil {
			return result, fmt.Errorf("failed to create a default logger: %w", err)
		}
		cfg.logger = logger
	}

	result = cfg
	return result, nil
}

type LoadtestOption func(*loadtestConfig)

// MaxTasks sets an upper bound on the number of tasks the loadtest could perform
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

func MetricsCsvFilename(s string) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.csvOutputFilename = s
	}
}

func MetricsCsvFlushInterval(d time.Duration) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.csvOutputFlushInterval = d
	}
}

func MetricsCsvWriterDisabled(b bool) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.csvOutputDisabled = b
	}
}

// FlushRetriesOnShutdown is useful when your loadtest is more like a smoke test
// that must have all tasks flush and be successful
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

func Logger(logger StructuredLogger) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.logger = logger
	}
}

package loadtester

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"
)

const (
	// a value of 10000 yields a precision of two decimal places
	//
	// a value of 100000 would yield 3 decimal places
	percentDonePrecisionFactor = 10000
)

type loadtestConfig struct {
	taskReader             TaskReader
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
	csvOutputEnabled       bool
	flushRetriesTimeout    time.Duration
	flushRetriesOnShutdown bool
	retry                  bool
	logger                 StructuredLogger
	resultsChanSize        int
	percentilesEnabled     bool
	variancesEnabled       bool
	metaProviderEnabled    bool
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
		csvOutputEnabled:       true,
		csvOutputFilename:      "metrics.csv",
		csvOutputFlushInterval: 5 * time.Second,
		retry:                  true,
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

	// check for integer overflows from user input when computing metrics
	if cfg.csvOutputEnabled {
		const intervalPossibleLagResultCount = 1
		const intervalSampleSizeResultCount = 1
		// note: if intervalPossibleLagResultCount is ever adjusted, then the bellow if statement needs to change
		if cfg.maxIntervalTasks > (math.MaxInt - intervalPossibleLagResultCount - intervalSampleSizeResultCount) {
			return result, errors.New("MaxIntervalTasks value is too large")
		}

		maxIntervalResultCount := cfg.maxIntervalTasks + intervalPossibleLagResultCount + intervalSampleSizeResultCount
		if maxIntervalResultCount > (math.MaxInt / cfg.outputBufferingFactor) {
			return result, errors.New("MaxIntervalTasks and OutputBufferingFactor values combination is too large")
		}

		cfg.resultsChanSize = maxIntervalResultCount * cfg.outputBufferingFactor

		if cfg.maxTasks > 0 {
			if cfg.maxTasks > (math.MaxInt / percentDonePrecisionFactor) {
				return result, errors.New("MaxTasks value is too large")
			}
		}
	}

	if cfg.maxIntervalTasks > (math.MaxInt / 2) {
		return result, errors.New("MaxIntervalTasks value is too large")
	}

	if cfg.maxWorkers > (math.MaxInt / 2) {
		return result, errors.New("MaxWorkers value is too large")
	}

	if cfg.taskReader == nil {
		return result, errors.New("config value must not be nil: TaskReader")
	}

	result = cfg
	return result, nil
}

type LoadtestOption func(*loadtestConfig)

// LoadtestOptions should never be instantiated manually
//
// Instead call NewOpts()
//
// This is only exported to allow godocs to discover the exported methods.
//
// LoadtestOptions will never have exported data members, just
// stateless exported functions.
type LoadtestOptions struct{}

func NewOpts() LoadtestOptions {
	return LoadtestOptions{}
}

func (LoadtestOptions) TaskReader(taskReader TaskReader) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.taskReader = taskReader
	}
}

// MaxTasks sets an upper bound on the number of tasks the loadtest could perform
func (LoadtestOptions) MaxTasks(max int) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.maxTasks = max
	}
}

func (LoadtestOptions) MaxWorkers(max int) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.maxWorkers = max
		cfg.maxWorkersSet = true
	}
}

func (LoadtestOptions) NumWorkers(n int) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.numWorkers = n
		cfg.numWorkersSet = true
	}
}

func (LoadtestOptions) OutputBufferingFactor(factor int) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.outputBufferingFactor = factor
	}
}

func (LoadtestOptions) MaxIntervalTasks(n int) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.maxIntervalTasks = n
		cfg.maxIntervalTasksSet = true
	}
}

func (LoadtestOptions) NumIntervalTasks(n int) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.numIntervalTasks = n
		cfg.numIntervalTasksSet = true
	}
}

func (LoadtestOptions) Interval(d time.Duration) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.interval = d
	}
}

func (LoadtestOptions) MetricsCsvFilename(s string) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.csvOutputFilename = s
	}
}

func (LoadtestOptions) MetricsCsvFlushInterval(d time.Duration) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.csvOutputFlushInterval = d
	}
}

func (LoadtestOptions) MetricsCsv(b bool) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.csvOutputEnabled = b
	}
}

// MetricsLatencyPercentile can greatly increase the amount of memory used
// and create additional delay while processing results.
//
// Make sure MaxIntervalTasks is either not set or if it must be set make
// sure it is not too large for the hosts's ram availability.
func (LoadtestOptions) MetricsLatencyPercentile(b bool) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.percentilesEnabled = b
	}
}

// MetricsLatencyVariance can create additional delay while processing results.
func (LoadtestOptions) MetricsLatencyVariance(b bool) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.variancesEnabled = b
	}
}

// FlushRetriesOnShutdown is useful when your loadtest is more like a smoke test
// that must have all tasks flush and be successful
func (LoadtestOptions) FlushRetriesOnShutdown(b bool) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.flushRetriesOnShutdown = b
	}
}

// FlushRetriesTimeout is only relevant when FlushRetriesOnShutdown(true) is used
func (LoadtestOptions) FlushRetriesTimeout(d time.Duration) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.flushRetriesTimeout = d
	}
}

// Retry sets wether the loadtester has retry support on or off.
//
// on == true (default)
func (LoadtestOptions) Retry(b bool) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.retry = b
	}
}

// MetadataProviderEnabled sets wether the context provided to a task's Do, CanRetry, and Retry methods
// returns a non-nil value when passed to GetTaskMetadataProvider(...)
//
// Use GetTaskMetadataProvider to instrument result reporting as you may see fit with far more granularity than the high level metrics.csv.
//
// off == false (default)
func (LoadtestOptions) MetadataProviderEnabled(b bool) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.metaProviderEnabled = b
	}
}

func (LoadtestOptions) Logger(logger StructuredLogger) LoadtestOption {
	return func(cfg *loadtestConfig) {
		cfg.logger = logger
	}
}

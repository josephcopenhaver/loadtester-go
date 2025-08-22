package loadtester

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

const (
	skipInterTaskSchedulingThreshold = 20 * time.Millisecond
)

var (
	ErrBadReadTasksImpl     = errors.New("bad ReadTasks implementation: returned a value less than zero or larger than the input slice length")
	ErrRetriesFailedToFlush = errors.New("failed to flush all retries")
	errLoadtestContextDone  = errors.New("loadtest parent context errored")
)

// TaskReader describes how to read tasks into a loadtest
type TaskReader interface {
	// ReadTasks fills the provided slice up to slice length starting at index 0 and returns how many records have been inserted
	//
	// Failing to fill the whole slice will signal the end of the loadtest.
	//
	// Note in general you should not use this behavior to signal loadtests to stop
	// if your loadtest needs to be time-bound. For that case you should signal a stop
	// via the context. This stop on failure to fill behavior only exists for cases
	// where the author wants to exhaustively run a set of tasks and not bound the
	// loadtest to a timespan but rather completeness of the tasks space.
	//
	// Note that if you have only partially filled the slice, those filled task slots
	// will still be run before termination of the loadtest.
	//
	// It is important to remember that if your loadtest loops around and you keep a large slice of
	// tasks in memory just to place small chunks of that list into the passed in slice of this function
	// that you could have a task executed by two separate goroutines at the same time under the right circumstances.
	// Therefore, it's really important that the tasks either be stateless or concrete copies of the original task object are
	// created when saved to the passed in slice of this function.
	ReadTasks([]Doer) int
}

type taskResultFlags struct {
	Passed      uint8
	Panicked    uint8
	RetryQueued uint8
	Errored     uint8
}

func (trf taskResultFlags) isZero() bool {
	return trf == taskResultFlags{}
}

type taskResult struct {
	taskResultFlags
	QueueDuration, TaskDuration time.Duration
	Meta                        taskMeta
}

type Loadtest struct {
	taskReader TaskReader
	maxTasks   int
	maxWorkers int
	numWorkers int
	// maxLiveSamples is the max buffer size of sample sizes that exist at any possible point in time
	maxLiveSamples  int
	pauseChans      []chan struct{}
	workerWaitGroup sync.WaitGroup
	resultWaitGroup sync.WaitGroup
	taskChan        chan taskWithMeta
	resultsChan     chan taskResult
	cfgUpdateChan   chan ConfigUpdate

	// intervalTasksSema will always have capacity set to the task interval rate + worker count
	// but it is created with the maximum weight to allow up to max task interval rate + max
	// worker count configurations ( accounts for work in progress and work queued )
	//
	// This addition prevents enqueuing more than beyond the desired tasks per interval
	// and creates negative pressure on the task enqueue routine since the max channel sizes
	// are static and bound to the maximums we could reach in some pacer implementation.
	//
	// Meaning we could send far more than we intended for the wall-clock interval and it
	// could cause severe bursts in load without this protection.
	//
	// As a result this does slow down each task runner, but only slightly as it's just acquiring
	// a lock, doing some simple math, and then unlocking as the task worker calls release on
	// the semaphore. It's worth it to me.
	intervalTasksSema *semaphore.Weighted

	numIntervalTasks int
	maxIntervalTasks int
	interval         time.Duration

	retryTaskChan chan *retryTask
	retryTaskPool sync.Pool

	startTime time.Time
	csvData   csvData

	flushRetriesTimeout    time.Duration
	logger                 StructuredLogger
	run                    func(context.Context, *error) error
	doTask                 func(context.Context, int, taskWithMeta)
	resultsHandler         func()
	latencies              latencyLists
	flushRetriesOnShutdown bool
	retry                  bool
	metricsEnabled         bool
	percentilesEnabled     bool
	variancesEnabled       bool
	metaProviderEnabled    bool
}

func NewLoadtest(options ...LoadtestOption) (*Loadtest, error) {

	cfg, err := newLoadtestConfig(options...)
	if err != nil {
		return nil, err
	}

	// lag results are reported per interval through the same channel as results
	// so it's important to account for them per interval when constructing max
	// config buffers

	var resultsChan chan taskResult
	var maxLiveSamples int
	if cfg.csvOutputEnabled {
		resultsChan = make(chan taskResult, cfg.resultsChanSize)
		maxLiveSamples = cfg.outputBufferingFactor
	}

	var retryTaskChan chan *retryTask
	var newRetryTask func() any
	var sm *semaphore.Weighted
	{
		maxNumInProgressOrQueuedTasks := maxPendingTasks(cfg.maxWorkers, cfg.maxIntervalTasks)
		sm = semaphore.NewWeighted(int64(maxNumInProgressOrQueuedTasks))
		if !sm.TryAcquire(int64(maxNumInProgressOrQueuedTasks)) {
			return nil, errors.New("failed to initialize load generation semaphore")
		}

		if cfg.retry {
			retryTaskChan = make(chan *retryTask, maxNumInProgressOrQueuedTasks)
			newRetryTask = func() any {
				return &retryTask{}
			}
		}
	}

	var latencies latencyLists
	if cfg.percentilesEnabled {
		latencies = newLatencyLists(cfg.maxIntervalTasks)
	}

	pauseChans := make([]chan struct{}, cfg.maxWorkers)
	for i := 0; i < cfg.maxWorkers; i++ {
		pauseChans[i] = make(chan struct{}, 2)
	}

	lt := &Loadtest{
		taskReader:     cfg.taskReader,
		maxTasks:       cfg.maxTasks,
		maxWorkers:     cfg.maxWorkers,
		numWorkers:     cfg.numWorkers,
		maxLiveSamples: maxLiveSamples,
		pauseChans:     pauseChans,
		taskChan:       make(chan taskWithMeta, cfg.maxIntervalTasks),
		resultsChan:    resultsChan,
		cfgUpdateChan:  make(chan ConfigUpdate),
		retryTaskChan:  retryTaskChan,

		maxIntervalTasks: cfg.maxIntervalTasks,
		numIntervalTasks: cfg.numIntervalTasks,
		interval:         cfg.interval,
		retryTaskPool: sync.Pool{
			New: newRetryTask,
		},

		csvData: csvData{
			outputFilename: cfg.csvOutputFilename,
			flushInterval:  cfg.csvOutputFlushInterval,
		},

		flushRetriesTimeout:    cfg.flushRetriesTimeout,
		flushRetriesOnShutdown: cfg.flushRetriesOnShutdown,
		retry:                  cfg.retry,
		logger:                 cfg.logger,
		intervalTasksSema:      sm,
		metricsEnabled:         cfg.csvOutputEnabled,
		percentilesEnabled:     cfg.percentilesEnabled,
		latencies:              latencies,
		variancesEnabled:       cfg.variancesEnabled,
		metaProviderEnabled:    cfg.metaProviderEnabled,
	}

	if cfg.retry {
		if cfg.csvOutputEnabled {
			if cfg.metaProviderEnabled {
				lt.doTask = lt.doTask_retriesEnabled_metricsEnabled_taskMetadataProviderEnabled
			} else {

				lt.doTask = lt.doTask_retriesEnabled_metricsEnabled_taskMetadataProviderDisabled
			}
		} else {
			if cfg.metaProviderEnabled {
				lt.doTask = lt.doTask_retriesEnabled_metricsDisabled_taskMetadataProviderEnabled
			} else {

				lt.doTask = lt.doTask_retriesEnabled_metricsDisabled_taskMetadataProviderDisabled
			}
		}
		if cfg.maxTasks > 0 {
			if cfg.csvOutputEnabled {
				lt.run = lt.run_retriesEnabled_maxTasksGTZero_metricsEnabled
			} else {
				lt.run = lt.run_retriesEnabled_maxTasksGTZero_metricsDisabled
			}
		} else if cfg.csvOutputEnabled {
			lt.run = lt.run_retriesEnabled_maxTasksNotGTZero_metricsEnabled
		} else {
			lt.run = lt.run_retriesEnabled_maxTasksNotGTZero_metricsDisabled
		}
	} else {
		if cfg.csvOutputEnabled {
			if cfg.metaProviderEnabled {
				lt.doTask = lt.doTask_retriesDisabled_metricsEnabled_taskMetadataProviderEnabled
			} else {

				lt.doTask = lt.doTask_retriesDisabled_metricsEnabled_taskMetadataProviderDisabled
			}
		} else {
			if cfg.metaProviderEnabled {
				lt.doTask = lt.doTask_retriesDisabled_metricsDisabled_taskMetadataProviderEnabled
			} else {

				lt.doTask = lt.doTask_retriesDisabled_metricsDisabled_taskMetadataProviderDisabled
			}
		}
		if cfg.maxTasks > 0 {
			if cfg.csvOutputEnabled {
				lt.run = lt.run_retriesDisabled_maxTasksGTZero_metricsEnabled
			} else {
				lt.run = lt.run_retriesDisabled_maxTasksGTZero_metricsDisabled
			}
		} else if cfg.csvOutputEnabled {
			lt.run = lt.run_retriesDisabled_maxTasksNotGTZero_metricsEnabled
		} else {
			lt.run = lt.run_retriesDisabled_maxTasksNotGTZero_metricsDisabled
		}
	}

	if cfg.maxTasks > 0 {
		if cfg.percentilesEnabled {
			if cfg.variancesEnabled {
				if cfg.retry {
					lt.resultsHandler = lt.resultsHandler_retryEnabled_maxTasksGTZero_percentileEnabled_varianceEnabled()
				} else {
					lt.resultsHandler = lt.resultsHandler_retryDisabled_maxTasksGTZero_percentileEnabled_varianceEnabled()
				}
			} else {
				if cfg.retry {
					lt.resultsHandler = lt.resultsHandler_retryEnabled_maxTasksGTZero_percentileEnabled_varianceDisabled()
				} else {
					lt.resultsHandler = lt.resultsHandler_retryDisabled_maxTasksGTZero_percentileEnabled_varianceDisabled()
				}
			}
		} else {
			if cfg.variancesEnabled {
				if cfg.retry {
					lt.resultsHandler = lt.resultsHandler_retryEnabled_maxTasksGTZero_percentileDisabled_varianceEnabled()
				} else {
					lt.resultsHandler = lt.resultsHandler_retryDisabled_maxTasksGTZero_percentileDisabled_varianceEnabled()
				}
			} else {
				if cfg.retry {
					lt.resultsHandler = lt.resultsHandler_retryEnabled_maxTasksGTZero_percentileDisabled_varianceDisabled()
				} else {
					lt.resultsHandler = lt.resultsHandler_retryDisabled_maxTasksGTZero_percentileDisabled_varianceDisabled()
				}
			}
		}
	} else {
		if cfg.percentilesEnabled {
			if cfg.variancesEnabled {
				if cfg.retry {
					lt.resultsHandler = lt.resultsHandler_retryEnabled_maxTasksNotGTZero_percentileEnabled_varianceEnabled()
				} else {
					lt.resultsHandler = lt.resultsHandler_retryDisabled_maxTasksNotGTZero_percentileEnabled_varianceEnabled()
				}
			} else {
				if cfg.retry {
					lt.resultsHandler = lt.resultsHandler_retryEnabled_maxTasksNotGTZero_percentileEnabled_varianceDisabled()
				} else {
					lt.resultsHandler = lt.resultsHandler_retryDisabled_maxTasksNotGTZero_percentileEnabled_varianceDisabled()
				}
			}
		} else {
			if cfg.variancesEnabled {
				if cfg.retry {
					lt.resultsHandler = lt.resultsHandler_retryEnabled_maxTasksNotGTZero_percentileDisabled_varianceEnabled()
				} else {
					lt.resultsHandler = lt.resultsHandler_retryDisabled_maxTasksNotGTZero_percentileDisabled_varianceEnabled()
				}
			} else {
				if cfg.retry {
					lt.resultsHandler = lt.resultsHandler_retryEnabled_maxTasksNotGTZero_percentileDisabled_varianceDisabled()
				} else {
					lt.resultsHandler = lt.resultsHandler_retryDisabled_maxTasksNotGTZero_percentileDisabled_varianceDisabled()
				}
			}
		}
	}

	return lt, nil
}

// UpdateConfig only returns false if the loadtest's run method has exited
//
// This call can potentially block the caller on loadtest shutdown phase.
func (lt *Loadtest) UpdateConfig(cu ConfigUpdate) (handled bool) {
	defer func() {
		if r := recover(); r != nil {
			handled = false
		}
	}()

	lt.cfgUpdateChan <- cu

	return true
}

func (lt *Loadtest) addWorker(ctx context.Context, workerID int) {
	lt.workerWaitGroup.Go(func() {
		lt.workerLoop(ctx, workerID)
	})
}

func (lt *Loadtest) loadtestConfigAsJson() any {
	type Config struct {
		StartTime              string `json:"start_time"`
		Interval               string `json:"interval"`
		MaxIntervalTasks       int    `json:"max_interval_tasks"`
		MaxTasks               int    `json:"max_tasks"`
		MaxWorkers             int    `json:"max_workers"`
		NumIntervalTasks       int    `json:"num_interval_tasks"`
		NumWorkers             int    `json:"num_workers"`
		MetricsEnabled         bool   `json:"metrics_enabled"`
		MetricsFlushInterval   string `json:"metrics_flush_interval"`
		FlushRetriesOnShutdown bool   `json:"flush_retries_on_shutdown"`
		FlushRetriesTimeout    string `json:"flush_retries_timeout"`
		Retry                  bool   `json:"retry_enabled"`
		PercentilesEnabled     bool   `json:"percentiles_enabled"`
		VariancesEnabled       bool   `json:"variances_enabled"`
		MetaProviderEnabled    bool   `json:"metadata_provider_enabled"`
	}

	return Config{
		StartTime:              timeToString(lt.startTime),
		Interval:               lt.interval.String(),
		MaxIntervalTasks:       lt.maxIntervalTasks,
		MaxTasks:               lt.maxTasks,
		MaxWorkers:             lt.maxWorkers,
		NumIntervalTasks:       lt.numIntervalTasks,
		NumWorkers:             lt.numWorkers,
		MetricsEnabled:         lt.metricsEnabled,
		MetricsFlushInterval:   lt.csvData.flushInterval.String(),
		FlushRetriesOnShutdown: lt.flushRetriesOnShutdown,
		FlushRetriesTimeout:    lt.flushRetriesTimeout.String(),
		Retry:                  lt.retry,
		PercentilesEnabled:     lt.percentilesEnabled,
		VariancesEnabled:       lt.variancesEnabled,
		MetaProviderEnabled:    lt.metaProviderEnabled,
	}
}

func (lt *Loadtest) workerLoop(ctx context.Context, workerID int) {
	var task taskWithMeta
	pauseChan := (<-chan struct{})(lt.pauseChans[workerID])

	for {
		// duplicating short-circuit signal control processing to give it priority over the randomizing nature of the multi-select
		// that follows
		//
		// ref: https://go.dev/ref/spec#Select_statements
		select {
		case _, ok := <-pauseChan:
			if !ok {
				// all work is done
				return
			}

			// paused
			_, ok = <-pauseChan
			if !ok {
				// all work is done
				return
			}

			continue
		default:
		}
		select {
		case _, ok := <-pauseChan:
			if !ok {
				// all work is done
				return
			}

			// paused
			_, ok = <-pauseChan
			if !ok {
				// all work is done
				return
			}

			continue
		case task = <-lt.taskChan:
		}

		lt.doTask(ctx, workerID, task)

		lt.intervalTasksSema.Release(1)
	}
}

// Run can only be called once per loadtest instance, it is stateful
func (lt *Loadtest) Run(ctx context.Context) (err_result error) {

	// all that should ever be present in this function is logic to aggregate async errors
	// into one error response
	//
	// and then a call to the internal lt.run(ctx, &cleanupErr) func

	// this defer ensures that any async csv writing error bubbles up to the err_result
	// if it would be nil
	var shutdownErr error
	defer func() {
		if err_result != nil {
			return
		}

		if shutdownErr != nil {
			err_result = shutdownErr
			return
		}

		if err := lt.csvData.writeErr; err != nil {
			err_result = err
		}
	}()

	return lt.run(ctx, &shutdownErr)
}

type latencyLists struct {
	queue latencyList
	task  latencyList
}

func newLatencyLists(size int) latencyLists {
	return latencyLists{
		latencyList{make([]time.Duration, 0, size)},
		latencyList{make([]time.Duration, 0, size)},
	}
}

//
// helpers
//

func timeToString(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func maxPendingTasks(numWorkers, numIntervalTasks int) int {
	// if the number of workers exceeds the number
	// of tasks then ideally we'll always have a
	// few idle workers
	//
	// in such a scenario we don't want to treat work
	// in progress larger than the number of new tasks
	// as expected work in progress, instead we should
	// have only up to the task count of work in progress
	// and ideally that work should be flushing to the
	// results consumer routine
	//
	// so to create back pressure we ignore the number
	// of workers that exceed the task count for the
	// interval
	//
	// having idle workers is valid in cases where tasks
	// are of a mixed type configuration and some take
	// longer than others - it's just another type of
	// throughput buffering authors can opt into using
	// for edge case reasons/simulations
	if numWorkers > numIntervalTasks {
		return numIntervalTasks * 2
	}

	return numIntervalTasks + numWorkers
}

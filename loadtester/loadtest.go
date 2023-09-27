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
	QueuedDuration, TaskDuration time.Duration
	Meta                         taskMeta
}

type Loadtest struct {
	taskReader      TaskReader
	maxTasks        int
	maxWorkers      int
	numWorkers      int
	workers         []chan struct{}
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
	writeOutputCsvRow      func(metricRecord)
	flushRetriesOnShutdown bool
	retriesDisabled        bool
	metricsEnabled         bool
}

func NewLoadtest(taskReader TaskReader, options ...LoadtestOption) (*Loadtest, error) {

	cfg, err := newLoadtestConfig(options...)
	if err != nil {
		return nil, err
	}

	// lag results are reported per interval through the same channel as results
	// so it's important to account for them per interval when constructing max
	// config buffers

	const intervalPossibleLagResultCount = 1
	resultsChanSize := (cfg.maxIntervalTasks + intervalPossibleLagResultCount) * cfg.outputBufferingFactor

	var resultsChan chan taskResult
	if !cfg.csvOutputDisabled {
		resultsChan = make(chan taskResult, resultsChanSize)
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

		if !cfg.retriesDisabled {
			retryTaskChan = make(chan *retryTask, maxNumInProgressOrQueuedTasks)
			newRetryTask = func() any {
				return &retryTask{}
			}
		}
	}

	lt := &Loadtest{
		taskReader:    taskReader,
		maxTasks:      cfg.maxTasks,
		maxWorkers:    cfg.maxWorkers,
		numWorkers:    cfg.numWorkers,
		workers:       make([]chan struct{}, 0, cfg.maxWorkers),
		taskChan:      make(chan taskWithMeta, cfg.maxIntervalTasks),
		resultsChan:   resultsChan,
		cfgUpdateChan: make(chan ConfigUpdate),
		retryTaskChan: retryTaskChan,

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
		retriesDisabled:        cfg.retriesDisabled,
		logger:                 cfg.logger,
		intervalTasksSema:      sm,
		metricsEnabled:         !cfg.csvOutputDisabled,
	}

	if cfg.maxTasks > 0 {
		lt.writeOutputCsvRow = lt.writeOutputCsvRow_maxTasksGTZero
	} else {
		lt.writeOutputCsvRow = lt.writeOutputCsvRow_maxTasksNotGTZero
	}

	if !cfg.retriesDisabled {
		if !cfg.csvOutputDisabled {
			lt.doTask = lt.doTask_retriesEnabled_metricsEnabled
		} else {
			lt.doTask = lt.doTask_retriesEnabled_metricsDisabled
		}
		if cfg.maxTasks > 0 {
			if !cfg.csvOutputDisabled {
				lt.run = lt.run_retriesEnabled_maxTasksGTZero_metricsEnabled
			} else {
				lt.run = lt.run_retriesEnabled_maxTasksGTZero_metricsDisabled
			}
		} else if !cfg.csvOutputDisabled {
			lt.run = lt.run_retriesEnabled_maxTasksNotGTZero_metricsEnabled
		} else {
			lt.run = lt.run_retriesEnabled_maxTasksNotGTZero_metricsDisabled
		}
	} else {
		if !cfg.csvOutputDisabled {
			lt.doTask = lt.doTask_retriesDisabled_metricsEnabled
		} else {
			lt.doTask = lt.doTask_retriesDisabled_metricsDisabled
		}
		if cfg.maxTasks > 0 {
			if !cfg.csvOutputDisabled {
				lt.run = lt.run_retriesDisabled_maxTasksGTZero_metricsEnabled
			} else {
				lt.run = lt.run_retriesDisabled_maxTasksGTZero_metricsDisabled
			}
		} else if !cfg.csvOutputDisabled {
			lt.run = lt.run_retriesDisabled_maxTasksNotGTZero_metricsEnabled
		} else {
			lt.run = lt.run_retriesDisabled_maxTasksNotGTZero_metricsDisabled
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
	pauseChan := make(chan struct{}, 2)
	lt.workerWaitGroup.Add(1)
	go func() {
		defer lt.workerWaitGroup.Done()

		lt.workerLoop(ctx, workerID, pauseChan)
	}()
	lt.workers = append(lt.workers, pauseChan)
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
		RetriesDisabled        bool   `json:"retries_disabled"`
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
		RetriesDisabled:        lt.retriesDisabled,
	}
}

func (lt *Loadtest) workerLoop(ctx context.Context, workerID int, pauseChan <-chan struct{}) {
	for {
		var task taskWithMeta

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

//
// helpers
//

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

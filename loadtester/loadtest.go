package loadtester

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
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

// TODO: RetriesDisabled runtime checks can turn into init time checks; same with MaxTasks based checks
// I would not dream of doing this before proving it is warranted first.

// TaskProvider describes how to read tasks into a
// loadtest and how to control a loadtest's configuration
// over time
type TaskProvider interface {
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
	// UpdateConfigChan should return the same channel each time or nil;
	// but once nil it must never be non-nil again
	UpdateConfigChan() <-chan ConfigUpdate
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
	taskProvider    TaskProvider
	maxTasks        int
	maxWorkers      int
	numWorkers      int
	workers         []chan struct{}
	workerWaitGroup sync.WaitGroup
	resultWaitGroup sync.WaitGroup
	taskChan        chan taskWithMeta
	resultsChan     chan taskResult

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

	flushRetriesOnShutdown bool
	flushRetriesTimeout    time.Duration
	retriesDisabled        bool

	logger SugaredLogger
}

func NewLoadtest(taskProvider TaskProvider, options ...LoadtestOption) (*Loadtest, error) {

	cfg, err := newLoadtestConfig(options...)
	if err != nil {
		return nil, err
	}

	// lag results are reported per interval through the same channel as results
	// so it's important to account for them per interval when constructing max
	// config buffers

	const intervalPossibleLagResultCount = 1
	resultsChanSize := (cfg.maxIntervalTasks + intervalPossibleLagResultCount) * cfg.outputBufferingFactor

	var csvWriteErr error
	if cfg.csvOutputDisabled {
		csvWriteErr = errCsvWriterDisabled
	}

	var retryTaskChan chan *retryTask
	var sm *semaphore.Weighted
	{
		maxNumInProgressOrQueuedTasks := maxPendingTasks(cfg.maxWorkers, cfg.maxIntervalTasks)
		sm = semaphore.NewWeighted(int64(maxNumInProgressOrQueuedTasks))
		if !sm.TryAcquire(int64(maxNumInProgressOrQueuedTasks)) {
			return nil, errors.New("failed to initialize load generation semaphore")
		}

		if !cfg.retriesDisabled {
			retryTaskChan = make(chan *retryTask, maxNumInProgressOrQueuedTasks)
		}
	}

	return &Loadtest{
		taskProvider:  taskProvider,
		maxTasks:      cfg.maxTasks,
		maxWorkers:    cfg.maxWorkers,
		numWorkers:    cfg.numWorkers,
		workers:       make([]chan struct{}, 0, cfg.maxWorkers),
		taskChan:      make(chan taskWithMeta, cfg.maxIntervalTasks),
		resultsChan:   make(chan taskResult, resultsChanSize),
		retryTaskChan: retryTaskChan,

		maxIntervalTasks: cfg.maxIntervalTasks,
		numIntervalTasks: cfg.numIntervalTasks,
		interval:         cfg.interval,
		retryTaskPool: sync.Pool{
			New: func() interface{} {
				return &retryTask{}
			},
		},

		csvData: csvData{
			outputFilename: cfg.csvOutputFilename,
			flushInterval:  cfg.csvOutputFlushInterval,
			writeErr:       csvWriteErr,
		},

		flushRetriesTimeout:    cfg.flushRetriesTimeout,
		flushRetriesOnShutdown: cfg.flushRetriesOnShutdown,
		retriesDisabled:        cfg.retriesDisabled,
		logger:                 cfg.logger,
		intervalTasksSema:      sm,
	}, nil
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

		taskStart := time.Now()
		respFlags := lt.doTask(ctx, workerID, task.doer)
		taskEnd := time.Now()

		lt.resultsChan <- taskResult{
			taskResultFlags: respFlags,
			QueuedDuration:  taskStart.Sub(task.enqueueTime),
			TaskDuration:    taskEnd.Sub(taskStart),
			Meta:            task.meta,
		}

		lt.intervalTasksSema.Release(1)
	}
}

func (lt *Loadtest) doTask(ctx context.Context, workerID int, task Doer) (result taskResultFlags) {
	var err_resp error

	// phase is the name of the step which has possibly caused a panic
	phase := "do"
	var rt *retryTask
	if !lt.retriesDisabled {
		if v, ok := task.(*retryTask); ok {
			rt = v
			phase = "retry"
			defer func() {
				*rt = retryTask{}
				lt.retryTaskPool.Put(v)
			}()
		}
	}
	defer func() {

		if err_resp != nil {
			lt.logger.Warnw(
				"task error",
				"worker_id", workerID,
				"error", err_resp,
			)
		}

		if r := recover(); r != nil {
			result.Panicked = 1
			result.Errored = 1

			switch v := r.(type) {
			case error:
				lt.logger.Errorw(
					"worker recovered from panic",
					"worker_id", workerID,
					"phase", phase,
					"error", v,
				)
			case []byte:
				lt.logger.Errorw(
					"worker recovered from panic",
					"worker_id", workerID,
					"phase", phase,
					"error", string(v),
				)
			case string:
				lt.logger.Errorw(
					"worker recovered from panic",
					"worker_id", workerID,
					"phase", phase,
					"error", v,
				)
			default:
				const msg = "unknown cause"

				lt.logger.Errorw(
					"worker recovered from panic",
					"worker_id", workerID,
					"phase", phase,
					"error", msg,
				)
			}
		}
	}()
	err := task.Do(ctx, workerID)
	phase = "" // done, no panic occurred
	if err == nil {
		result.Passed = 1
		return
	}

	err_resp = err
	result.Errored = 1

	if lt.retriesDisabled {
		return
	}

	var dr DoRetryer
	if rt != nil {
		dr = rt.DoRetryer
	} else if v, ok := task.(DoRetryer); ok {
		dr = v
	} else {
		return
	}

	phase = "can-retry"
	if v, ok := dr.(DoRetryChecker); ok && !v.CanRetry(ctx, workerID, err) {
		phase = "" // done, no panic occurred
		return
	}
	phase = "" // done, no panic occurred

	lt.retryTaskChan <- lt.newRetryTask(dr, err)

	result.RetryQueued = 1

	return
}

func (lt *Loadtest) newRetryTask(task DoRetryer, err error) *retryTask {
	result := lt.retryTaskPool.Get().(*retryTask)

	*result = retryTask{task, err}

	return result
}

func (lt *Loadtest) readRetries(p []Doer) int {
	// make sure you only fill up to len

	var i int
	for i < len(p) {
		select {
		case task := <-lt.retryTaskChan:
			p[i] = task
		default:
			return i
		}
		i++
	}

	return i
}

func (lt *Loadtest) getLoadtestConfigAsJson() interface{} {
	type Config struct {
		StartTime              string `json:"start_time"`
		Interval               string `json:"interval"`
		MaxIntervalTasks       int    `json:"max_interval_tasks"`
		MaxTasks               int    `json:"max_tasks"`
		MaxWorkers             int    `json:"max_workers"`
		NumIntervalTasks       int    `json:"num_interval_tasks"`
		NumWorkers             int    `json:"num_workers"`
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
		MetricsFlushInterval:   lt.csvData.flushInterval.String(),
		FlushRetriesOnShutdown: lt.flushRetriesOnShutdown,
		FlushRetriesTimeout:    lt.flushRetriesTimeout.String(),
		RetriesDisabled:        lt.retriesDisabled,
	}
}

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

		if err := lt.csvData.writeErr; err != nil && err != errCsvWriterDisabled {
			err_result = err
		}
	}()

	return lt.run(ctx, &shutdownErr)
}

func (lt *Loadtest) run(ctx context.Context, shutdownErrResp *error) error {

	lt.startTime = time.Now()

	cd := &lt.csvData

	if cd.writeErr == nil {

		csvFile, err := os.Create(cd.outputFilename)
		if err != nil {
			return fmt.Errorf("failed to open output csv metrics file for writing: %w", err)
		}
		defer lt.writeOutputCsvFooterAndClose(csvFile)

		cd.writeErr = lt.writeOutputCsvConfigComment(csvFile)

		if cd.writeErr == nil {

			cd.writer = csv.NewWriter(csvFile)

			cd.writeErr = lt.writeOutputCsvHeaders()
		}
	}

	lt.logger.Infow(
		"starting loadtest",
		"config", lt.getLoadtestConfigAsJson(),
	)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		lt.resultsHandler()
	}()

	numWorkers := lt.numWorkers

	// numTasks is the total number of tasks
	// scheduled to run ( including retries )
	var numTasks int

	intervalID := time.Now()

	maxTasks := lt.maxTasks

	interval := lt.interval
	numNewTasks := lt.numIntervalTasks
	ctxDone := ctx.Done()
	updateChan := lt.taskProvider.UpdateConfigChan()
	configChanges := make([]interface{}, 0, 12)
	meta := taskMeta{
		NumIntervalTasks: lt.numIntervalTasks,
	}
	var interTaskInterval time.Duration
	if meta.NumIntervalTasks > 0 {
		interTaskInterval = interval / time.Duration(meta.NumIntervalTasks)
	}

	taskBuf := make([]Doer, 0, lt.maxIntervalTasks)

	var delay time.Duration

	// stopping routine runs on return
	// flushing as much as possible
	defer func() {

		err := func(flushRetries bool) error {
			if !flushRetries {

				lt.logger.Debugw(
					"not waiting on retries to flush on shutdown",
					"reason", "retries disabled or flush retries on shutdown disabled",
					"num_tasks", numTasks,
				)

				return nil
			}

			if err := ctx.Err(); err != nil {
				lt.logger.Warnw(
					"not waiting on retries to flush on shutdown",
					"reason", "user stopped loadtest",
					"num_tasks", numTasks,
					"error", err,
				)
				return nil
			}

			lt.logger.Debugw(
				"waiting on retries to flush",
				"num_tasks", numTasks,
			)

			if meta.NumIntervalTasks <= 0 || numWorkers <= 0 {

				lt.logger.Errorw(
					"retry flushing could not be attempted",
					"num_tasks", numTasks,
					"num_interval_tasks", meta.NumIntervalTasks,
					"num_workers", numWorkers,
				)

				return ErrRetriesFailedToFlush
			}

			preflushNumTasks := numTasks

			lt.logger.Warnw(
				"shutting down: flushing retries",
				"num_tasks", numTasks,
				"flush_retries_timeout", lt.flushRetriesTimeout.String(),
			)

			shutdownCtx, cancel := context.WithTimeout(context.Background(), lt.flushRetriesTimeout)
			defer cancel()

			intervalID = time.Now()
			taskBuf = taskBuf[:0]
			meta.Lag = 0

			for {

				if err := shutdownCtx.Err(); err != nil {
					lt.logger.Errorw(
						"failed to flush all retries",
						"preflush_num_tasks", preflushNumTasks,
						"num_tasks", numTasks,
						"error", err,
					)

					return ErrRetriesFailedToFlush
				}

				lt.resultWaitGroup.Wait()

				for {

					if err := shutdownCtx.Err(); err != nil {
						lt.logger.Errorw(
							"failed to flush all retries",
							"preflush_num_tasks", preflushNumTasks,
							"num_tasks", numTasks,
							"error", err,
						)

						return ErrRetriesFailedToFlush
					}

					if maxTasks > 0 {
						if numTasks >= maxTasks {
							lt.logger.Errorw(
								"failed to flush all retries",
								"preflush_num_tasks", preflushNumTasks,
								"num_tasks", numTasks,
								"reason", "reached max tasks",
							)
							return ErrRetriesFailedToFlush
						}

						// 1. the below looks off/odd, why not use?:
						//
						// ```
						// if n := maxTasks - numTasks; n < numNewTasks {
						// 	numNewTasks = n
						// }
						// ```
						//
						// 2. And for that matter, why not keep meta.NumIntervalTasks in sync with numNewTasks?
						//
						// ---
						//
						// 1. The implementation would be exactly the same, just using another variable
						// 2. the meta.NumIntervalTasks value is used in RATE calculations, if we keep it in sync
						//    with BOUNDS values then the last tasks could run at a lower RATE than intended. It
						//    is only kept in sync when a user adjusts the RATE via a ConfigUpdate. Don't confuse
						//    bounds purpose values with rate purpose values.
						//
						numNewTasks = maxTasks - numTasks
						if numNewTasks > meta.NumIntervalTasks {
							numNewTasks = meta.NumIntervalTasks
						}
					}

					select {
					case <-ctxDone:
						lt.logger.Warnw(
							"user stopped loadtest while attempting to flush retries",
							"preflush_num_tasks", preflushNumTasks,
							"num_tasks", numTasks,
						)
						return nil
					default:
						// continue with load generating retries
					}

					// acquire load generation opportunity slots ( smooths bursts )
					//
					// in the shutdown retry flow we always want to acquire before reading retries
					// to avoid a deadlock edge case of the retry queue being full, all workers tasks failed and need to be retried
					if err := lt.intervalTasksSema.Acquire(shutdownCtx, int64(numNewTasks)); err != nil {
						lt.logger.Errorw(
							"failed to flush all retries",
							"preflush_num_tasks", preflushNumTasks,
							"num_tasks", numTasks,
							"error", err,
							"reason", "shutdown timeout likely reached while waiting for semaphore acquisition",
						)
						return ErrRetriesFailedToFlush
					}

					// read up to numNewTasks from retry slice
					taskBufSize := lt.readRetries(taskBuf[:numNewTasks:numNewTasks])
					if taskBufSize <= 0 {
						// wait for any pending tasks to flush and try read again

						lt.logger.Debugw(
							"verifying all retries have flushed",
							"preflush_num_tasks", preflushNumTasks,
							"num_tasks", numTasks,
						)

						lt.resultWaitGroup.Wait()

						// read up to numNewTasks from retry slice again
						taskBufSize = lt.readRetries(taskBuf[:numNewTasks:numNewTasks])
						if taskBufSize <= 0 {

							lt.logger.Infow(
								"all retries flushed",
								"preflush_num_tasks", preflushNumTasks,
								"num_tasks", numTasks,
							)
							return nil
						}
					}
					taskBuf = taskBuf[:taskBufSize]

					// re-release any extra load slots we allocated beyond what really remains to do
					if numNewTasks > taskBufSize {
						lt.intervalTasksSema.Release(int64(numNewTasks - taskBufSize))
					}

					lt.resultWaitGroup.Add(taskBufSize)

					meta.IntervalID = intervalID

					if taskBufSize == 1 || interTaskInterval <= skipInterTaskSchedulingThreshold {

						for _, task := range taskBuf {
							lt.taskChan <- taskWithMeta{task, intervalID, meta}
						}
					} else {

						lt.taskChan <- taskWithMeta{taskBuf[0], intervalID, meta}

						for _, task := range taskBuf[1:] {
							time.Sleep(interTaskInterval)
							lt.taskChan <- taskWithMeta{task, time.Now(), meta}
						}
					}

					taskBuf = taskBuf[:0]

					numTasks += taskBufSize

					meta.Lag = 0

					// wait for next interval time to exist
					nextIntervalID := intervalID.Add(interval)
					realNow := time.Now()
					delay = nextIntervalID.Sub(realNow)
					if delay > 0 {
						time.Sleep(delay)
						intervalID = nextIntervalID

						if taskBufSize < numNewTasks {
							// just finished this iteration of retry enqueuing
							//
							// break to loop through retry drain context again
							break
						}

						continue
					}

					if delay < 0 {
						lag := -delay

						intervalID = realNow
						meta.Lag = lag

						lt.resultWaitGroup.Add(1)
						lt.resultsChan <- taskResult{
							Meta: taskMeta{
								IntervalID: intervalID,
								Lag:        lag,
							},
						}
					}

					if taskBufSize < numNewTasks {
						// just finished this iteration of retry enqueuing
						//
						// break to loop through retry drain context again
						break
					}
				}
			}
		}(!lt.retriesDisabled && lt.flushRetriesOnShutdown)
		if err != nil {
			*shutdownErrResp = err
		}

		// wait for all results to come in
		lt.logger.Debugw("waiting for loadtest results")
		lt.resultWaitGroup.Wait()

		lt.logger.Debugw("stopping result handler routine")

		// signal for result handler routines to stop
		close(lt.resultsChan)

		// signal for workers to stop
		lt.logger.Debugw("stopping workers")
		for i := 0; i < len(lt.workers); i++ {
			close(lt.workers[i])
		}

		// wait for result handler routines to stop
		lt.logger.Debugw("waiting for result handler routines to stop")
		wg.Wait()

		// wait for workers to stop
		lt.logger.Debugw("waiting for workers to stop")
		lt.workerWaitGroup.Wait()

		lt.logger.Infow("loadtest stopped")
	}()

	// getTaskSlotCount is the task emission back pressure
	// throttle that conveys the number of tasks that
	// are allowed to be un-finished for the performance
	// interval under normal circumstances
	getTaskSlotCount := func() int {
		return maxPendingTasks(numWorkers, numNewTasks)
	}

	// apply initial task buffer limits to the interval semaphore
	taskSlotCount := getTaskSlotCount()
	lt.intervalTasksSema.Release(int64(taskSlotCount))

	configCausesPause := func() bool {
		return meta.NumIntervalTasks <= 0 || numWorkers <= 0
	}

	var paused bool
	var pauseStart time.Time

	handleConfigUpdateAndPauseState := func(cu ConfigUpdate) error {
		for {
			var prepSemaErr error
			var recomputeInterTaskInterval, recomputeTaskSlots bool

			if cu.numWorkers.set {
				recomputeTaskSlots = true

				n := cu.numWorkers.val

				// prevent over committing on the maxWorkers count
				if n < 0 {
					lt.logger.Errorw(
						"config update not within loadtest boundary conditions: numWorkers",
						"reason", "update tried to set numWorkers too low",
						"remediation_taken", "using min value",
						"requested", n,
						"min", 0,
					)
					n = 0
				} else if n > lt.maxWorkers {
					lt.logger.Errorw(
						"config update not within loadtest boundary conditions: numWorkers",
						"reason", "update tried to set numWorkers too high",
						"remediation_hint", "increase the loadtest MaxWorkers setting",
						"remediation_taken", "using max value",
						"requested", n,
						"max", lt.maxWorkers,
					)
					n = lt.maxWorkers
				}

				if n > numWorkers {

					// unpause workers
					for i := numWorkers; i < len(lt.workers); i++ {
						lt.workers[i] <- struct{}{}
					}

					// spawn new workers if needed
					for i := len(lt.workers); i < n; i++ {
						lt.addWorker(ctx, i)
					}
				} else if n < numWorkers {

					// pause workers if needed
					for i := numWorkers - 1; i >= n; i-- {
						lt.workers[i] <- struct{}{}
					}
				}

				configChanges = append(configChanges,
					"old_num_workers", numWorkers,
					"new_num_workers", n,
				)
				numWorkers = n
			}

			if cu.numIntervalTasks.set {
				recomputeInterTaskInterval = true
				recomputeTaskSlots = true

				n := cu.numIntervalTasks.val

				// prevent over committing on the maxIntervalTasks count
				if n < 0 {
					lt.logger.Errorw(
						"config update not within loadtest boundary conditions: numIntervalTasks",
						"reason", "update tried to set numIntervalTasks too low",
						"remediation_taken", "using min value",
						"requested", n,
						"min", 0,
					)
					n = 0
				} else if n > lt.maxIntervalTasks {
					lt.logger.Errorw(
						"config update not within loadtest boundary conditions: numIntervalTasks",
						"reason", "update tried to set numIntervalTasks too high",
						"remediation_hint", "increase the loadtest MaxIntervalTasks setting",
						"remediation_taken", "using max value",
						"requested", n,
						"max", lt.maxIntervalTasks,
					)
					n = lt.maxIntervalTasks
				}

				configChanges = append(configChanges,
					"old_num_interval_tasks", meta.NumIntervalTasks,
					"new_num_interval_tasks", n,
				)
				numNewTasks = n
				meta.NumIntervalTasks = n
			}

			if cu.interval.set {
				recomputeInterTaskInterval = true

				n := cu.interval.val

				if n < 0 {
					lt.logger.Errorw(
						"config update not within loadtest boundary conditions: interval",
						"reason", "update tried to set interval too low",
						"remediation_taken", "using min value",
						"requested", n,
						"min", 0,
					)
					n = 0
				}

				configChanges = append(configChanges,
					"old_interval", interval,
					"new_interval", n,
				)
				interval = n
			}

			// && clause: protects against divide by zero
			if recomputeInterTaskInterval && meta.NumIntervalTasks > 0 {
				interTaskInterval = interval / time.Duration(meta.NumIntervalTasks)
			}

			if recomputeTaskSlots {
				if newTaskSlotCount := getTaskSlotCount(); newTaskSlotCount != taskSlotCount {

					if newTaskSlotCount > taskSlotCount {
						lt.intervalTasksSema.Release(int64(newTaskSlotCount - taskSlotCount))
					} else {
						prepSemaErr = lt.intervalTasksSema.Acquire(ctx, int64(taskSlotCount-newTaskSlotCount))
						if prepSemaErr != nil {
							lt.logger.Errorw(
								"loadtest config update: failed to pre-acquire load generation slots",
								"error", prepSemaErr,
							)

							// not returning and error... yet
							// going to let config update log statement occur and then report the error present in prepSemaErr
						}
					}

					taskSlotCount = newTaskSlotCount
				}
			}

			if !cu.onStartup {
				lt.logger.Warnw(
					"loadtest config updated",
					configChanges...,
				)
			}
			configChanges = configChanges[:0]

			if prepSemaErr != nil {
				return prepSemaErr
			}

			// pause load generation if unable to schedule anything
			if configCausesPause() {

				if !paused {
					paused = true
					pauseStart = time.Now().UTC()

					lt.logger.Warnw(
						"pausing load generation",
						"num_interval_tasks", meta.NumIntervalTasks,
						"num_workers", numWorkers,
						"paused_at", pauseStart,
					)
				}

				// duplicating short-circuit signal control processing to give it priority over the randomizing nature of the multi-select
				// that follows
				//
				// ref: https://go.dev/ref/spec#Select_statements
				select {
				case <-ctxDone:
					return errLoadtestContextDone
				default:
				}
				select {
				case <-ctxDone:
					return errLoadtestContextDone
				case cu = <-updateChan:
					continue
				}
			}

			if paused {
				paused = false
				intervalID = time.Now()

				lt.logger.Warnw(
					"resuming load generation",
					"num_interval_tasks", meta.NumIntervalTasks,
					"num_workers", numWorkers,
					"paused_at", pauseStart,
					"resumed_at", intervalID.UTC(),
				)
			}

			return nil
		}
	}

	if configCausesPause() {
		if err := handleConfigUpdateAndPauseState(ConfigUpdate{onStartup: true}); err != nil {
			if err == errLoadtestContextDone {
				return nil
			}
			return err
		}
	}

	// start workers just before starting task scheduling
	for i := 0; i < numWorkers; i++ {
		lt.addWorker(ctx, i)
	}

	// main task scheduling loop
	for {
		if maxTasks > 0 {
			if numTasks >= maxTasks {
				lt.logger.Warnw(
					"loadtest finished: max task count reached",
					"max_tasks", maxTasks,
				)
				return nil
			}

			numNewTasks = maxTasks - numTasks
			if numNewTasks > meta.NumIntervalTasks {
				numNewTasks = meta.NumIntervalTasks
			}
		}

		select {
		case <-ctxDone:
			return nil
		case cu := <-updateChan:
			if err := handleConfigUpdateAndPauseState(cu); err != nil {
				if err == errLoadtestContextDone {
					return nil
				}
				return err
			}

			// re-loop
			continue
		default:
			// continue with load generation
		}

		var acquiredLoadGenerationSlots bool

		// read up to numNewTasks from retry slice
		taskBufSize := 0
		if !lt.retriesDisabled {

			// acquire load generation opportunity slots ( smooths bursts )
			//
			// do this early conditionally to allow retries to settle in the retry channel
			// so we can pick them up when enough buffer space has cleared
			//
			// thus we avoid a possible deadlock where the retry queue is full and the workers
			// all have failed tasks that wish to be retried
			acquiredLoadGenerationSlots = true
			if lt.intervalTasksSema.Acquire(ctx, int64(numNewTasks)) != nil {
				return nil
			}

			taskBufSize = lt.readRetries(taskBuf[:numNewTasks:numNewTasks])
		}
		taskBuf = taskBuf[:taskBufSize]

		if taskBufSize < numNewTasks {
			maxSize := numNewTasks - taskBufSize
			n := lt.taskProvider.ReadTasks(taskBuf[taskBufSize:numNewTasks:numNewTasks])
			if n < 0 || n > maxSize {
				panic(ErrBadReadTasksImpl)
			}
			if n == 0 {
				// iteration is technically done now
				// but there could be straggling retries
				// queued after this, those should continue
				// to be flushed if and only if maxTasks
				// has not been reached and if it is greater
				// than zero
				if taskBufSize == 0 {
					// return immediately if there is nothing
					// new to enqueue

					lt.logger.Warnw(
						"stopping loadtest: ReadTasks did not load enough tasks",
						"final_task_delta", 0,
					)

					return nil
				}

				lt.logger.Debugw(
					"scheduled: stopping loadtest: ReadTasks did not load enough tasks",
					"retry_count", taskBufSize,
				)
			}

			taskBufSize += n
			taskBuf = taskBuf[:taskBufSize]
		}

		// acquire load generation opportunity slots ( smooths bursts )
		// if not done already
		//
		// but if we allocated too many in our retry prep phase then release the
		// difference
		if acquiredLoadGenerationSlots {
			if numNewTasks > taskBufSize {
				lt.intervalTasksSema.Release(int64(numNewTasks - taskBufSize))
			}
		} else if lt.intervalTasksSema.Acquire(ctx, int64(taskBufSize)) != nil {
			return nil
		}

		lt.resultWaitGroup.Add(taskBufSize)

		meta.IntervalID = intervalID

		if taskBufSize <= 1 || interTaskInterval <= skipInterTaskSchedulingThreshold {

			for _, task := range taskBuf {
				lt.taskChan <- taskWithMeta{task, intervalID, meta}
			}
		} else {

			lt.taskChan <- taskWithMeta{taskBuf[0], intervalID, meta}

			for _, task := range taskBuf[1:] {
				time.Sleep(interTaskInterval)
				lt.taskChan <- taskWithMeta{task, time.Now(), meta}
			}
		}

		if numNewTasks > taskBufSize {
			// must have hit the end of ReadTasks iterator
			// increase numTasks total by actual number queued
			// and stop traffic generation
			numTasks += taskBufSize
			lt.logger.Warnw(
				"stopping loadtest: ReadTasks did not load enough tasks",
				"final_task_delta", taskBufSize,
			)
			return nil
		}

		taskBuf = taskBuf[:0]

		numTasks += taskBufSize

		meta.Lag = 0

		// wait for next interval time to exist
		nextIntervalID := intervalID.Add(interval)
		realNow := time.Now()
		delay = nextIntervalID.Sub(realNow)
		if delay > 0 {
			time.Sleep(delay)
			intervalID = nextIntervalID
			continue
		}

		if delay < 0 {
			lag := -delay

			intervalID = realNow
			meta.Lag = lag

			lt.resultWaitGroup.Add(1)
			lt.resultsChan <- taskResult{
				Meta: taskMeta{
					IntervalID: intervalID,
					Lag:        lag,
				},
			}
		}
	}
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

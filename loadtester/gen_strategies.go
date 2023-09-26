package loadtester

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"time"
)

func doTaskRetriesEnabled(ctx context.Context, lt *Loadtest, workerID int, task Doer) (result taskResultFlags) {
	var err_resp error

	// phase is the name of the step which has possibly caused a panic
	phase := "do"
	
	var rt *retryTask
	if v, ok := task.(*retryTask); ok {
		rt = v
		phase = "retry"
		defer func() {
			*rt = retryTask{}
			lt.retryTaskPool.Put(v)
		}()
	}
	defer func() {

		if err_resp != nil {
			lt.logger.WarnContext(ctx,
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
				lt.logger.ErrorContext(ctx,
					"worker recovered from panic",
					"worker_id", workerID,
					"phase", phase,
					"error", v,
				)
			case []byte:
				lt.logger.ErrorContext(ctx,
					"worker recovered from panic",
					"worker_id", workerID,
					"phase", phase,
					"error", string(v),
				)
			case string:
				lt.logger.ErrorContext(ctx,
					"worker recovered from panic",
					"worker_id", workerID,
					"phase", phase,
					"error", v,
				)
			default:
				const msg = "unknown cause"

				lt.logger.ErrorContext(ctx,
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

	// queue a new retry task
	{
		rt := lt.retryTaskPool.Get().(*retryTask)

		*rt = retryTask{dr, err}

		lt.retryTaskChan <- rt
	}

	result.RetryQueued = 1
	return
}

func runRetriesEnabled(ctx context.Context, lt *Loadtest, shutdownErrResp *error) error {

	cfgUpdateChan := lt.cfgUpdateChan
	defer close(cfgUpdateChan)

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

	lt.logger.InfoContext(ctx,
		"starting loadtest",
		"config", lt.loadtestConfigAsJson(),
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
	taskReader := lt.taskReader
	configChanges := make([]any, 0, 12)
	meta := taskMeta{
		NumIntervalTasks: lt.numIntervalTasks,
	}
	var interTaskInterval time.Duration
	if meta.NumIntervalTasks > 0 {
		interTaskInterval = interval / time.Duration(meta.NumIntervalTasks)
	}

	taskBuf := make([]Doer, 0, lt.maxIntervalTasks)

	var delay time.Duration

    
	readRetries := func(p []Doer) int {
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

	// stopping routine runs on return
	// flushing as much as possible
	defer func() {

        
		err := func(flushRetries bool) error {
			if !flushRetries {

				lt.logger.DebugContext(ctx,
					"not waiting on retries to flush on shutdown",
					"reason", "retries disabled or flush retries on shutdown disabled",
					"num_tasks", numTasks,
				)

				return nil
			}

			if err := ctx.Err(); err != nil {
				lt.logger.WarnContext(ctx,
					"not waiting on retries to flush on shutdown",
					"reason", "user stopped loadtest",
					"num_tasks", numTasks,
					"error", err,
				)
				return nil
			}

			lt.logger.DebugContext(ctx,
				"waiting on retries to flush",
				"num_tasks", numTasks,
			)

			if meta.NumIntervalTasks <= 0 || numWorkers <= 0 {

				lt.logger.ErrorContext(ctx,
					"retry flushing could not be attempted",
					"num_tasks", numTasks,
					"num_interval_tasks", meta.NumIntervalTasks,
					"num_workers", numWorkers,
				)

				return ErrRetriesFailedToFlush
			}

			preflushNumTasks := numTasks

			lt.logger.WarnContext(ctx,
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
					lt.logger.ErrorContext(ctx,
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
						lt.logger.ErrorContext(ctx,
							"failed to flush all retries",
							"preflush_num_tasks", preflushNumTasks,
							"num_tasks", numTasks,
							"error", err,
						)

						return ErrRetriesFailedToFlush
					}

					if maxTasks > 0 {
						if numTasks >= maxTasks {
							lt.logger.ErrorContext(ctx,
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
						lt.logger.WarnContext(ctx,
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
						lt.logger.ErrorContext(ctx,
							"failed to flush all retries",
							"preflush_num_tasks", preflushNumTasks,
							"num_tasks", numTasks,
							"error", err,
							"reason", "shutdown timeout likely reached while waiting for semaphore acquisition",
						)
						return ErrRetriesFailedToFlush
					}

					// read up to numNewTasks from retry slice
					taskBufSize := readRetries(taskBuf[:numNewTasks:numNewTasks])
					if taskBufSize <= 0 {
						// wait for any pending tasks to flush and try read again

						lt.logger.DebugContext(ctx,
							"verifying all retries have flushed",
							"preflush_num_tasks", preflushNumTasks,
							"num_tasks", numTasks,
						)

						lt.resultWaitGroup.Wait()

						// read up to numNewTasks from retry slice again
						taskBufSize = readRetries(taskBuf[:numNewTasks:numNewTasks])
						if taskBufSize <= 0 {

							lt.logger.InfoContext(ctx,
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
		}(lt.flushRetriesOnShutdown)
		if err != nil {
			*shutdownErrResp = err
		}

		// wait for all results to come in
		lt.logger.DebugContext(ctx,
			"waiting for loadtest results",
		)
		lt.resultWaitGroup.Wait()

		lt.logger.DebugContext(ctx,
			"stopping result handler routine",
		)

		// signal for result handler routines to stop
		close(lt.resultsChan)

		// signal for workers to stop
		lt.logger.DebugContext(ctx,
			"stopping workers",
		)
		for i := 0; i < len(lt.workers); i++ {
			close(lt.workers[i])
		}

		// wait for result handler routines to stop
		lt.logger.DebugContext(ctx,
			"waiting for result handler routines to stop",
		)
		wg.Wait()

		// wait for workers to stop
		lt.logger.DebugContext(ctx,
			"waiting for workers to stop",
		)
		lt.workerWaitGroup.Wait()

		lt.logger.InfoContext(ctx,
			"loadtest stopped",
		)
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
					lt.logger.ErrorContext(ctx,
						"config update not within loadtest boundary conditions: numWorkers",
						"reason", "update tried to set numWorkers too low",
						"remediation_taken", "using min value",
						"requested", n,
						"min", 0,
					)
					n = 0
				} else if n > lt.maxWorkers {
					lt.logger.ErrorContext(ctx,
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
					lt.logger.ErrorContext(ctx,
						"config update not within loadtest boundary conditions: numIntervalTasks",
						"reason", "update tried to set numIntervalTasks too low",
						"remediation_taken", "using min value",
						"requested", n,
						"min", 0,
					)
					n = 0
				} else if n > lt.maxIntervalTasks {
					lt.logger.ErrorContext(ctx,
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
					lt.logger.ErrorContext(ctx,
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
							lt.logger.ErrorContext(ctx,
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
				lt.logger.WarnContext(ctx,
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

					lt.logger.WarnContext(ctx,
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
				case cu = <-cfgUpdateChan:
					continue
				}
			}

			if paused {
				paused = false
				intervalID = time.Now()

				lt.logger.WarnContext(ctx,
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
				lt.logger.WarnContext(ctx,
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
		case cu := <-cfgUpdateChan:
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

		taskBufSize = readRetries(taskBuf[:numNewTasks:numNewTasks])
		
		
		taskBuf = taskBuf[:taskBufSize]

		if taskBufSize < numNewTasks {
			maxSize := numNewTasks - taskBufSize
			n := taskReader.ReadTasks(taskBuf[taskBufSize:numNewTasks:numNewTasks])
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

					lt.logger.WarnContext(ctx,
						"stopping loadtest: ReadTasks did not load enough tasks",
						"final_task_delta", 0,
					)

					return nil
				}

				lt.logger.DebugContext(ctx,
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
			lt.logger.WarnContext(ctx,
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

func doTaskRetriesDisabled(ctx context.Context, lt *Loadtest, workerID int, task Doer) (result taskResultFlags) {
	var err_resp error

	// phase is the name of the step which has possibly caused a panic
	phase := "do"
	
	defer func() {

		if err_resp != nil {
			lt.logger.WarnContext(ctx,
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
				lt.logger.ErrorContext(ctx,
					"worker recovered from panic",
					"worker_id", workerID,
					"phase", phase,
					"error", v,
				)
			case []byte:
				lt.logger.ErrorContext(ctx,
					"worker recovered from panic",
					"worker_id", workerID,
					"phase", phase,
					"error", string(v),
				)
			case string:
				lt.logger.ErrorContext(ctx,
					"worker recovered from panic",
					"worker_id", workerID,
					"phase", phase,
					"error", v,
				)
			default:
				const msg = "unknown cause"

				lt.logger.ErrorContext(ctx,
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

	
	return
}

func runRetriesDisabled(ctx context.Context, lt *Loadtest, shutdownErrResp *error) error {

	cfgUpdateChan := lt.cfgUpdateChan
	defer close(cfgUpdateChan)

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

	lt.logger.InfoContext(ctx,
		"starting loadtest",
		"config", lt.loadtestConfigAsJson(),
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
	taskReader := lt.taskReader
	configChanges := make([]any, 0, 12)
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

        
        *shutdownErrResp = nil

		// wait for all results to come in
		lt.logger.DebugContext(ctx,
			"waiting for loadtest results",
		)
		lt.resultWaitGroup.Wait()

		lt.logger.DebugContext(ctx,
			"stopping result handler routine",
		)

		// signal for result handler routines to stop
		close(lt.resultsChan)

		// signal for workers to stop
		lt.logger.DebugContext(ctx,
			"stopping workers",
		)
		for i := 0; i < len(lt.workers); i++ {
			close(lt.workers[i])
		}

		// wait for result handler routines to stop
		lt.logger.DebugContext(ctx,
			"waiting for result handler routines to stop",
		)
		wg.Wait()

		// wait for workers to stop
		lt.logger.DebugContext(ctx,
			"waiting for workers to stop",
		)
		lt.workerWaitGroup.Wait()

		lt.logger.InfoContext(ctx,
			"loadtest stopped",
		)
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
					lt.logger.ErrorContext(ctx,
						"config update not within loadtest boundary conditions: numWorkers",
						"reason", "update tried to set numWorkers too low",
						"remediation_taken", "using min value",
						"requested", n,
						"min", 0,
					)
					n = 0
				} else if n > lt.maxWorkers {
					lt.logger.ErrorContext(ctx,
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
					lt.logger.ErrorContext(ctx,
						"config update not within loadtest boundary conditions: numIntervalTasks",
						"reason", "update tried to set numIntervalTasks too low",
						"remediation_taken", "using min value",
						"requested", n,
						"min", 0,
					)
					n = 0
				} else if n > lt.maxIntervalTasks {
					lt.logger.ErrorContext(ctx,
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
					lt.logger.ErrorContext(ctx,
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
							lt.logger.ErrorContext(ctx,
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
				lt.logger.WarnContext(ctx,
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

					lt.logger.WarnContext(ctx,
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
				case cu = <-cfgUpdateChan:
					continue
				}
			}

			if paused {
				paused = false
				intervalID = time.Now()

				lt.logger.WarnContext(ctx,
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
				lt.logger.WarnContext(ctx,
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
		case cu := <-cfgUpdateChan:
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

        
		taskBufSize := 0
		taskBuf = taskBuf[:taskBufSize]

		if taskBufSize < numNewTasks {
			maxSize := numNewTasks - taskBufSize
			n := taskReader.ReadTasks(taskBuf[taskBufSize:numNewTasks:numNewTasks])
			if n < 0 || n > maxSize {
				panic(ErrBadReadTasksImpl)
			}
            if n == 0 {
                
                lt.logger.WarnContext(ctx,
                    "stopping loadtest: ReadTasks did not load enough tasks",
                    "final_task_delta", 0,
                )

				return nil
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
			lt.logger.WarnContext(ctx,
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


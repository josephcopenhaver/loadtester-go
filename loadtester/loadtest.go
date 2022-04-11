package loadtester

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
)

// TODO: offer a http client with a connection pool that matches max worker size plus some buffer/padding

// TODO: RetriesDisabled runtimes checks can turn into init time checks; same with MaxTotalTasks based checks
// I would not dream of doing this before proving it is warranted first.

type LoadtestImpl interface {
	// ReadTasks fills the provided slice up to slice length starting at index 0 and returns how many records have been inserted
	ReadTasks([]Doer) int
	// UpdateChan should return the same channel each time or nil;
	// but once nil it must never be non-nil again
	UpdateChan() <-chan ConfigUpdate
}

type Loadtest struct {
	impl            LoadtestImpl
	maxTotalTasks   int
	maxWorkers      int
	numWorkers      int
	workers         []chan struct{}
	workerWaitGroup sync.WaitGroup
	resultWaitGroup sync.WaitGroup
	taskChan        chan taskWithMeta
	resultsChan     chan taskResult

	numIntervalTasks int
	maxIntervalTasks int
	interval         time.Duration

	retryTaskChan chan *retryTask
	retryTaskPool sync.Pool

	startTime time.Time
	csvData

	flushRetriesOnShutdown bool
	flushRetriesTimeout    time.Duration
	RetriesDisabled        bool
}

func NewLoadtest(impl LoadtestImpl, options ...LoadtestOption) (*Loadtest, error) {

	opt := loadtestOptions{
		taskBufferingFactor:     4,
		maxWorkers:              1,
		numWorkers:              1,
		maxIntervalTasks:        1,
		numIntervalTasks:        1,
		interval:                time.Second,
		csvOutputFilename:       "metrics.csv",
		csvOutputFlushFrequency: 5 * time.Second,
		flushRetriesTimeout:     2 * time.Minute,
	}

	for _, op := range options {
		op(&opt)
	}

	if !opt.maxWorkersSet && opt.numWorkersSet {
		opt.maxWorkers = opt.numWorkers
	}

	if !opt.maxIntervalTasksSet && opt.numIntervalTasksSet {
		opt.maxIntervalTasks = opt.numIntervalTasks
	}

	if opt.maxWorkers < opt.numWorkers {
		return nil, errors.New("loadtest misconfigured: MaxWorkers < NumWorkers")
	}

	if opt.maxWorkers < 1 {
		return nil, errors.New("loadtest misconfigured: MaxWorkers < 1")
	}

	if opt.maxIntervalTasks < opt.numIntervalTasks {
		return nil, errors.New("loadtest misconfigured: MaxIntervalTasks < NumIntervalTasks")
	}

	if opt.maxIntervalTasks < 1 {
		return nil, errors.New("loadtest misconfigured: maxIntervalTasks < 1")
	}

	if opt.taskBufferingFactor <= 0 {
		opt.taskBufferingFactor = 1
	}

	maxWorkQueueSize := opt.maxIntervalTasks * opt.taskBufferingFactor
	numPossibleLagResults := opt.taskBufferingFactor

	var csvWriteErr error
	if opt.csvOutputDisabled {
		csvWriteErr = errCsvWriterDisabled
	}

	var retryTaskChan chan *retryTask
	if !opt.retriesDisabled {
		retryTaskChan = make(chan *retryTask, maxWorkQueueSize)
	}

	return &Loadtest{
		impl:          impl,
		maxTotalTasks: opt.maxTotalTasks,
		maxWorkers:    opt.maxWorkers,
		numWorkers:    opt.numWorkers,
		workers:       make([]chan struct{}, 0, opt.maxWorkers),
		taskChan:      make(chan taskWithMeta, maxWorkQueueSize),
		resultsChan:   make(chan taskResult, maxWorkQueueSize+numPossibleLagResults),
		retryTaskChan: retryTaskChan,

		maxIntervalTasks: opt.maxIntervalTasks,
		numIntervalTasks: opt.numIntervalTasks,
		interval:         opt.interval,
		retryTaskPool: sync.Pool{
			New: func() interface{} {
				return &retryTask{}
			},
		},

		csvData: csvData{
			outputFilename: opt.csvOutputFilename,
			flushFrequency: opt.csvOutputFlushFrequency,
			writeErr:       csvWriteErr,
		},

		flushRetriesTimeout:    opt.flushRetriesTimeout,
		flushRetriesOnShutdown: opt.flushRetriesOnShutdown,
		RetriesDisabled:        opt.retriesDisabled,
	}, nil
}

var (
	errCsvWriterDisabled = errors.New("csv metrics writer disabled")
	ErrBadReadTasksImpl  = errors.New("bad ReadTasks implementation: returned a value less than zero or larger than the input slice length")
)

func timeToString(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

type csvData struct {
	outputFilename string
	writer         *csv.Writer
	flushFrequency time.Duration
	flushDeadline  time.Time
	writeErr       error
}

type taskResult struct {
	Passed                       uint8
	Panicked                     uint8
	RetryQueued                  uint8
	Errored                      uint8
	QueuedDuration, TaskDuration time.Duration
	Meta                         taskMeta
}

type taskMeta struct {
	IntervalID       time.Time
	NumIntervalTasks int
	Lag              time.Duration `json:",omitempty"`
}

type taskWithMeta struct {
	doer        Doer
	enqueueTime time.Time
	meta        taskMeta
}

type retryTask struct {
	DoRetryer
	err error
}

func (rt *retryTask) Do(ctx context.Context, workerID int) error {
	return rt.DoRetryer.Retry(ctx, workerID, rt.err)
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
		case task := <-lt.taskChan:

			taskStart := time.Now()
			passed, errored, retryQueued, panicked := lt.doTask(ctx, workerID, task)
			taskEnd := time.Now()

			lt.resultsChan <- taskResult{
				Passed:         passed,
				Panicked:       panicked,
				RetryQueued:    retryQueued,
				Errored:        errored,
				QueuedDuration: taskStart.Sub(task.enqueueTime),
				TaskDuration:   taskEnd.Sub(taskStart),
				Meta:           task.meta,
			}
		}
	}
}

func (lt *Loadtest) doTask(ctx context.Context, workerID int, taskWithMeta taskWithMeta) (success_resp, errored_resp, retryQueued_resp, panicking_resp uint8) {
	var err_resp error

	task := taskWithMeta.doer
	defer func() {
		if v, ok := task.(*retryTask); ok {
			*v = retryTask{}
			lt.retryTaskPool.Put(v)
		}
	}()
	defer func() {

		if err_resp != nil {
			Logger.Warnw(
				"task error",
				"worker_id", workerID,
				"error", err_resp,
			)
		}

		if r := recover(); r != nil {
			panicking_resp = 1
			errored_resp = 1

			switch v := r.(type) {
			case error:
				Logger.Errorw(
					"worker recovered from panic",
					"worker_id", workerID,
					"error", v,
				)
			case []byte:
				Logger.Errorw(
					"worker recovered from panic",
					"worker_id", workerID,
					"error", string(v),
				)
			case string:
				Logger.Errorw(
					"worker recovered from panic",
					"worker_id", workerID,
					"error", v,
				)
			default:
				const msg = "unknown cause"

				Logger.Errorw(
					"worker recovered from panic",
					"worker_id", workerID,
					"error", msg,
				)
			}
		}
	}()

	err := task.Do(ctx, workerID)
	if err != nil {
		if !lt.RetriesDisabled {
			if v, ok := task.(DoRetryer); ok {
				if v, ok := v.(DoRetryChecker); ok && !v.CanRetry(ctx, workerID, err) {
					err_resp = err
					return 0, 1, 0, 0
				}

				if x, ok := v.(*retryTask); ok {
					v = x.DoRetryer
				}

				lt.retryTaskChan <- lt.newRetryTask(v, err)

				err_resp = err
				return 0, 1, 1, 0
			}
		}

		err_resp = err
		return 0, 1, 0, 0
	}

	return 1, 0, 0, 0
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

func (lt *Loadtest) resultsHandler(wg *sync.WaitGroup, stopChan <-chan struct{}) {
	defer wg.Done()

	var intervalID time.Time
	var totalResultCount, resultCount, numIntervalTasks, numPass, numFail, numRetry, numPanic int
	var lag, minQueuedDuration, maxQueuedDuration, sumQueuedDuration, minTaskDuration, maxTaskDuration, sumTaskDuration, sumLag time.Duration

	minQueuedDuration = maxDuration
	minTaskDuration = maxDuration

	writeRow := func() {
		totalResultCount += resultCount
		lt.csvData.writeErr = lt.writeOutputCsvRow(metricRecord{
			totalResultCount:  totalResultCount,
			intervalID:        intervalID,
			sumLag:            sumLag,
			numIntervalTasks:  numIntervalTasks,
			lag:               lag,
			numTasks:          resultCount,
			numPass:           numPass,
			numFail:           numFail,
			numRetry:          numRetry,
			numPanic:          numPanic,
			minQueuedDuration: minQueuedDuration,
			maxQueuedDuration: maxQueuedDuration,
			sumQueuedDuration: sumQueuedDuration,
			minTaskDuration:   minTaskDuration,
			maxTaskDuration:   maxTaskDuration,
			sumTaskDuration:   sumTaskDuration,
		})
	}

	lt.csvData.flushDeadline = time.Now().Add(lt.csvData.flushFrequency)

	for {
		var tr taskResult

		select {
		case tr = <-lt.resultsChan:
		case <-stopChan:
			if lt.csvData.writeErr == nil && resultCount > 0 {
				writeRow()
			}
			return
		}

		lt.resultWaitGroup.Done()

		if lt.csvData.writeErr != nil {
			continue
		}

		if tr.Meta.IntervalID.IsZero() {

			sumLag += tr.Meta.Lag

			continue
		}

		if intervalID.Before(tr.Meta.IntervalID) {
			intervalID = tr.Meta.IntervalID
			numIntervalTasks = tr.Meta.NumIntervalTasks
			lag = tr.Meta.Lag
		}

		if minQueuedDuration > tr.QueuedDuration {
			minQueuedDuration = tr.QueuedDuration
		}

		if minTaskDuration > tr.TaskDuration {
			minTaskDuration = tr.TaskDuration
		}

		if maxTaskDuration < tr.TaskDuration {
			maxTaskDuration = tr.TaskDuration
		}

		if maxQueuedDuration < tr.QueuedDuration {
			maxQueuedDuration = tr.QueuedDuration
		}

		sumQueuedDuration += tr.QueuedDuration
		sumTaskDuration += tr.TaskDuration
		numPass += int(tr.Passed)
		numFail += int(tr.Errored)
		numPanic += int(tr.Panicked)
		numRetry += int(tr.RetryQueued)

		resultCount++

		if resultCount >= numIntervalTasks {

			writeRow()

			if lt.csvData.writeErr == nil && !lt.csvData.flushDeadline.After(time.Now()) {
				lt.csvData.writer.Flush()
				lt.csvData.writeErr = lt.csvData.writer.Error()
				lt.csvData.flushDeadline = time.Now().Add(lt.csvData.flushFrequency)
			}

			// reset metrics
			minQueuedDuration = maxDuration
			minTaskDuration = maxDuration
			maxQueuedDuration = -1
			maxTaskDuration = -1
			sumQueuedDuration = 0
			sumTaskDuration = 0
			resultCount = 0
			numPass = 0
			numFail = 0
			numRetry = 0
			numPanic = 0
			sumLag = 0
			lag = 0
		}
	}
}

const (
	skipInterTaskSchedulingThreshold = 20 * time.Millisecond
	maxDuration                      = time.Duration((^uint64(0)) >> 1)
)

func (lt *Loadtest) getLoadtestConfigAsJson() interface{} {
	type Config struct {
		StartTime              string `json:"start_time"`
		Interval               string `json:"interval"`
		MaxIntervalTasks       int    `json:"max_interval_tasks"`
		MaxTotalTasks          int    `json:"max_total_tasks"`
		MaxWorkers             int    `json:"max_workers"`
		NumIntervalTasks       int    `json:"num_interval_tasks"`
		NumWorkers             int    `json:"num_workers"`
		MetricsFlushFrequency  string `json:"metrics_flush_frequency"`
		FlushRetriesOnShutdown bool   `json:"flush_retries_on_shutdown"`
		FlushRetriesTimeout    string `json:"flush_retries_timeout"`
	}

	return Config{
		StartTime:              timeToString(lt.startTime),
		Interval:               lt.interval.String(),
		MaxIntervalTasks:       lt.maxIntervalTasks,
		MaxTotalTasks:          lt.maxTotalTasks,
		MaxWorkers:             lt.maxWorkers,
		NumIntervalTasks:       lt.numIntervalTasks,
		NumWorkers:             lt.numWorkers,
		MetricsFlushFrequency:  lt.csvData.flushFrequency.String(),
		FlushRetriesOnShutdown: lt.flushRetriesOnShutdown,
		FlushRetriesTimeout:    lt.flushRetriesTimeout.String(),
	}
}

func (lt *Loadtest) writeOutputCsvConfigComment(w io.Writer) error {
	if _, err := w.Write([]byte(`# `)); err != nil {
		return err
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	err := enc.Encode(struct {
		C interface{} `json:"config"`
	}{lt.getLoadtestConfigAsJson()})
	if err != nil {
		return err
	}

	if _, err := w.Write([]byte("\n")); err != nil {
		return err
	}

	return nil
}

type metricRecord struct {
	intervalID                                              time.Time
	sumLag                                                  time.Duration
	numIntervalTasks                                        int
	lag                                                     time.Duration
	numTasks                                                int
	numPass                                                 int
	numFail                                                 int
	numRetry                                                int
	numPanic                                                int
	totalResultCount                                        int
	minTaskDuration, maxTaskDuration, sumTaskDuration       time.Duration
	minQueuedDuration, maxQueuedDuration, sumQueuedDuration time.Duration
}

func (lt *Loadtest) writeOutputCsvHeaders() error {

	fields := []string{
		"sample_time",
		"interval_id",        // gauge
		"num_interval_tasks", // gauge
		"lag",                // gauge
		"sum_lag",
		"num_tasks",
		"num_pass",
		"num_fail",
		"num_retry",
		"num_panic",
		"min_queued_duration",
		"avg_queued_duration",
		"max_queued_duration",
		"sum_queued_duration",
		"min_task_duration",
		"avg_task_duration",
		"max_task_duration",
		"sum_task_duration",
	}

	if lt.maxTotalTasks > 0 {
		fields = append(fields, "percent_done")
	}

	err := lt.csvData.writer.Write(fields)
	if err != nil {
		return err
	}

	// ensure headers flush asap
	lt.csvData.writer.Flush()

	return lt.csvData.writer.Error()
}

func (lt *Loadtest) writeOutputCsvRow(mr metricRecord) error {
	if lt.csvData.writeErr != nil {
		return nil
	}

	nowStr := timeToString(time.Now())

	fields := []string{
		nowStr,
		timeToString(mr.intervalID),
		strconv.Itoa(mr.numIntervalTasks),
		mr.lag.String(),
		mr.sumLag.String(),
		strconv.Itoa(mr.numTasks),
		strconv.Itoa(mr.numPass),
		strconv.Itoa(mr.numFail),
		strconv.Itoa(mr.numRetry),
		strconv.Itoa(mr.numPanic),
		mr.minQueuedDuration.String(),
		(mr.sumQueuedDuration / time.Duration(mr.numTasks)).String(),
		mr.maxQueuedDuration.String(),
		mr.sumQueuedDuration.String(),
		mr.minTaskDuration.String(),
		(mr.sumTaskDuration / time.Duration(mr.numTasks)).String(),
		mr.maxTaskDuration.String(),
		mr.sumTaskDuration.String(),
		"",
	}

	if lt.maxTotalTasks > 0 {
		high := mr.totalResultCount * 10000 / lt.maxTotalTasks
		low := high % 100
		high /= 100
		var prefix string
		if low < 10 {
			prefix = "0"
		}
		fields[len(fields)-1] = strconv.Itoa(high) + "." + prefix + strconv.Itoa(low)
	} else {
		fields = fields[:len(fields)-1]
	}

	return lt.csvData.writer.Write(fields)
}

var ErrRetriesFailedToFlush = errors.New("failed to flush all retries")

func (lt *Loadtest) Run(ctx context.Context) (err_result error) {

	lt.startTime = time.Now()

	if lt.csvData.writeErr == nil {

		csvf, err := os.Create(lt.csvData.outputFilename)
		if err != nil {
			return fmt.Errorf("failed to open output csv metrics file for writing: %w", err)
		}
		defer func() {
			if lt.csvData.writeErr == nil {
				lt.csvData.writer.Flush()
				lt.csvData.writeErr = lt.csvData.writer.Error()
				if lt.csvData.writeErr == nil {
					_, lt.csvData.writeErr = csvf.Write([]byte("\n# {\"done\":{\"end_time\":\"" + timeToString(time.Now()) + "\"}}\n"))
				}
			}

			err := csvf.Close()
			if err != nil {
				if lt.csvData.writeErr != nil {
					lt.csvData.writeErr = err
				}
			}

			if err_result == nil && lt.csvData.writeErr != errCsvWriterDisabled {
				err_result = lt.csvData.writeErr
			}
		}()

		lt.csvData.writeErr = lt.writeOutputCsvConfigComment(csvf)

		if lt.csvData.writeErr == nil {

			lt.csvData.writer = csv.NewWriter(csvf)

			lt.csvData.writeErr = lt.writeOutputCsvHeaders()
		}
	}

	Logger.Infow(
		"starting loadtest",
		"config", lt.getLoadtestConfigAsJson(),
	)

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	wg.Add(1)
	go lt.resultsHandler(&wg, stopChan)

	numWorkers := lt.numWorkers

	var totalNumTasks int

	intervalID := time.Now()

	maxTotalTasks := lt.maxTotalTasks

	interval := lt.interval
	numNewTasks := lt.numIntervalTasks
	ctxDone := ctx.Done()
	updatechan := lt.impl.UpdateChan()
	configChanges := make([]interface{}, 0, 12)
	meta := taskMeta{
		NumIntervalTasks: lt.numIntervalTasks,
	}
	interTaskInterval := interval / time.Duration(meta.NumIntervalTasks)

	taskBuf := make([]Doer, 0, lt.maxIntervalTasks)

	var delay time.Duration

	// stopping routine runs on return
	// flushing as much as possible
	defer func() {

		err := func(flushRetries bool) error {
			if !flushRetries {

				Logger.Debugw(
					"waiting on results to flush",
					"total_num_tasks", totalNumTasks,
				)

				return nil
			}

			if meta.NumIntervalTasks <= 0 || numWorkers <= 0 {

				Logger.Errorw(
					"retry flushing could not be attempted",
					"total_num_tasks", totalNumTasks,
					"num_interval_tasks", meta.NumIntervalTasks,
					"num_workers", numWorkers,
				)

				return ErrRetriesFailedToFlush
			}

			originalTotalNumTasks := totalNumTasks

			Logger.Warnw(
				"shutting down: flushing retries",
				"total_num_tasks", totalNumTasks,
				"flush_retries_timeout", lt.flushRetriesTimeout.String(),
			)

			shutdownCtx, cancel := context.WithTimeout(context.Background(), lt.flushRetriesTimeout)
			defer cancel()

			intervalID = time.Now()
			taskBuf = taskBuf[:0]
			meta.Lag = 0

			for {

				if shutdownCtx.Err() != nil {
					Logger.Errorw(
						"failed to flush all retries",
						"original_total_num_tasks", originalTotalNumTasks,
						"total_num_tasks", totalNumTasks,
					)

					return ErrRetriesFailedToFlush
				}

				lt.resultWaitGroup.Wait()

				for {

					if shutdownCtx.Err() != nil {
						Logger.Errorw(
							"failed to flush all retries",
							"original_total_num_tasks", originalTotalNumTasks,
							"total_num_tasks", totalNumTasks,
						)

						return ErrRetriesFailedToFlush
					}

					if maxTotalTasks > 0 {
						if totalNumTasks >= maxTotalTasks {
							Logger.Errorw(
								"failed to flush all retries",
								"original_total_num_tasks", originalTotalNumTasks,
								"total_num_tasks", totalNumTasks,
								"reason", "reached max total tasks",
							)
							return ErrRetriesFailedToFlush
						}

						numNewTasks = maxTotalTasks - totalNumTasks
						if numNewTasks > meta.NumIntervalTasks {
							numNewTasks = meta.NumIntervalTasks
						}
					}

					select {
					case <-ctxDone:
						Logger.Warnw(
							"user stopped loadtest while attempting to flush retries",
							"original_total_num_tasks", originalTotalNumTasks,
							"total_num_tasks", totalNumTasks,
						)
						return nil
					default:
						// continue with load generating retries
					}

					// read up to numNewTasks from retry slice
					taskBufSize := lt.readRetries(taskBuf[:numNewTasks:numNewTasks])
					if taskBufSize <= 0 {
						// wait for any pending tasks to flush and try read again

						Logger.Debugw(
							"verifying all retries have flushed",
							"original_total_num_tasks", originalTotalNumTasks,
							"total_num_tasks", totalNumTasks,
						)

						lt.resultWaitGroup.Wait()

						// read up to numNewTasks from retry slice again
						taskBufSize = lt.readRetries(taskBuf[:numNewTasks:numNewTasks])
						if taskBufSize <= 0 {

							Logger.Infow(
								"all retries flushed",
								"original_total_num_tasks", originalTotalNumTasks,
								"total_num_tasks", totalNumTasks,
							)
							return nil
						}
					}
					taskBuf = taskBuf[:taskBufSize]

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

					totalNumTasks += taskBufSize

					meta.Lag = 0

					// wait for next interval time to exist
					nextIntervalID := intervalID.Add(interval)
					realNow := time.Now()
					delay = nextIntervalID.Sub(realNow)
					if delay > 0 {
						time.Sleep(delay)
						intervalID = nextIntervalID

						if taskBufSize < numNewTasks {
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
								Lag: lag,
							},
						}
					}

					if taskBufSize < numNewTasks {
						break
					}
				}
			}
		}(!lt.RetriesDisabled && lt.flushRetriesOnShutdown)
		if err != nil && err_result == nil {
			err_result = err
		}

		lt.resultWaitGroup.Wait()

		Logger.Debugw("stopping result handler routines")

		// signal for handler routines to stop
		close(stopChan)

		Logger.Debugw("waiting for result handler routines to stop")

		// wait for handler routines to stop
		wg.Wait()

		Logger.Debugw("stopping workers")

		// signal for workers to stop
		for i := 0; i < len(lt.workers); i++ {
			close(lt.workers[i])
		}

		Logger.Debugw("waiting for workers to stop")

		// wait for workers to stop
		lt.workerWaitGroup.Wait()
	}()

	for i := 0; i < numWorkers; i++ {
		lt.addWorker(ctx, i)
	}

	for {
		if maxTotalTasks > 0 {
			if totalNumTasks >= maxTotalTasks {
				Logger.Warnw(
					"loadtest finished: max total task count reached",
					"max_total_tasks", maxTotalTasks,
				)
				return
			}

			numNewTasks = maxTotalTasks - totalNumTasks
			if numNewTasks > meta.NumIntervalTasks {
				numNewTasks = meta.NumIntervalTasks
			}
		}

		select {
		case <-ctxDone:
			return
		case cu := <-updatechan:
			var recomputeInterTaskInterval bool

			if cu.numWorkers.set {
				n := cu.numWorkers.val

				// prevent over commiting on the maxWorkers count
				if n > lt.maxWorkers {
					Logger.Errorw(
						"config update not within loadtest boundary conditions: numWorkers",
						"reason", "update tried to set numWorkers set too high",
						"remediation_hint", "increase the loadtests's MaxWorkers setting",
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

				n := cu.numIntervalTasks.val

				// prevent over commiting on the maxIntervalTasks count
				if n > lt.maxIntervalTasks {
					Logger.Errorw(
						"config update not within loadtest boundary conditions: numIntervalTasks",
						"reason", "update tried to set numIntervalTasks set too high",
						"remediation_hint", "increase the loadtests's MaxIntervalTasks setting",
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

				configChanges = append(configChanges,
					"old_interval", interval,
					"new_interval", cu.interval.val,
				)
				interval = cu.interval.val
			}

			if recomputeInterTaskInterval {
				interTaskInterval = interval / time.Duration(meta.NumIntervalTasks)
			}

			Logger.Warnw(
				"loadtest config updated",
				configChanges...,
			)
			configChanges = configChanges[:0]

			// pause load generation if unable to schedule anything
			if meta.NumIntervalTasks <= 0 || numWorkers <= 0 {

				pauseStart := time.Now()

				Logger.Warnw(
					"pausing load generation",
					"num_interval_tasks", meta.NumIntervalTasks,
					"num_workers", numWorkers,
					"paused_at", pauseStart.UTC(),
				)

				select {
				case <-ctxDone:
					return
				case cu = <-updatechan:
					pauseEnd := time.Now()

					Logger.Warnw(
						"resuming load generation",
						"num_interval_tasks", meta.NumIntervalTasks,
						"num_workers", numWorkers,
						"paused_at", pauseStart,
						"resumed_at", pauseEnd.UTC(),
					)

					intervalID = pauseEnd
				}
			}

			// re-loop
			continue
		default:
			// continue with load generation
		}

		// read up to numNewTasks from retry slice
		taskBufSize := 0
		if !lt.RetriesDisabled {
			taskBufSize = lt.readRetries(taskBuf[:numNewTasks:numNewTasks])
		}
		taskBuf = taskBuf[:taskBufSize]

		if taskBufSize < numNewTasks {
			maxSize := numNewTasks - taskBufSize
			n := lt.impl.ReadTasks(taskBuf[taskBufSize:numNewTasks:numNewTasks])
			if n < 0 || n > maxSize {
				panic(ErrBadReadTasksImpl)
			}
			if n == 0 {
				// iteration is technically done now
				// but there could be straggling retries
				// queued after this, those should continue
				// to be flushed if and only if maxTotalTasks
				// has not been reached and if it is greater
				// than zero
				if taskBufSize == 0 {
					// return immediately if there is nothing
					// new to enqueue

					Logger.Warnw(
						"stopping loadtest: NextTask did not return a task",
						"final_task_detla", 0,
					)

					return
				}

				Logger.Debugw("scheduled: stopping loadtest: NextTask did not return a task")

				break
			}

			taskBufSize += n
			taskBuf = taskBuf[:taskBufSize]
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
			// must have hit the end of NextTask iterator
			// increase total by actual number queued
			// and stop traffic generation
			totalNumTasks += taskBufSize
			Logger.Warnw(
				"stopping loadtest: NextTask did not return a task",
				"final_task_detla", taskBufSize,
			)
			return
		}

		taskBuf = taskBuf[:0]

		totalNumTasks += taskBufSize

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
					Lag: lag,
				},
			}
		}
	}

	return nil
}

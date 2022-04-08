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

type LoadtestImpl interface {
	NextTask() Doer
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

	mRetry        sync.Mutex
	retryTasks    []*retryTask
	retryTaskPool sync.Pool

	csvData
}

func NewLoadtest(impl LoadtestImpl, options ...LoadtestOption) *Loadtest {

	opt := loadtestOptions{
		taskBufferingFactor:     4,
		maxWorkers:              1,
		numWorkers:              1,
		maxIntervalTasks:        1,
		numIntervalTasks:        1,
		interval:                time.Second,
		csvOutputFilename:       "metrics.csv",
		csvOutputFlushFrequency: 5 * time.Second,
	}

	for _, op := range options {
		op(&opt)
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

	return &Loadtest{
		impl:          impl,
		maxTotalTasks: opt.maxTotalTasks,
		maxWorkers:    opt.maxWorkers,
		numWorkers:    opt.numWorkers,
		workers:       make([]chan struct{}, 0, opt.maxWorkers),
		taskChan:      make(chan taskWithMeta, maxWorkQueueSize),
		resultsChan:   make(chan taskResult, maxWorkQueueSize+numPossibleLagResults),
		retryTasks:    make([]*retryTask, 0, maxWorkQueueSize),

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
	}
}

var errCsvWriterDisabled = errors.New("csv metrics writer disabled")

type csvData struct {
	outputFilename string
	writer         *csv.Writer
	flushFrequency time.Duration
	flushDeadline  time.Time
	writeErr       error
}

type taskResult struct {
	Panicked    bool
	RetryQueued bool
	Errored     bool
	Elapsed     time.Duration
	Meta        taskMeta
}

type taskMeta struct {
	IntervalID  time.Time
	NumNewTasks int
	Lag         time.Duration `json:",omitempty"`
}

type taskWithMeta struct {
	doer Doer
	meta taskMeta
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

			start := time.Now().UTC()
			errored, retryQueued, panicked := lt.doTask(ctx, workerID, task)
			elapsed := time.Since(start)

			lt.resultsChan <- taskResult{
				Panicked:    panicked,
				RetryQueued: retryQueued,
				Errored:     errored,
				Elapsed:     elapsed,
				Meta:        task.meta,
			}
		}
	}
}

func (lt *Loadtest) doTask(ctx context.Context, workerID int, taskWithMeta taskWithMeta) (errored_resp bool, retryQueued_resp, panicking_resp bool) {
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
			panicking_resp = true
			errored_resp = true

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
		if v, ok := task.(DoRetryer); ok {
			if v, ok := v.(DoRetryChecker); ok && !v.CanRetry(ctx, workerID, err) {
				err_resp = err
				return true, false, false
			}

			if x, ok := v.(*retryTask); ok {
				v = x.DoRetryer
			}
			lt.enqueueRetry(v, err)

			err_resp = err
			return true, true, false
		}

		err_resp = err
		return true, false, false
	}

	return false, false, false
}

func (lt *Loadtest) newRetryTask(task DoRetryer, err error) *retryTask {
	result := lt.retryTaskPool.Get().(*retryTask)

	*result = retryTask{task, err}

	return result
}

func (lt *Loadtest) enqueueRetry(task DoRetryer, err error) {
	lt.mRetry.Lock()
	defer lt.mRetry.Unlock()

	lt.retryTasks = append(lt.retryTasks, lt.newRetryTask(task, err))
}

func (lt *Loadtest) loadRetries(taskBuf []Doer, numTasks int) []Doer {
	lt.mRetry.Lock()
	defer lt.mRetry.Unlock()

	result := taskBuf

	retryTasks := lt.retryTasks

	size := len(retryTasks)

	if size == 0 {
		return result
	}

	if size <= numTasks {
		for i := 0; i < size; i++ {
			result = append(result, retryTasks[i])
		}

		lt.retryTasks = retryTasks[:0]
	} else {
		size = numTasks

		for i := 0; i < size; i++ {
			result = append(result, retryTasks[i])
		}

		copy(retryTasks, retryTasks[size:])
		lt.retryTasks = retryTasks[:len(retryTasks)-size]
	}

	return result
}

func (lt *Loadtest) resultsHandler(wg *sync.WaitGroup, stopChan <-chan struct{}) {
	defer wg.Done()

	var intervalID time.Time
	var resultCount, numNewTasks, numPass, numFail, numRetry, numPanic int
	var lag, minElapsed, maxElapsed, sumElapsed, sumLag time.Duration

	minElapsed = maxDuration

	flushMetrics := func(now time.Time) {
		lt.csvData.writeErr = lt.writeOutputCsvRow(metricRecord{
			intervalID:  intervalID,
			sumLag:      sumLag,
			numNewTasks: numNewTasks,
			lag:         lag,
			numTasks:    resultCount,
			numPass:     numPass,
			numFail:     numFail,
			numRetry:    numRetry,
			numPanic:    numPanic,
			minElapsed:  minElapsed,
			maxElapsed:  maxElapsed,
			sumElapsed:  sumElapsed,
		})

		if !lt.csvData.flushDeadline.After(now) {
			if lt.csvData.writeErr == nil {
				lt.csvData.writer.Flush()
				lt.csvData.writeErr = lt.csvData.writer.Error()
			}
			lt.csvData.flushDeadline = time.Now().UTC().Add(lt.csvData.flushFrequency)
		}
	}

	lt.csvData.flushDeadline = time.Now().UTC().Add(lt.csvData.flushFrequency)

	for {
		var tr taskResult

		select {
		case tr = <-lt.resultsChan:
		case <-stopChan:
			if lt.csvData.writeErr == nil && resultCount > 0 {
				now := time.Now().UTC()
				lt.csvData.flushDeadline = now
				flushMetrics(now)
			}
			return
		}

		lt.resultWaitGroup.Done()

		// Logger.Debugw(
		// 	"got result",
		// 	"result", tr,
		// )

		// TODO: include percentage complete when maxTotalTasks is greater than 0

		if lt.csvData.writeErr != nil {
			continue
		}

		if tr.Meta.IntervalID.IsZero() {

			sumLag += tr.Meta.Lag

			continue
		}

		if intervalID.Before(tr.Meta.IntervalID) {
			intervalID = tr.Meta.IntervalID
			numNewTasks = tr.Meta.NumNewTasks
			lag = tr.Meta.Lag
		}
		if minElapsed > tr.Elapsed {
			minElapsed = tr.Elapsed
		}
		if maxElapsed < tr.Elapsed {
			maxElapsed = tr.Elapsed
		}

		sumElapsed += tr.Elapsed

		// TODO: remove ifs
		if !tr.Errored {
			numPass++
		} else {
			numFail++
		}
		if tr.Panicked {
			numPanic++
		}
		if tr.RetryQueued {
			numRetry++
		}

		resultCount++

		if resultCount >= numNewTasks {

			flushMetrics(time.Now().UTC())

			// reset metrics
			minElapsed = maxDuration
			maxElapsed = -1
			sumElapsed = 0
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

func (lt *Loadtest) writeOutputCsvConfigComment(w io.Writer) error {
	if _, err := w.Write([]byte(`# `)); err != nil {
		return err
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	type Config struct {
		StartTime             string `json:"start_time"`
		Interval              string `json:"interval"`
		MaxIntervalTasks      int    `json:"max_interval_tasks"`
		MaxTotalTasks         int    `json:"max_total_tasks"`
		MaxWorkers            int    `json:"max_workers"`
		NumIntervalTasks      int    `json:"num_interval_tasks"`
		NumWorkers            int    `json:"num_workers"`
		MetricsFlushFrequency string `json:"metrics_flush_frequency"`
	}

	cfg := Config{
		StartTime:             time.Now().UTC().Format(time.RFC3339Nano),
		Interval:              lt.interval.String(),
		MaxIntervalTasks:      lt.maxIntervalTasks,
		MaxTotalTasks:         lt.maxTotalTasks,
		MaxWorkers:            lt.maxWorkers,
		NumIntervalTasks:      lt.numIntervalTasks,
		NumWorkers:            lt.numWorkers,
		MetricsFlushFrequency: lt.csvData.flushFrequency.String(),
	}

	Logger.Infow("writing metics config", "config", cfg)

	err := enc.Encode(struct {
		Config `json:"config"`
	}{cfg})
	if err != nil {
		return err
	}

	if _, err := w.Write([]byte("\n")); err != nil {
		return err
	}

	return nil
}

type metricRecord struct {
	intervalID                         time.Time
	sumLag                             time.Duration
	numNewTasks                        int
	lag                                time.Duration
	numTasks                           int
	numPass                            int
	numFail                            int
	numRetry                           int
	numPanic                           int
	minElapsed, maxElapsed, sumElapsed time.Duration
}

func (lt *Loadtest) writeOutputCsvHeaders() error {
	return lt.csvData.writer.Write([]string{
		"sample_time",
		"interval_id",   // gauge
		"num_new_tasks", // gauge
		"lag",           // gauge
		"sum_lag",
		"num_tasks",
		"num_pass",
		"num_fail",
		"num_retry",
		"num_panic",
		"min_elapsed",
		"avg_elapsed",
		"max_elapsed",
		"sum_elapsed",
	})
}

func (lt *Loadtest) writeOutputCsvRow(mr metricRecord) error {
	if lt.csvData.writeErr != nil {
		return nil
	}

	nowStr := time.Now().UTC().Format(time.RFC3339Nano)

	return lt.csvData.writer.Write([]string{
		nowStr,
		mr.intervalID.Format(time.RFC3339Nano),
		strconv.Itoa(mr.numNewTasks),
		mr.lag.String(),
		mr.sumLag.String(),
		strconv.Itoa(mr.numTasks),
		strconv.Itoa(mr.numPass),
		strconv.Itoa(mr.numFail),
		strconv.Itoa(mr.numRetry),
		strconv.Itoa(mr.numPanic),
		mr.minElapsed.String(),
		(mr.sumElapsed / time.Duration(mr.numTasks)).String(),
		mr.maxElapsed.String(),
		mr.sumElapsed.String(),
	})
}

func (lt *Loadtest) Run(ctx context.Context) (err_result error) {

	if lt.csvData.writeErr == nil {

		csvf, err := os.Create(lt.csvData.outputFilename)
		if err != nil {
			return fmt.Errorf("failed to open output csv metrics file for writing: %w", err)
		}
		defer func() {
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

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	wg.Add(1)
	go lt.resultsHandler(&wg, stopChan)

	numWorkers := lt.numWorkers

	// stopping routine runs on return
	// flushing as much as possible
	defer func() {
		Logger.Debugw("waiting on results to flush")

		// wait for all results to flush through
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

	var numTotalTasks int

	taskBuf := make([]Doer, 0, lt.maxIntervalTasks)

	intervalID := time.Now().UTC()

	maxTotalTasks := lt.maxTotalTasks

	interval := lt.interval
	numIntervalTasks := lt.numIntervalTasks
	numNewTasks := lt.numIntervalTasks
	ctxDone := ctx.Done()
	updatechan := lt.impl.UpdateChan()
	meta := taskMeta{
		NumNewTasks: numNewTasks,
	}
	interTaskInterval := interval / time.Duration(numIntervalTasks)
	var delay time.Duration
	for {
		if maxTotalTasks > 0 {
			if numTotalTasks >= maxTotalTasks {
				return
			}

			numNewTasks = maxTotalTasks - numTotalTasks
			if numNewTasks > numIntervalTasks {
				numNewTasks = numIntervalTasks
			}

			meta.NumNewTasks = numNewTasks
		}

		select {
		case <-ctxDone:
			return
		case cu := <-updatechan:
			for {
				var recomputeInterTaskInterval bool

				if cu.numWorkers.set {
					n := cu.numWorkers.val

					// prevent over commiting on the maxWorkers count
					if n > lt.maxWorkers {
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

					numWorkers = n
				}

				if cu.numIntervalTasks.set {
					recomputeInterTaskInterval = true

					n := cu.numIntervalTasks.val

					// prevent over commiting on the maxIntervalTasks count
					// note this upper bound may be removed as it is more of a hint
					if n > lt.maxIntervalTasks {
						n = lt.maxIntervalTasks
					}

					numIntervalTasks = n
				}

				if cu.interval.set {
					recomputeInterTaskInterval = true

					interval = cu.interval.val
				}

				if recomputeInterTaskInterval {
					interTaskInterval = interval / time.Duration(numIntervalTasks)
				}

				// TODO: log configuration updates when they occur

				// pause load generation if unable to schedule anything
				if numIntervalTasks <= 0 || numWorkers <= 0 {
					select {
					case <-ctxDone:
						return
					case cu = <-updatechan:
						continue
					}
				}

				// continue with load generation
				break
			}
		default:
			// continue with load generation
		}

		// read up to numNewTasks from retry slice
		taskBuf = lt.loadRetries(taskBuf, numNewTasks)

		for i := len(taskBuf); i < numNewTasks; i++ {
			taskBuf = append(taskBuf, lt.impl.NextTask())
		}

		lt.resultWaitGroup.Add(numNewTasks)

		meta.IntervalID = intervalID

		if numNewTasks <= 1 || interTaskInterval <= skipInterTaskSchedulingThreshold {

			for _, task := range taskBuf {
				lt.taskChan <- taskWithMeta{task, meta}
			}
		} else {

			lt.taskChan <- taskWithMeta{taskBuf[0], meta}

			for _, task := range taskBuf[1:] {
				time.Sleep(interTaskInterval)
				lt.taskChan <- taskWithMeta{task, meta}
			}
		}

		taskBuf = taskBuf[:0]

		numTotalTasks += numNewTasks

		meta.Lag = 0

		// wait for next interval time to exist
		nextIntervalID := intervalID.Add(interval)
		realNow := time.Now().UTC()
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
}

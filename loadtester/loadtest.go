package loadtester

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

// TODO: RetriesDisabled runtimes checks can turn into init time checks; same with MaxTasks based checks
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
	ReadTasks([]Doer) int
	// UpdateConfigChan should return the same channel each time or nil;
	// but once nil it must never be non-nil again
	UpdateConfigChan() <-chan ConfigUpdate
}

type csvData struct {
	outputFilename string
	writer         *csv.Writer
	flushFrequency time.Duration
	flushDeadline  time.Time
	writeErr       error
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

	// intervalTasksSema will always have the capacity of double the task interval rate
	// but it is created with the maximum weight to allow up two batches of maxIntervalTasks
	// to be spawned ( one batch in progress, one batch pending in channel queue )
	//
	// This addition prevents enqueuing more than twice the count of desired tasks per interval
	// and creates negative pressure on the task enqueue routine since the max channel sizes
	// are static and bound to the maximums we could reach in some pacer implementation.
	//
	// Meaning we could send far more than we intended for the wall-clock interval and it
	// could cause severe bursts in load without this protection.
	//
	// As a result this does slow down each task runner, but only slightly as it's just acquiring
	// a lock, doing some simple math, and then unlocking as the task worker calls release on
	// the sempahore. It's worth it to me.
	intervalTasksSema *semaphore.Weighted

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

	logger SugaredLogger
}

func NewLoadtest(taskProvider TaskProvider, options ...LoadtestOption) (*Loadtest, error) {

	cfg := loadtestConfig{
		outputBufferingFactor:   4,
		maxWorkers:              1,
		numWorkers:              1,
		maxIntervalTasks:        1,
		numIntervalTasks:        1,
		interval:                time.Second,
		csvOutputFilename:       "metrics.csv",
		csvOutputFlushFrequency: 5 * time.Second,
		flushRetriesTimeout:     2 * time.Minute,
	}

	for _, option := range options {
		option(&cfg)
	}

	if !cfg.maxWorkersSet && cfg.numWorkersSet {
		cfg.maxWorkers = cfg.numWorkers
	}

	if !cfg.maxIntervalTasksSet && cfg.numIntervalTasksSet {
		cfg.maxIntervalTasks = cfg.numIntervalTasks
	}

	if cfg.maxWorkers < cfg.numWorkers {
		return nil, errors.New("loadtest misconfigured: MaxWorkers < NumWorkers")
	}

	if cfg.maxWorkers < 1 {
		return nil, errors.New("loadtest misconfigured: MaxWorkers < 1")
	}

	if cfg.maxIntervalTasks < cfg.numIntervalTasks {
		return nil, errors.New("loadtest misconfigured: MaxIntervalTasks < NumIntervalTasks")
	}

	if cfg.maxIntervalTasks < 1 {
		return nil, errors.New("loadtest misconfigured: maxIntervalTasks < 1")
	}

	if cfg.outputBufferingFactor <= 0 {
		cfg.outputBufferingFactor = 1
	}

	if cfg.logger == nil {
		logger, err := NewLogger(zap.InfoLevel)
		if err != nil {
			return nil, fmt.Errorf("failed to create a default logger: %w", err)
		}
		cfg.logger = logger
	}

	// lag results are reported per interval through the same channel as results
	// so it's important to account for them per interval when constructing max
	// config buffers

	const intervalPossibleLagResultCount = 1
	var resultsChanSize int
	{
		resultsChanSize = (cfg.maxIntervalTasks + intervalPossibleLagResultCount) * cfg.outputBufferingFactor
	}

	var csvWriteErr error
	if cfg.csvOutputDisabled {
		csvWriteErr = errCsvWriterDisabled
	}

	var retryTaskChan chan *retryTask
	if !cfg.retriesDisabled {
		retryTaskChan = make(chan *retryTask, cfg.maxIntervalTasks)
	}

	var sm *semaphore.Weighted
	{
		size := int64(cfg.maxIntervalTasks) * 2
		sm = semaphore.NewWeighted(size)
		if !sm.TryAcquire(size) {
			return nil, errors.New("failed to initialize load generation semaphore")
		}
		sm.Release(int64(cfg.numIntervalTasks) * 2)
	}

	return &Loadtest{
		taskProvider:  taskProvider,
		maxTasks:      cfg.maxTasks,
		maxWorkers:    cfg.maxWorkers,
		numWorkers:    cfg.numWorkers,
		workers:       make([]chan struct{}, 0, cfg.maxWorkers),
		taskChan:      make(chan taskWithMeta, cfg.maxIntervalTasks+intervalPossibleLagResultCount),
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
			flushFrequency: cfg.csvOutputFlushFrequency,
			writeErr:       csvWriteErr,
		},

		flushRetriesTimeout:    cfg.flushRetriesTimeout,
		flushRetriesOnShutdown: cfg.flushRetriesOnShutdown,
		RetriesDisabled:        cfg.retriesDisabled,
		logger:                 cfg.logger,
		intervalTasksSema:      sm,
	}, nil
}

var (
	errCsvWriterDisabled    = errors.New("csv metrics writer disabled")
	ErrBadReadTasksImpl     = errors.New("bad ReadTasks implementation: returned a value less than zero or larger than the input slice length")
	ErrRetriesFailedToFlush = errors.New("failed to flush all retries")
)

func timeToString(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
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
		var task taskWithMeta

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

		lt.intervalTasksSema.Release(1)
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
			lt.logger.Warnw(
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
				lt.logger.Errorw(
					"worker recovered from panic",
					"worker_id", workerID,
					"error", v,
				)
			case []byte:
				lt.logger.Errorw(
					"worker recovered from panic",
					"worker_id", workerID,
					"error", string(v),
				)
			case string:
				lt.logger.Errorw(
					"worker recovered from panic",
					"worker_id", workerID,
					"error", v,
				)
			default:
				const msg = "unknown cause"

				lt.logger.Errorw(
					"worker recovered from panic",
					"worker_id", workerID,
					"error", msg,
				)
			}
		}
	}()

	err := task.Do(ctx, workerID)
	if err == nil {
		success_resp = 1
		return
	}

	err_resp = err
	errored_resp = 1

	if lt.RetriesDisabled {
		return
	}

	dr, ok := task.(DoRetryer)
	if !ok {
		return
	}

	if v, ok := dr.(DoRetryChecker); ok && !v.CanRetry(ctx, workerID, err) {
		return
	}

	if x, ok := dr.(*retryTask); ok {
		dr = x.DoRetryer
	}

	lt.retryTaskChan <- lt.newRetryTask(dr, err)

	retryQueued_resp = 1

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

func (lt *Loadtest) resultsHandler(wg *sync.WaitGroup, stopChan <-chan struct{}) {
	defer wg.Done()

	var intervalID time.Time
	var totalNumTasks, numTasks, numIntervalTasks, numPass, numFail, numRetry, numPanic int
	var lag, minQueuedDuration, maxQueuedDuration, sumQueuedDuration, minTaskDuration, maxTaskDuration, sumTaskDuration, sumLag time.Duration

	minQueuedDuration = maxDuration
	minTaskDuration = maxDuration

	writeRow := func() {
		totalNumTasks += numTasks
		lt.csvData.writeErr = lt.writeOutputCsvRow(metricRecord{
			totalNumTasks:     totalNumTasks,
			intervalID:        intervalID,
			sumLag:            sumLag,
			numIntervalTasks:  numIntervalTasks,
			lag:               lag,
			numTasks:          numTasks,
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
			if lt.csvData.writeErr == nil && numTasks > 0 {
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

		numTasks++

		if numTasks >= numIntervalTasks {

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
			numTasks = 0
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
		MaxTasks               int    `json:"max_tasks"`
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
		MaxTasks:               lt.maxTasks,
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
	totalNumTasks                                           int
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

	if lt.maxTasks > 0 {
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

	if lt.maxTasks > 0 {
		high := mr.totalNumTasks * 10000 / lt.maxTasks
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

// HttpTransport returns a new configured *http.Transport
// which implements http.RoundTripper that can be used in
// tasks which have http clients
//
// Note, you may need to increase the value of MaxIdleConns
// if your tasks target multiple hosts. MaxIdleConnsPerHost
// does not override the limit established by MaxIdleConns
// and if the tasks are expected to communicate to multiple
// hosts you probably need to apply some scaling factor to
// it to let connections go idle for a time and still be reusable.
//
// Note that if you are not connecting to a loadbalancer
// which preserves connections to a client much of the intent
// we're trying to establish here is not applicable.
//
// Also if the loadbalancer does not have "max connection lifespan"
// behavior nor a "round robin" or "connection balancing" feature
// without forcing the loadtesting client to reconnect then as
// you increase load your established connections may prevent the
// spread of load to newly scaled-out recipients of that load.
//
// By default golang's http standard lib does not expose a way for us
// to attempt to address this. The problem is also worse if your
// loadbalancer ( or number of exposed ips for a dns record ) increase.
//
// Rectifying this issue requires a fix like/option like
// https://github.com/golang/go/pull/46714
// to be accepted by the go maintainers.
func (lt *Loadtest) NewHttpTransport() *http.Transport {

	// adding runtime CPU count to the max
	// just to ensure whenever one worker releases
	// a connection back to the pool we're not impacted
	// by the delay of that connection getting re-pooled
	maxConnections := lt.maxWorkers + runtime.NumCPU()

	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 15 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          maxConnections,
		IdleConnTimeout:       20 * time.Second, // default was 90
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   maxConnections,
		MaxConnsPerHost:       maxConnections,
	}
}

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

	lt.logger.Infow(
		"starting loadtest",
		"config", lt.getLoadtestConfigAsJson(),
	)

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	wg.Add(1)
	go lt.resultsHandler(&wg, stopChan)

	numWorkers := lt.numWorkers

	// numTasks is the total number of tasks
	// scheduled to run ( including retries )
	var numTasks int

	intervalID := time.Now()

	maxTasks := lt.maxTasks

	interval := lt.interval
	numNewTasks := lt.numIntervalTasks
	ctxDone := ctx.Done()
	updatechan := lt.taskProvider.UpdateConfigChan()
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

				lt.logger.Debugw(
					"waiting on results to flush",
					"num_tasks", numTasks,
				)

				return nil
			}

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

				if shutdownCtx.Err() != nil {
					lt.logger.Errorw(
						"failed to flush all retries",
						"preflush_num_tasks", preflushNumTasks,
						"num_tasks", numTasks,
					)

					return ErrRetriesFailedToFlush
				}

				lt.resultWaitGroup.Wait()

				for {

					if shutdownCtx.Err() != nil {
						lt.logger.Errorw(
							"failed to flush all retries",
							"preflush_num_tasks", preflushNumTasks,
							"num_tasks", numTasks,
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

					// acquire load generation opportunity slots ( smooths bursts )
					if err := lt.intervalTasksSema.Acquire(shutdownCtx, int64(taskBufSize)); err != nil {
						lt.logger.Errorw(
							"failed to flush all retries",
							"preflush_num_tasks", preflushNumTasks,
							"num_tasks", numTasks,
							"error", err,
							"reason", "shutdown timeout likely reached while waiting for semaphore acquisition",
						)
						return ErrRetriesFailedToFlush
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

		lt.logger.Debugw("stopping result handler routines")

		// signal for handler routines to stop
		close(stopChan)

		lt.logger.Debugw("waiting for result handler routines to stop")

		// wait for handler routines to stop
		wg.Wait()

		lt.logger.Debugw("stopping workers")

		// signal for workers to stop
		for i := 0; i < len(lt.workers); i++ {
			close(lt.workers[i])
		}

		lt.logger.Debugw("waiting for workers to stop")

		// wait for workers to stop
		lt.workerWaitGroup.Wait()
	}()

	for i := 0; i < numWorkers; i++ {
		lt.addWorker(ctx, i)
	}

	for {
		if maxTasks > 0 {
			if numTasks >= maxTasks {
				lt.logger.Warnw(
					"loadtest finished: max task count reached",
					"max_tasks", maxTasks,
				)
				return
			}

			numNewTasks = maxTasks - numTasks
			if numNewTasks > meta.NumIntervalTasks {
				numNewTasks = meta.NumIntervalTasks
			}
		}

		select {
		case <-ctxDone:
			return
		case cu := <-updatechan:
			var prepSemaErr error
			var recomputeInterTaskInterval bool

			if cu.numWorkers.set {
				n := cu.numWorkers.val

				// prevent over commiting on the maxWorkers count
				if n > lt.maxWorkers {
					lt.logger.Errorw(
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
					lt.logger.Errorw(
						"config update not within loadtest boundary conditions: numIntervalTasks",
						"reason", "update tried to set numIntervalTasks set too high",
						"remediation_hint", "increase the loadtests's MaxIntervalTasks setting",
						"remediation_taken", "using max value",
						"requested", n,
						"max", lt.maxIntervalTasks,
					)
					n = lt.maxIntervalTasks
				}

				if n > numNewTasks {
					lt.intervalTasksSema.Release(int64(n-numNewTasks) * 2)
				} else if n < numNewTasks {
					prepSemaErr = lt.intervalTasksSema.Acquire(ctx, int64(numNewTasks-n)*2)
					if prepSemaErr != nil {
						lt.logger.Errorw(
							"loadtest config udpate: failed to pre-acquire load generation slots",
							"error", prepSemaErr,
						)

						// not returning and error... yet
						// going to let config update log statement occur and then report the error present in prepSemaErr
					}
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

			// && clause: protects against divide by zero
			if recomputeInterTaskInterval && meta.NumIntervalTasks >= 0 {
				interTaskInterval = interval / time.Duration(meta.NumIntervalTasks)
			}

			lt.logger.Warnw(
				"loadtest config updated",
				configChanges...,
			)
			configChanges = configChanges[:0]

			if prepSemaErr != nil {
				return prepSemaErr
			}

			// pause load generation if unable to schedule anything
			if meta.NumIntervalTasks <= 0 || numWorkers <= 0 {

				pauseStart := time.Now()

				lt.logger.Warnw(
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

					lt.logger.Warnw(
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
						"stopping loadtest: NextTask did not return a task",
						"final_task_delta", 0,
					)

					return
				}

				lt.logger.Debugw("scheduled: stopping loadtest: NextTask did not return a task")

				break
			}

			taskBufSize += n
			taskBuf = taskBuf[:taskBufSize]
		}

		// acquire load generation opportunity slots ( smooths bursts )
		if lt.intervalTasksSema.Acquire(ctx, int64(taskBufSize)) != nil {
			return
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
			// increase numTasks total by actual number queued
			// and stop traffic generation
			numTasks += taskBufSize
			lt.logger.Warnw(
				"stopping loadtest: NextTask did not return a task",
				"final_task_delta", taskBufSize,
			)
			return
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
					Lag: lag,
				},
			}
		}
	}

	return nil
}

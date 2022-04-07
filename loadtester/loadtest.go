package loadtester

import (
	"context"
	"fmt"
	"sync"
	"time"

	pkgerrors "github.com/pkg/errors"
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
}

func NewLoadtest(impl LoadtestImpl, options ...LoadtestOption) *Loadtest {

	opts := loadtestOptions{
		taskBufferingFactor: 4,
		maxWorkers:          1,
		numWorkers:          1,
		maxIntervalTasks:    1,
		numIntervalTasks:    1,
		interval:            time.Second,
	}

	for _, op := range options {
		op(&opts)
	}

	if opts.taskBufferingFactor <= 0 {
		opts.taskBufferingFactor = 1
	}

	maxWorkQueueSize := opts.maxWorkers * opts.taskBufferingFactor

	return &Loadtest{
		impl:          impl,
		maxTotalTasks: opts.maxTotalTasks,
		maxWorkers:    opts.maxWorkers,
		numWorkers:    opts.numWorkers,
		workers:       make([]chan struct{}, 0, opts.maxWorkers),
		taskChan:      make(chan taskWithMeta, maxWorkQueueSize),
		resultsChan:   make(chan taskResult, maxWorkQueueSize),
		retryTasks:    make([]*retryTask, 0, maxWorkQueueSize),

		maxIntervalTasks: opts.maxIntervalTasks,
		numIntervalTasks: opts.numIntervalTasks,
		interval:         opts.interval,
		retryTaskPool: sync.Pool{
			New: func() interface{} {
				return &retryTask{}
			},
		},
	}
}

type taskResult struct {
	Panicked    bool  `json:",omitempty"`
	RetryQueued bool  `json:",omitempty"`
	Err         error `json:",omitempty"`
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
			err, retryQueued, panicked := lt.doTask(ctx, workerID, task)
			elapsed := time.Since(start)

			lt.resultsChan <- taskResult{
				Panicked:    panicked,
				RetryQueued: retryQueued,
				Err:         err,
				Elapsed:     elapsed,
				Meta:        task.meta,
			}
		}
	}
}

func (lt *Loadtest) doTask(ctx context.Context, workerID int, taskWithMeta taskWithMeta) (err_resp error, retryQueued_resp, panicking_resp bool) {
	task := taskWithMeta.doer
	defer func() {
		if v, ok := task.(*retryTask); ok {
			*v = retryTask{}
			lt.retryTaskPool.Put(v)
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			panicking_resp = true

			switch v := r.(type) {
			case error:
				Logger.Errorw(
					"worker recovered from panic",
					"worker_id", workerID,
					"error", v,
				)

				if _, ok := v.(fmt.Formatter); ok {
					err_resp = v
				} else {
					err_resp = pkgerrors.WithStack(v)
				}
			case []byte:
				Logger.Errorw(
					"worker recovered from panic",
					"worker_id", workerID,
					"error", string(v),
				)

				err_resp = pkgerrors.New(string(v))
			case string:
				Logger.Errorw(
					"worker recovered from panic",
					"worker_id", workerID,
					"error", v,
				)

				err_resp = pkgerrors.New(v)
			default:
				const msg = "unknown cause"

				Logger.Errorw(
					"worker recovered from panic",
					"worker_id", workerID,
					"error", msg,
				)

				err_resp = pkgerrors.New(msg)
			}
		}
	}()

	err := task.Do(ctx, workerID)
	if err != nil {
		if v, ok := task.(DoRetryer); ok {
			if x, ok := v.(*retryTask); ok {
				v = x.DoRetryer
			}
			lt.enqueueRetry(v, err)
			return err, true, false
		}

		return err, false, false
	}

	return nil, false, false
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

	for {
		var tr taskResult

		select {
		case tr = <-lt.resultsChan:
		case <-stopChan:
			return
		}

		lt.resultWaitGroup.Done()

		Logger.Debugw(
			"got result",
			"result", tr,
		)
	}
}

const (
	skipInterTaskSchedulingThreshold = 20 * time.Millisecond
)

func (lt *Loadtest) Run(ctx context.Context) {

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

		if delay < 0 {
			meta.Lag = -delay
		}

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
			intervalID = realNow
			meta.Lag = -delay
		}
	}
}

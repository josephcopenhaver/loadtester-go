package loadtester

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	maxDuration = time.Duration((^uint64(0)) >> 1)
)

type csvData struct {
	outputFilename string
	writer         *csv.Writer
	flushInterval  time.Duration
	flushDeadline  time.Time

	writeRWM sync.RWMutex
	writeErr error
}

func (cd *csvData) err() error {
	cd.writeRWM.RLock()
	defer cd.writeRWM.RUnlock()

	return cd.writeErr
}

func (cd *csvData) setErr(err error) {
	cd.writeRWM.Lock()
	defer cd.writeRWM.Unlock()

	if cd.writeErr != nil {
		return
	}

	cd.writeErr = err
}

func (lt *Loadtest) writeOutputCsvConfigComment(w io.Writer) error {

	if _, err := w.Write([]byte(`# `)); err != nil {
		return err
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	err := enc.Encode(struct {
		C any `json:"config"`
	}{lt.loadtestConfigAsJson()})
	if err != nil {
		return err
	}

	if _, err := w.Write([]byte("\n")); err != nil {
		return err
	}

	return nil
}

type metricRecordResetables struct {
	numTasks                                                int
	numPass                                                 int
	numFail                                                 int
	numRetry                                                int
	numPanic                                                int
	sumLag                                                  time.Duration
	lag                                                     time.Duration
	minTaskDuration, maxTaskDuration, sumTaskDuration       time.Duration
	minQueuedDuration, maxQueuedDuration, sumQueuedDuration time.Duration
}

type metricRecord struct {
	// fields that are preserved
	intervalID       time.Time
	numIntervalTasks int
	// totalNumTasks is only modified if the loadtest's maxTasks setting is > 0
	totalNumTasks int

	metricRecordResetables
}

func (mr *metricRecord) reset() {
	mr.metricRecordResetables = metricRecordResetables{
		minTaskDuration:   maxDuration,
		minQueuedDuration: maxDuration,
	}
}

func (lt *Loadtest) writeOutputCsvHeaders() error {

	cd := &lt.csvData

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
		"",
	}

	if lt.maxTasks > 0 {
		fields[len(fields)-1] = "percent_done"
	} else {
		fields = fields[:len(fields)-1]
	}

	err := cd.writer.Write(fields)
	if err != nil {
		return err
	}

	// ensure headers flush asap
	cd.writer.Flush()

	return cd.writer.Error()
}

// writeOutputCsvRow_maxTasksGTZero writes the metric record to the target csv file when maxTasks is > 0
func (lt *Loadtest) writeOutputCsvRow_maxTasksGTZero(mr metricRecord) {

	cd := &lt.csvData
	if cd.writeErr != nil {
		return
	}

	nowStr := timeToString(time.Now())

	var percent string
	{
		high := mr.totalNumTasks * 10000 / lt.maxTasks
		low := high % 100
		high /= 100

		var sep string
		if low < 10 {
			sep = ".0"
		} else {
			sep = "."
		}

		percent = strconv.Itoa(high) + sep + strconv.Itoa(low)
	}

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
		percent,
	}

	if err := cd.writer.Write(fields); err != nil {
		cd.setErr(err) // sets error state in multiple goroutine safe way
	}
}

// writeOutputCsvRow_maxTasksNotGTZero writes the metric record to the target csv file when maxTasks is <= 0
func (lt *Loadtest) writeOutputCsvRow_maxTasksNotGTZero(mr metricRecord) {

	cd := &lt.csvData
	if cd.writeErr != nil {
		return
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
	}

	if err := cd.writer.Write(fields); err != nil {
		cd.setErr(err) // sets error state in multiple goroutine safe way
	}
}

func (lt *Loadtest) writeOutputCsvFooterAndClose(csvFile *os.File) {

	cd := &lt.csvData
	cd.writeErr = cd.err() // read error state after other goroutines have settled ( guaranteed )

	defer func() {
		if err := csvFile.Close(); err != nil && cd.writeErr == nil {
			cd.writeErr = err
		}
	}()

	if cd.writeErr != nil {
		return
	}

	if cd.writer == nil {
		return
	}

	cd.writer.Flush()

	cd.writeErr = cd.writer.Error()
	if cd.writeErr != nil {
		return
	}

	_, cd.writeErr = csvFile.Write([]byte("\n# {\"done\":{\"end_time\":\"" + timeToString(time.Now()) + "\"}}\n"))
}

func (lt *Loadtest) resultsHandler() {

	cd := &lt.csvData
	var mr metricRecord
	mr.reset()

	var writeRow func()
	if lt.maxTasks > 0 {
		writeRow = func() {
			mr.totalNumTasks += mr.numTasks
			lt.writeOutputCsvRow(mr)
		}
	} else {
		writeRow = func() {
			lt.writeOutputCsvRow(mr)
		}
	}

	cd.flushDeadline = time.Now().Add(cd.flushInterval)

	for {
		tr, ok := <-lt.resultsChan
		if !ok {
			if cd.writeErr == nil && mr.numTasks > 0 {
				writeRow()
			}
			return
		}

		lt.resultWaitGroup.Done()

		if cd.writeErr != nil {
			continue
		}

		if tr.taskResultFlags.isZero() {

			mr.sumLag += tr.Meta.Lag

			continue
		}

		if mr.intervalID.Before(tr.Meta.IntervalID) {
			mr.intervalID = tr.Meta.IntervalID
			mr.numIntervalTasks = tr.Meta.NumIntervalTasks
			mr.lag = tr.Meta.Lag
		}

		if mr.minQueuedDuration > tr.QueuedDuration {
			mr.minQueuedDuration = tr.QueuedDuration
		}

		if mr.minTaskDuration > tr.TaskDuration {
			mr.minTaskDuration = tr.TaskDuration
		}

		if mr.maxTaskDuration < tr.TaskDuration {
			mr.maxTaskDuration = tr.TaskDuration
		}

		if mr.maxQueuedDuration < tr.QueuedDuration {
			mr.maxQueuedDuration = tr.QueuedDuration
		}

		mr.sumQueuedDuration += tr.QueuedDuration
		mr.sumTaskDuration += tr.TaskDuration
		mr.numPass += int(tr.Passed)
		mr.numFail += int(tr.Errored)
		mr.numPanic += int(tr.Panicked)
		mr.numRetry += int(tr.RetryQueued)

		mr.numTasks++

		if mr.numTasks >= mr.numIntervalTasks {

			writeRow()
			mr.reset()

			if cd.writeErr == nil && !cd.flushDeadline.After(time.Now()) {
				cd.writer.Flush()
				if err := cd.writer.Error(); err != nil {
					cd.setErr(err) // sets error state in multiple goroutine safe way
				}
				cd.flushDeadline = time.Now().Add(cd.flushInterval)
			}
		}
	}
}

//
// helpers
//

func timeToString(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

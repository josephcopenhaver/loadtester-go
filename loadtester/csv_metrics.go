package loadtester

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"time"
)

const (
	maxDuration = time.Duration((^uint64(0)) >> 1)
)

var (
	errCsvWriterDisabled = errors.New("csv metrics writer disabled")
)

type csvData struct {
	outputFilename string
	writer         *csv.Writer
	flushInterval  time.Duration
	flushDeadline  time.Time
	writeErr       error
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
	totalNumTasks    int

	metricRecordResetables
}

func (mr *metricRecord) reset() {
	mr.metricRecordResetables = metricRecordResetables{
		minTaskDuration:   maxDuration,
		minQueuedDuration: maxDuration,
	}
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

// writeOutputCsvRow writes the metric record to the target csv file
func (lt *Loadtest) writeOutputCsvRow(mr metricRecord) {
	if lt.csvData.writeErr != nil {
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

	lt.csvData.writeErr = lt.csvData.writer.Write(fields)
}

func (lt *Loadtest) writeOutputCsvFooterAndClose(csvFile *os.File) {
	defer func() {
		if err := csvFile.Close(); err != nil {
			if lt.csvData.writeErr == nil {
				lt.csvData.writeErr = err
			}
		}
	}()

	if lt.csvData.writeErr == nil {
		lt.csvData.writer.Flush()
		lt.csvData.writeErr = lt.csvData.writer.Error()
		if lt.csvData.writeErr == nil {
			_, lt.csvData.writeErr = csvFile.Write([]byte("\n# {\"done\":{\"end_time\":\"" + timeToString(time.Now()) + "\"}}\n"))
		}
	}
}

func (lt *Loadtest) resultsHandler() {
	var mr metricRecord
	mr.reset()

	writeRow := func() {
		mr.totalNumTasks += mr.numTasks
		lt.writeOutputCsvRow(mr)
	}

	lt.csvData.flushDeadline = time.Now().Add(lt.csvData.flushInterval)

	for {
		tr, ok := <-lt.resultsChan
		if !ok {
			if lt.csvData.writeErr == nil && mr.numTasks > 0 {
				writeRow()
			}
			return
		}

		lt.resultWaitGroup.Done()

		if lt.csvData.writeErr != nil {
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

			if lt.csvData.writeErr == nil && !lt.csvData.flushDeadline.After(time.Now()) {
				lt.csvData.writer.Flush()
				lt.csvData.writeErr = lt.csvData.writer.Error()
				lt.csvData.flushDeadline = time.Now().Add(lt.csvData.flushInterval)
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

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

	queuedDurations *latencyList
	taskDurations   *latencyList
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
		"",
		"",
	}

	if lt.latencyPercentile != 0 {
		fields[len(fields)-3] = "p" + strconv.Itoa(int(lt.latencyPercentile)) + "_queued_duration"
		fields[len(fields)-2] = "p" + strconv.Itoa(int(lt.latencyPercentile)) + "_task_duration"
	} else {
		fields = fields[:len(fields)-2]
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

//
// helpers
//

func timeToString(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

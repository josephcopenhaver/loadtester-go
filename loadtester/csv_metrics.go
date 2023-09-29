package loadtester

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"math"
	"math/big"
	"os"
	"sync"
	"time"
)

const (
	maxCsvNumColumns = 37
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
	numTasks                           int
	numPass                            int
	numFail                            int
	numRetry                           int
	numPanic                           int
	sumLag                             time.Duration
	lag                                time.Duration
	minTaskDuration, maxTaskDuration   time.Duration
	minQueueDuration, maxQueueDuration time.Duration
}

type metricRecord struct {
	// fields that are preserved
	intervalID       time.Time
	numIntervalTasks int
	// totalNumTasks is only modified if the loadtest's maxTasks setting is > 0
	totalNumTasks int

	sumTaskDuration, sumQueueDuration big.Int

	metricRecordResetables
}

func (mr *metricRecord) reset() {
	mr.sumTaskDuration.SetUint64(0)
	mr.sumQueueDuration.SetUint64(0)
	mr.metricRecordResetables = metricRecordResetables{
		minTaskDuration:  math.MaxInt64,
		minQueueDuration: math.MaxInt64,
	}
}

func (lt *Loadtest) writeOutputCsvHeaders() error {

	cd := &lt.csvData

	fields := append(make([]string, 0, maxCsvNumColumns), []string{
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
		"min_queue_latency",
		"avg_queue_latency",
		"max_queue_latency",
		"min_task_latency",
		"avg_task_latency",
		"max_task_latency",
	}...)

	if lt.latencies != nil {
		fields = append(fields, []string{
			"p25_queue_latency",
			"p50_queue_latency",
			"p75_queue_latency",
			"p80_queue_latency",
			"p85_queue_latency",
			"p90_queue_latency",
			"p95_queue_latency",
			"p99_queue_latency",
			"p99p9_queue_latency",
			"p99p99_queue_latency",
			"p25_task_latency",
			"p50_task_latency",
			"p75_task_latency",
			"p80_task_latency",
			"p85_task_latency",
			"p90_task_latency",
			"p95_task_latency",
			"p99_task_latency",
			"p99p9_task_latency",
			"p99p99_task_latency",
		}...)
	}

	if lt.maxTasks > 0 {
		fields = append(fields, "percent_done")
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

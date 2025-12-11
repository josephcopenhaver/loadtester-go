package loadtester

import (
	"bufio"
	"encoding/json"
	"io"
	"math"
	"os"
	"sync"
	"time"

	"github.com/josephcopenhaver/csv-go/v3"
)

const (
	maxCsvNumColumns = 39
)

type csvData struct {
	outputFilename string
	bufWriter      *bufio.Writer
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

	return nil
}

func (lt *Loadtest) writeOutputCsvHeaders() error {

	cd := &lt.csvData

	fields := append(make([]string, 0, maxCsvNumColumns),
		"sample_time",
		"interval_id",        // gauge
		"num_interval_tasks", // gauge
		"lag",                // gauge
		"sum_lag",
		"num_tasks",
		"num_pass",
		"num_fail",
	)

	if lt.retry {
		fields = append(fields,
			"num_retry",
		)
	}

	fields = append(fields,
		"num_panic",
		"min_queue_latency",
		"avg_queue_latency",
		"max_queue_latency",
		"min_task_latency",
		"avg_task_latency",
		"max_task_latency",
	)

	if lt.percentilesEnabled {
		fields = append(fields,
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
		)
	}

	if lt.variancesEnabled {
		fields = append(fields,
			"queue_latency_variance",
			"task_latency_variance",
		)
	}

	if lt.maxTasks > 0 {
		fields = append(fields,
			"percent_done",
		)
	}

	_, err := cd.writer.WriteHeader(
		csv.WriteHeaderOpts().Headers(fields...),
	)
	if err != nil {
		return err
	}

	// ensure headers flush asap
	return cd.bufWriter.Flush()
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

	cd.writeErr = cd.bufWriter.Flush()
	if cd.writeErr != nil {
		return
	}

	_, cd.writeErr = csvFile.Write([]byte("# {\"done\":{\"end_time\":\"" + timeToString(time.Now()) + "\"}}"))
}

//
// helpers
//

func timeToString(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func csvFmtVariance(rw *csv.RecordWriter, f float64) {
	f = math.Round(f)

	if math.IsNaN(f) {
		rw.String("")
		return
	}

	v := int64(math.MaxInt64)
	if !math.IsInf(f, 1) {
		if n := int64(f); n >= 0 {
			v = n
		}
	}

	rw.Int64(v)
}

func csvFmtLatency(rw *csv.RecordWriter, n latency) {
	if n < 0 {
		rw.String("")
		return
	}

	rw.Int64(int64(n))
}

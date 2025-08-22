package loadtester

import (
	"bufio"
	"encoding/json/v2"
	"io"
	"os"
	"sync"
	"time"

	"github.com/josephcopenhaver/loadtester-go/v5/loadtester/internal/csv"
)

const (
	maxCsvNumColumns = 39
)

type csvData struct {
	outputFilename string
	writer         *bufio.Writer
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

	{
		err := json.MarshalWrite(w,
			struct {
				C any `json:"config"`
			}{lt.loadtestConfigAsJson()},
		)
		if err != nil {
			return err
		}
	}

	if _, err := w.Write([]byte("\n")); err != nil {
		return err
	}

	return nil
}

func (lt *Loadtest) writeOutputCsvHeaders() error {

	cd := &lt.csvData

	fields := append(make([]csv.Field, 0, maxCsvNumColumns),
		csv.Str("sample_time"),
		csv.Str("interval_id"),        // gauge
		csv.Str("num_interval_tasks"), // gauge
		csv.Str("lag"),                // gauge
		csv.Str("sum_lag"),
		csv.Str("num_tasks"),
		csv.Str("num_pass"),
		csv.Str("num_fail"),
	)

	if lt.retry {
		fields = append(fields,
			csv.Str("num_retry"),
		)
	}

	fields = append(fields,
		csv.Str("num_panic"),
		csv.Str("min_queue_latency"),
		csv.Str("avg_queue_latency"),
		csv.Str("max_queue_latency"),
		csv.Str("min_task_latency"),
		csv.Str("avg_task_latency"),
		csv.Str("max_task_latency"),
	)

	if lt.percentilesEnabled {
		fields = append(fields,
			csv.Str("p25_queue_latency"),
			csv.Str("p50_queue_latency"),
			csv.Str("p75_queue_latency"),
			csv.Str("p80_queue_latency"),
			csv.Str("p85_queue_latency"),
			csv.Str("p90_queue_latency"),
			csv.Str("p95_queue_latency"),
			csv.Str("p99_queue_latency"),
			csv.Str("p99p9_queue_latency"),
			csv.Str("p99p99_queue_latency"),
			csv.Str("p25_task_latency"),
			csv.Str("p50_task_latency"),
			csv.Str("p75_task_latency"),
			csv.Str("p80_task_latency"),
			csv.Str("p85_task_latency"),
			csv.Str("p90_task_latency"),
			csv.Str("p95_task_latency"),
			csv.Str("p99_task_latency"),
			csv.Str("p99p9_task_latency"),
			csv.Str("p99p99_task_latency"),
		)
	}

	if lt.variancesEnabled {
		fields = append(fields,
			csv.Str("queue_latency_variance"),
			csv.Str("task_latency_variance"),
		)
	}

	if lt.maxTasks > 0 {
		fields = append(fields,
			csv.Str("percent_done"),
		)
	}

	err := csv.WriteRow(cd.writer, fields...)
	if err != nil {
		return err
	}

	// ensure headers flush asap
	return cd.writer.Flush()
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

	cd.writeErr = cd.writer.Flush()
	if cd.writeErr != nil {
		return
	}

	now := time.Now().UTC()

	const (
		prefix = `# {"done":{"end_time":"`
		suffix = `"}}`

		maxRFC3339NanoSerializedBytes = 30
	)

	buf := make([]byte, 0, len(prefix)+maxRFC3339NanoSerializedBytes+len(suffix))

	buf = append(buf, prefix...)
	buf = now.AppendFormat(buf, time.RFC3339Nano)
	buf = append(buf, suffix...)

	_, cd.writeErr = csvFile.Write(buf)
}

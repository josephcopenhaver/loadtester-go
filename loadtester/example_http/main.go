package main

// This example focuses on the simplest form of sending one http request over and over and only validating the response status code
// plus encouraging connection reuse as much as possible

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/josephcopenhaver/loadtester-go/v3/loadtester"
)

type task struct {
	baseReq *http.Request
	client  *http.Client
}

func (t *task) Do(ctx context.Context, workerId int) error {

	req := t.baseReq.Clone(ctx)

	res, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, res.Body) // purposefully ignoring the read error: just trying to read the full body to ensure connection reuse on the happy path

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("expected status code of 200; got %d instead", res.StatusCode)
	}

	return nil
}

type myTaskReader struct {
	task *task
}

func (tr *myTaskReader) ReadTasks(p []loadtester.Doer) int {
	// make sure you only fill up to len
	// filling less than len will signal that the loadtest is over

	var i int
	for i < len(p) {

		// since the task is essentially stateless, just going to repeat the internal task over and over
		//
		// the *http.Client is already designed to be shared
		//
		// and the baseReq will be cloned at the start of each Do() call
		//
		// there is no state involved

		p[i] = tr.task
		i++
	}

	return i
}

func (tr *myTaskReader) SetTransport(rt http.RoundTripper) {
	tr.task.client.Transport = rt
}

func (tr *myTaskReader) CloseIdleConnections() {
	tr.task.client.CloseIdleConnections()
}

func newMyTaskReader(timeout time.Duration, req *http.Request) *myTaskReader {

	t := task{
		baseReq: req,
		client: &http.Client{
			Timeout: timeout,
			// Transport should be set later!
		},
	}

	return &myTaskReader{
		task: &t,
	}
}

func main() {

	var logger loadtester.StructuredLogger
	{
		level := slog.LevelInfo

		if s := os.Getenv("LOG_LEVEL"); s != "" {
			var v slog.Level
			err := v.UnmarshalText([]byte(s))
			if err != nil {
				panic(fmt.Errorf("failed to parse LOG_LEVEL environment variable: %w", err))
			}

			level = v
		}

		v, err := loadtester.NewLogger(level)
		if err != nil {
			panic(err)
		}

		slog.SetDefault(v)
		logger = v
	}

	var ctx context.Context
	{
		c, cancel := loadtester.RootContext(logger)
		defer cancel()
		ctx = c
	}

	var tr *myTaskReader
	{
		req, err := http.NewRequest(http.MethodGet, "https://example.com/", http.NoBody)
		if err != nil {
			panic(err)
		}

		// You should always set a meaningful user agent to identify your product runtime.
		// see https://www.rfc-editor.org/rfc/rfc7231#section-5.5.3
		req.Header.Set("User-Agent", "github.com--josephcopenhaver--loadtester-go--loadtester--example_http--main.go/1.0")

		v := newMyTaskReader(20*time.Second, req)
		defer v.CloseIdleConnections() // ensures your process closes idle connections in the http client's connection pool on shutdown

		tr = v
	}

	const parallelism = 1

	op := loadtester.NewOpts()
	lt, err := loadtester.NewLoadtest(
		op.TaskReader(tr),
		op.Logger(logger),
		op.NumWorkers(parallelism),
		op.NumIntervalTasks(parallelism),
		op.Interval(5*time.Second),
		op.Retry(false), // default is true; not required for this example since no tasks can be retried, plus saves some minor compute and disk io
		// op.MetricsLatencyPercentile(true), // default is false
		// op.MetricsLatencyVarianceEnabled(true),    // default is false
		// op.FlushRetriesOnShutdown(true), // default is false
		// op.MetricsCsv(false) // default is true; set to false to stop creating a metrics.csv file on loadtest run
	)
	if err != nil {
		panic(err)
	}

	// ensure the http client's transport config is setup correctly for the load test upper bounds
	{
		ht := lt.NewHttpTransport()

		// TODO: feel free to customize the transport's IdleConnTimeout or TLSHandshakeTimeout as needed for your case

		tr.SetTransport(ht)
	}

	if err := lt.Run(ctx); err != nil {
		panic(err)
	}
}

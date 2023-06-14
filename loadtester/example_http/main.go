package main

// This example focuses on the simplest form of sending one http request over and over and only validating the response status code
// plus encouraging connection reuse as much as possible

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/josephcopenhaver/loadtester-go/loadtester"
	"go.uber.org/zap/zapcore"
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
	_, _ = io.Copy(ioutil.Discard, res.Body) // purposefully ignoring the read error: just trying to read the full body to ensure connection reuse on the happy path

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("expected status code of 200; got %d instead", res.StatusCode)
	}

	return nil
}

type myTaskProvider struct {
	task    *task
	cfgChan chan loadtester.ConfigUpdate
}

func (tp *myTaskProvider) ReadTasks(p []loadtester.Doer) int {
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

		p[i] = tp.task
		i++
	}

	return i
}

func (tp *myTaskProvider) UpdateConfigChan() <-chan loadtester.ConfigUpdate {
	return tp.cfgChan
}

func (tp *myTaskProvider) SetTransport(rt http.RoundTripper) {
	tp.task.client.Transport = rt
}

func (tp *myTaskProvider) CloseIdleConnections() {
	tp.task.client.CloseIdleConnections()
}

func newMyTaskProvider(timeout time.Duration, req *http.Request) *myTaskProvider {

	t := task{
		baseReq: req,
		client: &http.Client{
			Timeout: timeout,
			// Transport should be set later!
		},
	}

	return &myTaskProvider{
		task:    &t,
		cfgChan: make(chan loadtester.ConfigUpdate),
	}
}

func main() {

	var logger loadtester.SugaredLogger
	{
		level := zapcore.InfoLevel

		if s := os.Getenv("LOG_LEVEL"); s != "" {
			v, err := zapcore.ParseLevel(s)
			if err != nil {
				panic(fmt.Errorf("failed to parse LOG_LEVEL environment variable: %w", err))
			}

			level = v
		}

		v, err := loadtester.NewLogger(level)
		if err != nil {
			panic(err)
		}

		logger = v
	}

	var ctx context.Context
	{
		c, cancel := loadtester.RootContext(logger)
		defer cancel()
		ctx = c
	}

	var tp *myTaskProvider
	{
		req, err := http.NewRequest(http.MethodGet, "https://example.com/", http.NoBody)
		if err != nil {
			panic(err)
		}

		// You should always set a meaningful user agent to identify your product runtime.
		// see https://www.rfc-editor.org/rfc/rfc7231#section-5.5.3
		req.Header.Set("User-Agent", "github.com--josephcopenhaver--loadtester-go--loadtester--example_http--main.go/1.0")

		v := newMyTaskProvider(20*time.Second, req)
		defer v.CloseIdleConnections() // ensures your process closes idle connections in the http client's connection pool on shutdown

		tp = v
	}

	const parallelism = 1

	lt, err := loadtester.NewLoadtest(
		tp,
		loadtester.Logger(logger),
		loadtester.NumWorkers(parallelism),
		loadtester.NumIntervalTasks(parallelism),
		loadtester.Interval(5*time.Second),
		// loadtester.FlushRetriesOnShutdown(true), // default is false
	)
	if err != nil {
		panic(err)
	}

	// ensure the http client's transport config is setup correctly for the load test upper bounds
	{
		ht := lt.NewHttpTransport()

		// TODO: feel free to customize the transport's IdleConnTimeout or TLSHandshakeTimeout as needed for your case

		tp.SetTransport(ht)
	}

	if err := lt.Run(ctx); err != nil {
		panic(err)
	}
}

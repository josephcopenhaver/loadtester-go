package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/josephcopenhaver/loadtester-go/loadtester"
	"go.uber.org/zap/zapcore"
)

type task struct{}

func (t *task) Do(ctx context.Context, workerID int) error {
	return nil
}

type MyLoadtest struct {
	cfgChan chan loadtester.ConfigUpdate
}

func (lt *MyLoadtest) ReadTasks(p []loadtester.Doer) int {
	// make sure you only fill up to len
	// filling less than len will signal that the loadtest is over

	var i int
	for i < len(p) {
		p[i] = &task{}
		i++
	}

	return i
}

func (lt *MyLoadtest) UpdateConfigChan() <-chan loadtester.ConfigUpdate {
	return lt.cfgChan
}

func NewMyLoadtest() *MyLoadtest {
	return &MyLoadtest{
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

	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
	}()

	ctx, cancel := loadtester.RootContext(logger)
	defer cancel()

	mlt := NewMyLoadtest()

	numWorkers := 5

	lt, err := loadtester.NewLoadtest(
		mlt,
		loadtester.Logger(logger),
		loadtester.NumWorkers(numWorkers),
		loadtester.NumIntervalTasks(25),
		loadtester.Interval(1*time.Second),
		// loadtester.FlushRetriesOnShutdown(true), // default is false
	)
	if err != nil {
		panic(err)
	}

	//
	// start loadtest routine
	//
	wg.Add(1)
	go func() {
		defer wg.Done()

		// note, if you do not want to support user input then just end main by starting
		// the loadtest and don't use a wait group or goroutine for it

		logger.Infow("running")
		defer func() {
			logger.Infow("stopped")
		}()
		if err := lt.Run(ctx); err != nil {
			logger.Panicw(
				"loadtest errored",
				"error", err,
			)
		}
	}()

	//
	// support user input to adjust the loadtest config
	//

	// define input handling channel and closer
	inputChan := make(chan string)
	closeInputChan := func() func() {
		var once sync.Once

		c := inputChan

		return func() {
			once.Do(func() {
				close(c)
			})
		}
	}()

	//
	// start example user line input handling routines
	//

	// routine that listens for context done and closes input channel
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-ctx.Done()
		closeInputChan()
	}()

	// routine that offers user strings to input channel
	go func() {
		// note routine intentionally allowed to leak because
		// there is no way to make the reader context aware
		defer closeInputChan()

		reader := bufio.NewReader(os.Stdin)
		for {
			s, err := reader.ReadString('\n')
			if err != nil {
				if errors.Is(err, io.EOF) && ctx.Err() != nil {
					return
				}
				panic(err)
			}

			// note it's possible for this channel
			// write to panic due to the user
			// doing thing really fast and pressing control-c afterward
			//
			// but in that case we're still going through the stop procedure so meh

			inputChan <- strings.TrimSuffix(s, "\n")
		}
	}()

	// user input channel processing loop
	for s := range inputChan {
		var cu loadtester.ConfigUpdate

		switch s {
		case "stop":
			return
		case "set workers":
			cu.SetNumWorkers(numWorkers)
			mlt.cfgChan <- cu
		case "del worker", "remove worker":
			numWorkers -= 1

			cu.SetNumWorkers(numWorkers)
			mlt.cfgChan <- cu
		case "add worker":
			numWorkers += 1

			cu.SetNumWorkers(numWorkers)
			mlt.cfgChan <- cu
		}
	}
}

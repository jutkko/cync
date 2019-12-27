package cync

import (
	"context"
	"errors"
	"time"
)

type Cync struct {
	ErrorHandler func(error)
}

type Job struct {
	Meta string
}

// If ctx is nil, a new context with timeoutSecond timeout will be created for each workerFunc run.
// The library supports the case when the size of jobs is smaller, equal or larger than the
// parallelism setup.
func (c *Cync) Fanout(ctx context.Context, timeoutSeconds int, jobs []*Job, parallelism int64, workerFunc func(context.Context, *Job) error) {
	jobsChan := make(chan *Job)
	results := make(chan error)

	for w := int64(0); w < parallelism; w++ {
		go func() {
			for job := range jobsChan {
				if ctx != nil {
					results <- workerFunc(ctx, job)
				} else if timeoutSeconds != 0 {
					ctx, _ := context.WithTimeout(context.Background(), time.Second*time.Duration(timeoutSeconds))
					results <- workerFunc(ctx, job)
				} else {
					results <- errors.New("invalid context passed into the fanout function")
				}
			}
		}()
	}

	go func() {
		for _, d := range jobs {
			jobsChan <- d
		}
		close(jobsChan)
	}()

	for i := 0; i < len(jobs); i++ {
		err := <-results
		if err != nil {
			// Best practices are: log the error with the corresponding job ID and call the ErrorHandler
			c.ErrorHandler(err)
		}
	}
	close(results)
}

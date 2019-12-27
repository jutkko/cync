package cync

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"sync"
	"testing"
)

type jobTracker struct {
	tracker []string
	lock sync.Mutex
}

func TestDeploymentPool_fanoutSimpleSerialWorker(t *testing.T) {
	jt := &jobTracker{
		tracker: []string{},
	}

	jobs := []*Job{
		{
			Meta: "job 1",
		},
	}

	c := &Cync{
		// Noop
		ErrorHandler: func(err error) {},
	}

	workerFunc := func(ctx context.Context, job *Job) error {
		jt.tracker = append(jt.tracker, job.Meta)
		return nil
	}

	c.Fanout(context.TODO(), 0, jobs, 1, workerFunc)
	assert.Equal(t, "job 1", jt.tracker[0], "missing success message")
}

func TestDeploymentPool_fanoutSimpleWorkerFail(t *testing.T) {
	jt := &jobTracker{
		tracker: []string{},
	}

	jobs := []*Job{
		{
			Meta: "job1",
		},
	}

	c := &Cync{
		// Noop
		ErrorHandler: func(err error) {
			jt.tracker = append(jt.tracker, fmt.Sprintf("error while dealing with fanin while doing test: %s", err.Error()))
		},
	}

	workerFunc := func(ctx context.Context, job *Job) error {
		return errors.New(fmt.Sprintf("job failed %s", job.Meta))
	}

	c.Fanout(context.TODO(), 0, jobs, 1, workerFunc)
	assert.Equal(t, "error while dealing with fanin while doing test: job failed job1", jt.tracker[0], "missing fail message")
}

func TestDeploymentPool_fanoutManyWorkersHighParallelism(t *testing.T) {
	jt := &jobTracker{
		tracker: []string{},
	}

	jobs := []*Job{
		{
			Meta: "test1",
		},
		{
			Meta: "test2",
		},
	}

	c := &Cync{
		// Noop
		ErrorHandler: func(err error) {},
	}

	workerFunc := func(ctx context.Context, job *Job) error {
		jt.lock.Lock()
		jt.tracker = append(jt.tracker, fmt.Sprintf("job %s", job.Meta))
		jt.lock.Unlock()

		return nil
	}

	c.Fanout(context.TODO(), 0, jobs, 50, workerFunc)
	assert.Equal(t, 2, len(jt.tracker), "missing success message")
	assert.True(t, strings.Contains(jt.tracker[1], "job"), "missing success message")
}

func TestDeploymentPool_fanoutFewWorkersHighParallelism(t *testing.T) {
	jt := &jobTracker{
		tracker: []string{},
	}

	var jobs []*Job
	jobsSize := 100
	for i := 0; i < jobsSize; i++ {
		jobs = append(jobs, &Job{
			Meta: fmt.Sprintf("%d", i),
		})
	}

	c := &Cync{
		// Noop
		ErrorHandler: func(err error) {},
	}

	workerFunc := func(ctx context.Context, job *Job) error {
		jt.lock.Lock()
		jt.tracker = append(jt.tracker, fmt.Sprintf("job %s", job.Meta))
		jt.lock.Unlock()

		return nil
	}

	c.Fanout(context.TODO(), 0, jobs, int64(jobsSize/2), workerFunc)
	assert.Equal(t, jobsSize, len(jt.tracker), "missing success message")
	assert.True(t, strings.Contains(jt.tracker[0], "job"), "missing success message")
}

func TestDeploymentPool_fanoutGenericContext(t *testing.T) {
	jt := &jobTracker{
		tracker: []string{},
	}

	var jobs []*Job
	jobsSize := 100
	for i := 0; i < jobsSize; i++ {
		jobs = append(jobs, &Job{
			Meta: fmt.Sprintf("%d", i),
		})
	}

	c := &Cync{
		// Noop
		ErrorHandler: func(err error) {},
	}

	workerFunc := func(ctx context.Context, job *Job) error {
		jt.lock.Lock()
		jt.tracker = append(jt.tracker, fmt.Sprintf("job %s", job.Meta))
		jt.lock.Unlock()

		return nil
	}

	c.Fanout(context.TODO(), 0, jobs, int64(jobsSize/2), workerFunc)
	assert.Equal(t, jobsSize, len(jt.tracker), "missing success message")
}

func TestDeploymentPool_fanoutSpecificContext(t *testing.T) {
	jt := &jobTracker{
		tracker: []string{},
	}

	var jobs []*Job
	jobsSize := 100
	for i := 0; i < jobsSize; i++ {
		jobs = append(jobs, &Job{
			Meta: fmt.Sprintf("%d", i),
		})
	}

	c := &Cync{
		// Noop
		ErrorHandler: func(err error) {},
	}

	workerFunc := func(ctx context.Context, job *Job) error {
		jt.lock.Lock()
		jt.tracker = append(jt.tracker, fmt.Sprintf("job %s", job.Meta))
		jt.lock.Unlock()

		return nil
	}

	c.Fanout(nil, 1, jobs, int64(jobsSize/2), workerFunc)
	assert.Equal(t, jobsSize, len(jt.tracker), "missing success message")
	assert.True(t, strings.Contains(jt.tracker[len(jt.tracker)-1], "job"), "missing success message")
}

func TestDeploymentPool_fanoutInvalidContext(t *testing.T) {
	jt := &jobTracker{
		tracker: []string{},
	}

	var jobs []*Job
	jobsSize := 100
	for i := 0; i < jobsSize; i++ {
		jobs = append(jobs, &Job{
			Meta: fmt.Sprintf("%d", i),
		})
	}

	c := &Cync{
		// Noop
		ErrorHandler: func(err error) {
			jt.tracker = append(jt.tracker, fmt.Sprintf("error while dealing with fanin while doing test: %s", err.Error()))
		},
	}

	workerFunc := func(ctx context.Context, job *Job) error {
		jt.lock.Lock()
		jt.tracker = append(jt.tracker, fmt.Sprintf("job %s", job.Meta))
		jt.lock.Unlock()

		return nil
	}

	c.Fanout(nil, 0, jobs, int64(jobsSize/2), workerFunc)
	assert.Equal(t, jobsSize, len(jt.tracker), "missing success message")
	assert.True(t, strings.Contains(jt.tracker[len(jt.tracker)-1], "invalid context passed into the fanout function"), "missing error message")
}

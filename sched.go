// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

// TaskEnd is closed when a task finishes or never executes.
type TaskEnd chan struct{}

type BasicScheduler struct {
	logger Logger

	closed  atomic.Bool
	running sync.WaitGroup
	tasks   chan task
	lock    sync.RWMutex
}

func NewScheduler(logger Logger, maxTasks uint64) *BasicScheduler {
	s := &BasicScheduler{
		logger: logger,
		tasks:  make(chan task, maxTasks),
	}

	s.logger.Debug("Created Scheduler", zap.Uint64("maxTasks", maxTasks))

	s.running.Add(1)
	go s.run()

	return s
}

func (as *BasicScheduler) Size() int {
	return len(as.tasks)
}

func (as *BasicScheduler) Close() {
	as.lock.Lock()
	defer as.lock.Unlock()

	as.closed.Store(true)
	close(as.tasks)
	defer as.running.Wait()
}

func (as *BasicScheduler) run() {
	defer as.running.Done()
	for task := range as.tasks {
		if as.closed.Load() {
			close(task.done)
			return
		}
		as.logger.Debug("Running task", zap.Int("remaining ready tasks", len(as.tasks)))
		id := task.f()
		close(task.done)
		as.logger.Debug("Task finished execution", zap.Stringer("taskID", id))
	}
}

func (as *BasicScheduler) Schedule(f Task) TaskEnd {
	as.lock.RLock()
	defer as.lock.RUnlock()

	done := make(TaskEnd)


	if as.closed.Load() {
		close(done)
		return done
	}


	as.logger.Debug("Scheduling new ready task", zap.Int("ready tasks", len(as.tasks)+1))
	select {
	case as.tasks <- task{
		f:    f,
		done: done,
	}:
	default:
		close(done)
		return done
		as.logger.Warn("Scheduler task queue is full; task was not scheduled")
	}

	return done
}

type Task func() Digest

type task struct {
	f    Task
	done TaskEnd
}
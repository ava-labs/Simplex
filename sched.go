// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"sync"

	"go.uber.org/zap"
)

type BasicScheduler struct {
	logger Logger

	mu      sync.Mutex
	closed  bool
	running sync.WaitGroup
	tasks   chan Task
}

func NewScheduler(logger Logger, maxTasks uint64) *BasicScheduler {
	s := &BasicScheduler{
		logger: logger,
		tasks:  make(chan Task, maxTasks),
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
	as.mu.Lock()
	if as.closed {
		as.mu.Unlock()
		return
	}
	as.closed = true
	close(as.tasks)
	as.mu.Unlock()

	// we cannot hold the lock while waiting for the scheduler to finish,
	// otherwise [scheduler.schedule] will be blocked trying to acquire the lock
	as.running.Wait()
}

func (as *BasicScheduler) run() {
	defer as.running.Done()
	for task := range as.tasks {
		as.mu.Lock()
		closed := as.closed
		as.mu.Unlock()
		if closed {
			return
		}
		as.logger.Debug("Running task", zap.Int("remaining ready tasks", len(as.tasks)))
		id := task()
		as.logger.Debug("Task finished execution", zap.Stringer("taskID", id))
	}
}

func (as *BasicScheduler) Schedule(task Task) {
	as.mu.Lock()
	defer as.mu.Unlock()

	if as.closed {
		return
	}

	as.logger.Debug("Scheduling new ready task", zap.Int("ready tasks", len(as.tasks)+1))
	select {
	case as.tasks <- task:
	default:
		as.logger.Warn("Scheduler task queue is full; task was not scheduled")
	}
}

type Task func() Digest

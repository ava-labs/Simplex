// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

type BasicScheduler struct {
	logger Logger
	ec ExecutingCounter
	closed  atomic.Bool
	running sync.WaitGroup
	tasks   chan Task
	lock    sync.RWMutex
}

func NewScheduler(logger Logger, maxTasks uint64, ec ExecutingCounter) *BasicScheduler {
	s := &BasicScheduler{
		ec: ec,
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
			as.ec.Decrement()
			return
		}
		as.logger.Debug("Running task", zap.Int("remaining ready tasks", len(as.tasks)))
		id := task()
		as.ec.Decrement()
		as.logger.Debug("Task finished execution", zap.Stringer("taskID", id))
	}
}

func (as *BasicScheduler) Schedule(task Task) {
	as.lock.RLock()
	defer as.lock.RUnlock()

	if as.closed.Load() {
		return
	}

	as.ec.Increment()

	as.logger.Debug("Scheduling new ready task", zap.Int("ready tasks", len(as.tasks)+1))
	select {
	case as.tasks <- task:
	default:
		as.ec.Decrement()
		as.logger.Warn("Scheduler task queue is full; task was not scheduled")
	}
}

type Task func() Digest

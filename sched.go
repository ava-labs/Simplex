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

	closed  atomic.Bool
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
	as.closed.Store(true)
	close(as.tasks)
	defer as.running.Wait()
}

func (as *BasicScheduler) run() {
	defer as.running.Done()
	for task := range as.tasks {
		if as.closed.Load() {
			return
		}
		as.logger.Debug("Running task", zap.Int("remaining ready tasks", len(as.tasks)))
		id := task()
		as.logger.Debug("Task finished execution", zap.Stringer("taskID", id))
	}
}

func (as *BasicScheduler) Schedule(task Task) {
	if as.closed.Load() {
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

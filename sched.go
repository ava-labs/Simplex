// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"sync/atomic"

	"go.uber.org/zap"
)

type Scheduler struct {
	logger Logger

	closed         atomic.Bool
	tasks          chan Task
	onTaskFinished func(Digest)
}

func NewScheduler(logger Logger, maxTasks uint64, onTaskFinished func(Digest)) *Scheduler {
	s := &Scheduler{
		logger:         logger,
		tasks:          make(chan Task, maxTasks),
		onTaskFinished: onTaskFinished,
	}

	s.logger.Debug("Created Scheduler", zap.Uint64("maxTasks", maxTasks))

	go s.run()

	return s
}

func (as *Scheduler) Size() int {
	return len(as.tasks)
}

func (as *Scheduler) Close() {
	as.closed.Store(true)
}

func (as *Scheduler) run() {
	for task := range as.tasks {
		as.logger.Debug("Running task", zap.Int("remaining ready tasks", len(as.tasks)))
		id := task()
		as.logger.Debug("Task finished execution", zap.Stringer("taskID", id))

		as.onTaskFinished(id)

		if as.closed.Load() {
			return
		}
	}
}

func (as *Scheduler) Schedule(task Task) {
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

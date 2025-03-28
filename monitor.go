// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type Monitor struct {
	logger     Logger
	close      chan struct{}
	time       atomic.Value
	ticks      chan time.Time
	tasks      chan taskFunction
	futureTask atomic.Value
}

type futureTask struct {
	deadline time.Time
	f        taskFunction
}

type taskFunction func(bool)

func NewMonitor(startTime time.Time, logger Logger) *Monitor {
	m := &Monitor{
		logger: logger,
		close:  make(chan struct{}),
		tasks:  make(chan taskFunction, 1),
		ticks:  make(chan time.Time, 1),
	}

	m.time.Store(startTime)

	go m.run()

	return m
}

func (m *Monitor) AdvanceTime(t time.Time) {
	m.time.Store(t)
	select {
	case m.ticks <- t:
	default:
		m.logger.Warn("Dropping tick, channel is full")
	}
}

func (m *Monitor) tick(now time.Time, taskID uint64) {
	defer m.logger.Verbo("Ticked", zap.Uint64("taskID", taskID), zap.Time("time", now))
	ft := m.futureTask.Load()
	if ft == nil {
		return
	}

	task := ft.(*futureTask)

	if task.f == nil || task.deadline.IsZero() || now.Before(task.deadline) {
		return
	}

	m.logger.Verbo("Executing f", zap.Uint64("taskID", taskID), zap.Time("deadline", task.deadline))
	task.f(true)
	m.logger.Verbo("Executed f", zap.Uint64("taskID", taskID), zap.Time("time", now), zap.Time("deadline", task.deadline))

	// clean up future task to mark we have already executed it and to release memory
	m.futureTask.Store(&futureTask{})
}

func (m *Monitor) run() {
	var taskID uint64
	for m.shouldRun() {
		select {
		case tick := <-m.ticks:
			m.tick(tick, taskID)
		case f := <-m.tasks:
			m.logger.Verbo("Executing f", zap.Uint64("taskID", taskID))
			go f(true)
			m.logger.Verbo("Task executed", zap.Uint64("taskID", taskID))
		}
		taskID++
	}
}

func (m *Monitor) shouldRun() bool {
	select {
	case <-m.close:
		return false
	default:
		return true
	}
}

func (m *Monitor) Close() {
	select {
	case <-m.close:
		return
	default:
		close(m.close)
	}
}

func (m *Monitor) WaitFor(f taskFunction) {
	select {
	case m.tasks <- f:
	default:
		m.logger.Warn("Tasks channel is full")
		oldTask := <-m.tasks
		m.logger.Verbo("Executing previous task")
		oldTask(false)
		m.logger.Verbo("Previous task executed")
		m.logger.Verbo("Re-queuing new task", zap.Int("len", len(m.tasks)))
		m.tasks <- f
	}
}

func (m *Monitor) WaitUntil(timeout time.Duration, f func(bool)) context.CancelFunc {
	t := m.time.Load()
	time := t.(time.Time)

	currentTask := m.futureTask.Load()
	if currentTask != nil {
		currentTask := currentTask.(*futureTask)
		if currentTask.f != nil {
			m.logger.Warn("Overridding deadline", zap.Time("deadline", currentTask.deadline))
		}
	}
	m.futureTask.Store(&futureTask{
		f:        f,
		deadline: time.Add(timeout),
	})

	m.logger.Verbo("Scheduling task", zap.Duration("timeout", timeout), zap.Time("deadline", time))

	return func() {
		m.futureTask.Store(&futureTask{})
	}
}

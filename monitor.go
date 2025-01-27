// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"sync"
	"time"
)

type Monitor struct {
	lock  sync.Mutex
	close bool
	time  time.Time
	ticks chan time.Time
	tasks chan func()
	// waitUntil
	deadline   time.Time
	futureTask func()
}

func NewMonitor(startTime time.Time) *Monitor {
	m := &Monitor{
		tasks: make(chan func()),
		time:  startTime,
		ticks: make(chan time.Time, 1),
	}

	return m
}

func (m *Monitor) AdvanceTime(t time.Time) {
	m.time = t
	select {
	case m.ticks <- t:
	default:

	}
}

func (m *Monitor) tick(now time.Time) {
	if !m.deadline.IsZero() && now.After(m.deadline) {
		m.futureTask()
		m.deadline = time.Time{}
		m.futureTask = nil
	}
}

func (m *Monitor) run() {
	for m.shouldRun() {
		select {
		case tick := <-m.ticks:
			m.tick(tick)
		case f := <-m.tasks:
			f()
			m.drainTicks()
		}
	}
}

func (m *Monitor) drainTicks() {
	select {
	case <-m.ticks:
	default:

	}
}

func (m *Monitor) shouldRun() bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	return !m.close
}

func (m *Monitor) Close() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.close = true
}

func (m *Monitor) WaitFor(f func()) {
	m.tasks <- f
}

func (m *Monitor) WaitUntil(timeout time.Duration, f func()) context.CancelFunc {
	m.futureTask = f
	m.deadline = m.time.Add(timeout)
	return nil
}

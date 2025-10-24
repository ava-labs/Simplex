// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

type timeoutRunner func(ids []uint64)

type TimeoutHandler struct {
	// how often to run through the tasks
	runInterval time.Duration
	// function to run tasks
	taskRunner timeoutRunner

	lock  sync.Mutex
	ticks chan time.Time
	close chan struct{}
	// maps id to a task
	tasks map[uint64]struct{}
	now   time.Time

	log Logger
}

// NewTimeoutHandler returns a TimeoutHandler and starts a new goroutine that
// listens for ticks and executes TimeoutTasks.
func NewTimeoutHandler(log Logger, startTime time.Time, runInterval time.Duration, taskRunner timeoutRunner) *TimeoutHandler {
	t := &TimeoutHandler{
		now:         startTime,
		tasks:       make(map[uint64]struct{}),
		ticks:       make(chan time.Time, 1),
		close:       make(chan struct{}),
		runInterval: runInterval,
		taskRunner:  taskRunner,
		log:         log,
	}

	go t.run(startTime)

	return t
}

func (t *TimeoutHandler) run(startTime time.Time) {
	lastTickTime := startTime

	for t.shouldRun() {
		select {
		case now := <-t.ticks:
			if now.Sub(lastTickTime) < t.runInterval {
				continue
			}
			lastTickTime = now

			// update the current time
			t.lock.Lock()
			t.now = now
			t.lock.Unlock()

			t.maybeRunTasks()
		case <-t.close:
			return
		}
	}
}

func (t *TimeoutHandler) maybeRunTasks() {
	// go through the heap executing relevant tasks
	// grab all sequences
	ids := make([]uint64, 0, len(t.tasks))

	t.lock.Lock()
	for id := range t.tasks {
		ids = append(ids, id)
	}
	t.lock.Unlock()

	if len(ids) == 0 {
		return
	}

	t.taskRunner(ids)
}

func (t *TimeoutHandler) shouldRun() bool {
	select {
	case <-t.close:
		return false
	default:
		return true
	}
}

func (t *TimeoutHandler) Tick(now time.Time) {
	select {
	case t.ticks <- now:
		t.lock.Lock()
		t.now = now
		t.lock.Unlock()
	default:
		t.log.Debug("Dropping tick in timeouthandler")
	}
}

func (t *TimeoutHandler) AddTask(id uint64) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.tasks[id] = struct{}{}
	t.log.Debug("Adding timeout task", zap.Uint64("id", id))
}

func (t *TimeoutHandler) RemoveTask(ID uint64) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if _, ok := t.tasks[ID]; !ok {
		return
	}

	// find the task using the task map
	// remove it from the heap using the index
	t.log.Debug("Removing timeout task", zap.Uint64("id", ID))
	delete(t.tasks, ID)
}

func (t *TimeoutHandler) RemoveOldTasks(cutoff uint64) {
	t.lock.Lock()
	defer t.lock.Unlock()

	for id := range t.tasks {
		if id < cutoff {
			t.log.Debug("Removing old timeout task", zap.Uint64("id", id))
			delete(t.tasks, id)
		}
	}
}

func (t *TimeoutHandler) Close() {
	select {
	case <-t.close:
		return
	default:
		close(t.close)
	}
}

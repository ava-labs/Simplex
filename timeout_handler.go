// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"container/heap"
	"sync"
	"time"

	"go.uber.org/zap"
)

type TimeoutTask struct {
	ID       string
	Task     func()
	Deadline time.Time

	index int // for heap to work more efficiently
}

func NewTimeoutTask(ID string, task func(), timeout time.Time) *TimeoutTask {
	return &TimeoutTask{
		ID:       ID,
		Task:     task,
		Deadline: timeout,
	}
}

type TimeoutHandler struct {
	lock sync.Mutex

	ticks chan time.Time
	close chan struct{}
	tasks map[string]*TimeoutTask
	heap  TaskHeap
	now   time.Time

	log Logger
}

// NewTimeoutHandler returns a TimeoutHandler and starts a new goroutine that
// listens for ticks and executes TimeoutTasks.
func NewTimeoutHandler(log Logger, startTime time.Time) *TimeoutHandler {
	t := &TimeoutHandler{
		now:   startTime,
		tasks: make(map[string]*TimeoutTask),
		heap:  TaskHeap{},
		ticks: make(chan time.Time),
		close: make(chan struct{}),
		log:   log,
	}

	go t.run()

	return t
}

func (t *TimeoutHandler) GetTime() time.Time {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.now
}

func (t *TimeoutHandler) run() {
	for t.shouldRun() {
		now := <-t.ticks

		t.lock.Lock()
		t.now = now
		t.lock.Unlock()

		t.maybeRunTasks()

	}
}

func (t *TimeoutHandler) maybeRunTasks() {
	// go through the heap executing relevant tasks
	for {
		t.lock.Lock()
		if t.heap.Len() == 0 {
			t.lock.Unlock()
			break
		}

		next := t.heap[0]
		if next.Deadline.After(t.now) {
			t.lock.Unlock()
			break
		}

		heap.Pop(&t.heap)
		delete(t.tasks, next.ID)
		t.lock.Unlock()
		t.log.Debug("Executing timeout task", zap.String("taskid", next.ID))
		next.Task()
	}
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
	// update the time of the handler
	t.ticks <- now
}

func (t *TimeoutHandler) AddTask(task *TimeoutTask) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// adds a task to the heap and the tasks map
	if _, ok := t.tasks[task.ID]; ok {
		// TODO: log warn instead of return
		return
	}

	t.tasks[task.ID] = task
	t.log.Debug("Adding timeout task", zap.String("taskid", task.ID))
	heap.Push(&t.heap, task)
}

func (t *TimeoutHandler) RemoveTask(ID string) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if _, ok := t.tasks[ID]; !ok {
		return
	}

	// find the task using the task map
	// remove it from the heap using the index
	t.log.Debug("Removing timeout task", zap.String("taskid", ID))
	heap.Remove(&t.heap, t.tasks[ID].index)
	delete(t.tasks, ID)
}

func (t *TimeoutHandler) Close() {
	select {
	case <-t.close:
		return
	default:
		close(t.close)
	}
}

// ----------------------------------------------------------------------
type TaskHeap []*TimeoutTask

func (h *TaskHeap) Len() int { return len(*h) }

// Less returns if the task at index [i] has a lower timeout than the task at index [j]
func (h *TaskHeap) Less(i, j int) bool { return (*h)[i].Deadline.Before((*h)[j].Deadline) }

// Swap swaps the values at index [i] and [j]
func (h *TaskHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
	(*h)[i].index = i
	(*h)[j].index = j
}

func (h *TaskHeap) Push(x any) {
	task := x.(*TimeoutTask)
	task.index = h.Len()
	*h = append(*h, task)
}

func (h *TaskHeap) Pop() any {
	old := *h
	len := h.Len()
	task := old[len-1]
	old[len-1] = nil
	*h = old[0 : len-1]
	task.index = -1
	return task
}

func (h *TaskHeap) Peep() any {
	if h.Len() == 0 {
		return nil
	}

	return (*h)[0]
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

type TimeoutTask struct {
	NodeID   NodeID
	TaskID   string
	Start    uint64
	End      uint64
	Task     func()
	Deadline time.Time

	index int // for heap to work more efficiently
}

type TimeoutHandler struct {
	lock sync.Mutex

	ticks chan time.Time
	close chan struct{}
	// nodeids -> range -> task
	tasks map[string]map[string]*TimeoutTask
	heap  TaskHeap
	now   time.Time

	log Logger
}

// NewTimeoutHandler returns a TimeoutHandler and starts a new goroutine that
// listens for ticks and executes TimeoutTasks.
func NewTimeoutHandler(log Logger, startTime time.Time, nodes []NodeID) *TimeoutHandler {
	tasks := make(map[string]map[string]*TimeoutTask)
	for _, node := range nodes {
		tasks[string(node)] = make(map[string]*TimeoutTask)
	}

	t := &TimeoutHandler{
		now:   startTime,
		tasks: tasks,
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
		delete(t.tasks[string(next.NodeID)], next.TaskID)
		t.lock.Unlock()
		t.log.Debug("Executing timeout task", zap.String("taskid", next.TaskID))
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

	if _, ok := t.tasks[string(task.NodeID)]; !ok {
		t.log.Info("Attempting to add a task for an unknown node", zap.Stringer("node", task.NodeID))
	}

	// adds a task to the heap and the tasks map
	if _, ok := t.tasks[string(task.NodeID)][task.TaskID]; ok {
		t.log.Debug("Trying to add an already included task", zap.Stringer("node", task.NodeID), zap.String("Task ID", task.TaskID))
		return
	}

	t.tasks[string(task.NodeID)][task.TaskID] = task
	t.log.Debug("Adding timeout task", zap.Stringer("node", task.NodeID), zap.String("taskid", task.TaskID))
	heap.Push(&t.heap, task)
}

func (t *TimeoutHandler) RemoveTask(nodeID NodeID, ID string) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if _, ok := t.tasks[string(nodeID)]; !ok {
		t.log.Info("Attempting to add a remove a task for an unknown node", zap.Stringer("node", nodeID))
		return
	}

	if _, ok := t.tasks[string(nodeID)][ID]; !ok {
		return
	}

	// find the task using the task map
	// remove it from the heap using the index
	t.log.Debug("Removing timeout task", zap.String("taskid", ID))
	heap.Remove(&t.heap, t.tasks[string(nodeID)][ID].index)
	delete(t.tasks[string(nodeID)], ID)
}

func (t *TimeoutHandler) Close() {
	select {
	case <-t.close:
		return
	default:
		close(t.close)
	}
}

// FindTask returns the first TimeoutTask assigned to [node] that contains any sequence in [seqs].
// A sequence is considered "contained" if it falls between a task's Start (inclusive) and End (inclusive).
func (t *TimeoutHandler) FindTask(node NodeID, seqs []uint64) *TimeoutTask {
	var task *TimeoutTask
seq:
	for _, seq := range seqs {
		for _, t := range t.tasks[string(node)] {
			if seq >= t.Start && seq <= t.End {
				task = t
				break seq
			}
		}
	}

	return task
}

const delimiter = "_"

func getTimeoutID(start, end uint64) string {
	return fmt.Sprintf("%d%s%d", start, delimiter, end)
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

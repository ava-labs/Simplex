package simplex

import (
	"container/heap"
	"sync"
	"time"
)

type ID string

type TimeoutTask struct {
	ID      ID
	Task    func()
	Timeout time.Time

	index int // for heap to work more efficiently
}

func NewTimeoutTask(ID ID, task func(), timeout time.Time) *TimeoutTask {
	return &TimeoutTask{
		ID:      ID,
		Task:    task,
		Timeout: timeout,
	}
}

type TimeoutHandler struct {
	lock sync.Mutex

	tasks map[ID]*TimeoutTask
	heap  TaskHeap
	now   time.Time
}

func NewTimeoutHandler(startTime time.Time) *TimeoutHandler {
	return &TimeoutHandler{
		now:   startTime,
		tasks: make(map[ID]*TimeoutTask),
		heap:  TaskHeap{},
	}
}

func (t *TimeoutHandler) Tick(now time.Time) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// update the time of the handler
	t.now = now

	// go through the heap executing relevant tasks
	for t.heap.Len() > 0 {
		next := t.heap[0]
		if next.Timeout.After(t.now) {
			break
		}

		heap.Pop(&t.heap)
		delete(t.tasks, next.ID)
		go next.Task()
	}
}

func (t *TimeoutHandler) AddTask(task *TimeoutTask) {
	// adds a task to the heap and the tasks map
	t.lock.Lock()
	defer t.lock.Unlock()

	if _, ok := t.tasks[task.ID]; ok {
		// TODO: log warn instead of return
		return
	}

	t.tasks[task.ID] = task
	heap.Push(&t.heap, task)
}

func (t *TimeoutHandler) RemoveTask(ID ID) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if _, ok := t.tasks[ID]; !ok {
		return
	}

	// find the task using the task map
	// remove it from the heap using the index
	heap.Remove(&t.heap, t.tasks[ID].index)
	delete(t.tasks, ID)
}

// ----------------------------------------------------------------------
type TaskHeap []*TimeoutTask

func (h *TaskHeap) Len() int { return len(*h) }

// Less returns if the task at index [i] has a lower timeout than the task at index [j]
func (h *TaskHeap) Less(i, j int) bool { return (*h)[i].Timeout.Before((*h)[j].Timeout) }

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

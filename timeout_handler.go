package simplex

import (
	"container/heap"
	"sync"
	"time"
)

type ID string

type TimeoutTask struct {
	id ID
	task func()
	timeout time.Time

	index int // for heap to work more efficiently
}

type TimeoutHandler struct {
	lock sync.Mutex

	tasks map[ID]*TimeoutTask
	heap TaskHeap
	now time.Time
}

func NewTimeoutHandler(startTime time.Time) *TimeoutHandler {
	return &TimeoutHandler{
		now: startTime,
		tasks: make(map[ID]*TimeoutTask),
		heap: TaskHeap{},
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
		if next.timeout.After(t.now) {
			break
		}

		heap.Pop(&t.heap)
		delete(t.tasks, next.id)
		go next.task()
	}
}

func (t *TimeoutHandler) AddTask(task *TimeoutTask) {
	// adds a task to the heap and the tasks map
	t.lock.Lock()
	defer t.lock.Unlock()

	if _, ok := t.tasks[task.id]; ok {
		// TODO: log warn instead of return
		return
	}

	t.tasks[task.id] = task
	heap.Push(&t.heap, task)
}

func (t *TimeoutHandler) RemoveTask(id ID) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if _, ok := t.tasks[id]; !ok {
		return
	}

	// find the task using the task map
	// remove it from the heap using the index
	heap.Remove(&t.heap, t.tasks[id].index)
	delete(t.tasks, id)
}




// ----------------------------------------------------------------------
type TaskHeap []*TimeoutTask

func (h *TaskHeap) Len() int {return len(*h)}
// Less returns if the task at index [i] has a lower timeout than the task at index [j]
func (h *TaskHeap) Less(i, j int) bool { return (*h)[i].timeout.Before((*h)[j].timeout) }

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
	*h = old[0: len - 1]
	task.index = -1
	return task
}

func (h *TaskHeap) Peep() any {
	if h.Len() == 0 {
		return nil
	}

	return (*h)[0]
}
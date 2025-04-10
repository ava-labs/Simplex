package simplex

import (
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
	mu sync.Mutex

	tasks map[ID]*TimeoutTask
	heap TaskHeap
	now time.Time
}

func (t *TimeoutHandler) Tick(now time.Time) {
	// update the time of the handler
	t.now = now

	// go through the heap executing relevant tasks
}

func (t *TimeoutHandler) AddTask(task TimeoutTask) {
	// adds a task to the heap and the tasks map
}

func (t *TimeoutHandler) RemoveTask(id ID) {
	// find the task using the task map
	// remove it from the heap using the index
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
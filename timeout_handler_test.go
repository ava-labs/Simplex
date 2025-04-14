package simplex_test

import (
	"simplex"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAddAndRunTask(t *testing.T) {
	start := time.Now()
	handler := simplex.NewTimeoutHandler(start)

	sent := make(chan struct{}, 1)

	task := simplex.NewTimeoutTask("task1", func() {
		sent <- struct{}{}
	}, start.Add(5*time.Second))

	handler.AddTask(task)
	handler.Tick(start.Add(2 * time.Second))
	require.Zero(t, len(sent))
	handler.Tick(start.Add(6 * time.Second))
	<-sent
}

func TestRemoveTask(t *testing.T) {
	start := time.Now()
	handler := simplex.NewTimeoutHandler(start)

	var ran bool
	task := &simplex.TimeoutTask{
		ID:      "task2",
		Timeout: start.Add(1 * time.Second),
		Task: func() {
			ran = true
		},
	}

	handler.AddTask(task)
	handler.RemoveTask("task2")
	handler.Tick(start.Add(2 * time.Second))
	require.False(t, ran)

	// since we run tasks in different go routine
	time.Sleep(10 * time.Millisecond)
	require.False(t, ran)
}

func TestTaskOrder(t *testing.T) {
	start := time.Now()
	handler := simplex.NewTimeoutHandler(start)

	var mu sync.Mutex
	results := []string{}

	handler.AddTask(&simplex.TimeoutTask{
		ID:      "first",
		Timeout: start.Add(1 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "first")
			mu.Unlock()
		},
	})

	handler.AddTask(&simplex.TimeoutTask{
		ID:      "second",
		Timeout: start.Add(2 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "second")
			mu.Unlock()
		},
	})

	handler.AddTask(&simplex.TimeoutTask{
		ID:      "noruntask",
		Timeout: start.Add(4 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "norun")
			mu.Unlock()
		},
	})

	handler.Tick(start.Add(3 * time.Second))
	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	require.Equal(t, 2, len(results))
	// they may not be in the same order due to tasks running in ther own goroutine
	require.Contains(t, results, "first")
	require.Contains(t, results, "second")
}

func TestAddTasksOutOfOrder(t *testing.T) {
	start := time.Now()
	handler := simplex.NewTimeoutHandler(start)

	var mu sync.Mutex
	results := []string{}

	handler.AddTask(&simplex.TimeoutTask{
		ID:      "third",
		Timeout: start.Add(3 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "third")
			mu.Unlock()
		},
	})

	handler.AddTask(&simplex.TimeoutTask{
		ID:      "second",
		Timeout: start.Add(2 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "second")
			mu.Unlock()
		},
	})

	handler.AddTask(&simplex.TimeoutTask{
		ID:      "fourth",
		Timeout: start.Add(4 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "fourth")
			mu.Unlock()
		},
	})

	handler.AddTask(&simplex.TimeoutTask{
		ID:      "first",
		Timeout: start.Add(1 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "first")
			mu.Unlock()
		},
	})

	handler.Tick(start.Add(1 * time.Second))
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	require.Equal(t, 1, len(results))
	require.Contains(t, results, "first")
	mu.Unlock()

	handler.Tick(start.Add(3 * time.Second))
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	require.Equal(t, 3, len(results))
	require.Contains(t, results, "second")
	require.Contains(t, results, "third")
	mu.Unlock()

	handler.Tick(start.Add(4 * time.Second))
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	require.Equal(t, 4, len(results))
	require.Contains(t, results, "fourth")
	mu.Unlock()
}

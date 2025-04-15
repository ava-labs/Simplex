// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"simplex"
	"simplex/testutil"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAddAndRunTask(t *testing.T) {
	start := time.Now()
	l := testutil.MakeLogger(t, 1)
	nodes := []simplex.NodeID{{1}, {2}}
	handler := simplex.NewTimeoutHandler(l, start, nodes)

	sent := make(chan struct{}, 1)

	task := &simplex.TimeoutTask{
		NodeID:   nodes[0],
		TaskID:   "simplerun",
		Deadline: start.Add(5 * time.Second),
		Task: func() {
			sent <- struct{}{}
		},
	}

	handler.AddTask(task)
	handler.Tick(start.Add(2 * time.Second))
	require.Zero(t, len(sent))
	handler.Tick(start.Add(6 * time.Second))
	<-sent
}

func TestRemoveTask(t *testing.T) {
	start := time.Now()
	l := testutil.MakeLogger(t, 1)
	nodes := []simplex.NodeID{{1}, {2}}
	handler := simplex.NewTimeoutHandler(l, start, nodes)

	var ran bool
	task := &simplex.TimeoutTask{
		NodeID:   nodes[0],
		TaskID:   "task2",
		Deadline: start.Add(1 * time.Second),
		Task: func() {
			ran = true
		},
	}

	handler.AddTask(task)
	handler.RemoveTask(nodes[0], "task2")
	handler.Tick(start.Add(2 * time.Second))
	require.False(t, ran)

	// ensure no panic
	handler.RemoveTask(nodes[1], "task-doesn't-exist")
}

func TestTaskOrder(t *testing.T) {
	start := time.Now()
	l := testutil.MakeLogger(t, 1)
	nodes := []simplex.NodeID{{1}, {2}}
	handler := simplex.NewTimeoutHandler(l, start, nodes)
	finished := make(chan struct{})

	var mu sync.Mutex
	results := []string{}

	handler.AddTask(&simplex.TimeoutTask{
		NodeID:   nodes[0],
		TaskID:   "first",
		Deadline: start.Add(1 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "first")
			finished <- struct{}{}
			mu.Unlock()
		},
	})

	handler.AddTask(&simplex.TimeoutTask{
		NodeID:   nodes[1],
		TaskID:   "second",
		Deadline: start.Add(2 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "second")
			finished <- struct{}{}
			mu.Unlock()
		},
	})

	handler.AddTask(&simplex.TimeoutTask{
		NodeID:   nodes[0],
		TaskID:   "noruntask",
		Deadline: start.Add(4 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "norun")
			mu.Unlock()
		},
	})

	handler.Tick(start.Add(3 * time.Second))

	<-finished
	<-finished

	mu.Lock()
	defer mu.Unlock()

	require.Equal(t, 2, len(results))
	require.Contains(t, results, "first")
	require.Contains(t, results, "second")
}

func TestAddTasksOutOfOrder(t *testing.T) {
	start := time.Now()
	l := testutil.MakeLogger(t, 1)
	nodes := []simplex.NodeID{{1}, {2}}
	handler := simplex.NewTimeoutHandler(l, start, nodes)
	finished := make(chan struct{})
	var mu sync.Mutex
	results := []string{}

	handler.AddTask(&simplex.TimeoutTask{
		NodeID:   nodes[0],
		TaskID:   "third",
		Deadline: start.Add(3 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "third")
			finished <- struct{}{}
			mu.Unlock()
		},
	})

	handler.AddTask(&simplex.TimeoutTask{
		NodeID:   nodes[0],
		TaskID:   "second",
		Deadline: start.Add(2 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "second")
			finished <- struct{}{}
			mu.Unlock()
		},
	})

	handler.AddTask(&simplex.TimeoutTask{
		NodeID:   nodes[1],
		TaskID:   "fourth",
		Deadline: start.Add(4 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "fourth")
			finished <- struct{}{}
			mu.Unlock()
		},
	})

	handler.AddTask(&simplex.TimeoutTask{
		NodeID:   nodes[0],
		TaskID:   "first",
		Deadline: start.Add(1 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "first")
			finished <- struct{}{}
			mu.Unlock()
		},
	})

	handler.Tick(start.Add(1 * time.Second))
	<-finished
	mu.Lock()
	require.Equal(t, 1, len(results))
	require.Contains(t, results, "first")
	mu.Unlock()

	handler.Tick(start.Add(3 * time.Second))
	<-finished
	<-finished
	mu.Lock()
	require.Equal(t, 3, len(results))
	require.Contains(t, results, "second")
	require.Contains(t, results, "third")
	mu.Unlock()

	handler.Tick(start.Add(4 * time.Second))
	<-finished
	mu.Lock()
	require.Equal(t, 4, len(results))
	require.Contains(t, results, "fourth")
	mu.Unlock()
}

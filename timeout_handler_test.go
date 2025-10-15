// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"

	"github.com/stretchr/testify/require"
)

const testRunInterval = 1 * time.Second

func TestAddAndRunTask(t *testing.T) {
	start := time.Now()
	l := testutil.MakeLogger(t, 1)
	ran := make(chan uint64, 1)
	runner := func(ids []uint64) {
		require.Len(t, ids, 1)
		ran <- ids[0]
	}

	handler := simplex.NewTimeoutHandler(l, start, testRunInterval, runner)
	defer handler.Close()

	handler.AddTask(1)
	handler.Tick(start.Add(testRunInterval))
	value := <-ran
	require.Equal(t, uint64(1), value)

	// if we dont remove the task, it should run again
	handler.Tick(start.Add(2 * testRunInterval))
	value = <-ran
	require.Equal(t, uint64(1), value)

	handler.RemoveTask(1)
	handler.Tick(start.Add(3 * testRunInterval))
	time.Sleep(100 * time.Millisecond) // give some time for the task to run if it was going to
	require.Empty(t, ran)
}

func TestRemoveTask(t *testing.T) {
	start := time.Now()
	l := testutil.MakeLogger(t, 1)
	runner := func(ids []uint64) {
		require.Fail(t, "shouldn't run")
	}
	handler := simplex.NewTimeoutHandler(l, start, testRunInterval, runner)
	defer handler.Close()

	handler.AddTask(1)
	handler.Tick(start.Add(testRunInterval / 2))
	handler.RemoveTask(1)
	handler.Tick(start.Add(testRunInterval))
	time.Sleep(100 * time.Millisecond) // give some time for the task to run if it was going to
}

func TestMultipleTasks(t *testing.T) {
	start := time.Now()
	l := testutil.MakeLogger(t, 1)
	ran := make(chan uint64, 2)
	runner := func(ids []uint64) {
		for _, id := range ids {
			ran <- id
		}
	}

	handler := simplex.NewTimeoutHandler(l, start, testRunInterval, runner)
	defer handler.Close()

	handler.AddTask(1)
	handler.AddTask(2)
	handler.Tick(start.Add(testRunInterval))
	value1 := <-ran
	value2 := <-ran
	require.Contains(t, []uint64{1, 2}, value1)
	require.Contains(t, []uint64{1, 2}, value2)
	require.NotEqual(t, value1, value2)

	// remove one task, the other should still run
	handler.RemoveTask(value1)
	handler.Tick(start.Add(2 * testRunInterval))
	value := <-ran
	require.Equal(t, value2, value)
	require.Empty(t, ran)
}

func TestClosed(t *testing.T) {
	start := time.Now()
	l := testutil.MakeLogger(t, 1)
	runner := func(ids []uint64) {
		require.Fail(t, "shouldn't run")
	}

	handler := simplex.NewTimeoutHandler(l, start, testRunInterval, runner)

	handler.Close()
	handler.AddTask(1)
	handler.Tick(start.Add(testRunInterval))
	time.Sleep(100 * time.Millisecond) // give some time for the task to run if it was going to
}

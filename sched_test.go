// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"crypto/rand"
	"fmt"
	rand2 "math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"

	"github.com/stretchr/testify/require"
)

var emptyDigest = simplex.Digest{}

func TestDependencyTree(t *testing.T) {
	dt := simplex.NewDependencies()

	for i := 0; i < 5; i++ {
		dt.Insert(simplex.Task{
			F: func() simplex.Digest {
				return simplex.Digest{uint8(i + 1)}
			}, ParentBlockDependency: simplex.Digest{uint8(i)},
		},
		)
	}

	require.Equal(t, 5, dt.Size())

	for i := 0; i < 5; i++ {
		j := dt.RemoveDigest(simplex.Digest{uint8(i)})
		require.Len(t, j, 1)
		require.Equal(t, simplex.Digest{uint8(i + 1)}, j[0].F())
	}
}

func TestSchedulerWithEmptyRoundDependencies(t *testing.T) {
	t.Run("Single Empty Round", func(t *testing.T) {
		as := simplex.NewScheduler(testutil.MakeLogger(t))
		defer as.Close()

		var counter atomic.Int32
		var wg sync.WaitGroup
		wg.Add(1)

		dig2 := makeDigest(t)

		as.Schedule(func() simplex.Digest {
			defer wg.Done()
			counter.Add(1)
			return dig2
		}, emptyDigest, []uint64{1})

		require.Zero(t, counter.Load())
		as.ExecuteEmptyNotarizationDependents(1)
		wg.Wait()
		require.Equal(t, int32(1), counter.Load())
	})
	t.Run("Multiple Empty Rounds", func(t *testing.T) {
		as := simplex.NewScheduler(testutil.MakeLogger(t))
		defer as.Close()

		var counter atomic.Int32
		var wg sync.WaitGroup
		ticks := make(chan struct{})

		wg.Add(1)
		as.Schedule(func() simplex.Digest {
			defer wg.Done()
			counter.Add(1)
			return makeDigest(t)
		}, emptyDigest, []uint64{1, 2, 3})

		wg.Add(1)
		as.Schedule(func() simplex.Digest {
			defer wg.Done()
			counter.Add(2)
			ticks <- struct{}{}
			return makeDigest(t)
		}, emptyDigest, []uint64{1})

		require.Zero(t, counter.Load())
		as.ExecuteEmptyNotarizationDependents(1)
		<-ticks
		require.Equal(t, int32(2), counter.Load())

		as.ExecuteEmptyNotarizationDependents(2)
		as.ExecuteEmptyNotarizationDependents(3)
		wg.Wait()
		require.Equal(t, int32(3), counter.Load())
	})

	t.Run("Empty Round With Digest", func(t *testing.T) {
		as := simplex.NewScheduler(testutil.MakeLogger(t))
		defer as.Close()

		var counter atomic.Int32
		var wg sync.WaitGroup
		wg.Add(1)

		dig1 := makeDigest(t)
		dig2 := makeDigest(t)

		as.Schedule(func() simplex.Digest {
			defer wg.Done()
			counter.Add(1)
			return dig2
		}, dig1, []uint64{1})

		require.Zero(t, counter.Load())
		as.ExecuteEmptyNotarizationDependents(1)
		require.Zero(t, counter.Load())
		as.ExecuteBlockDependents(dig1)
		wg.Wait()
		require.Equal(t, int32(1), counter.Load())
	})

	t.Run("Parent Digest But Child Still Has Empty Rounds", func(t *testing.T) {
		as := simplex.NewScheduler(testutil.MakeLogger(t))
		defer as.Close()

		var counter atomic.Int32
		var wg sync.WaitGroup
		tasks := make(chan struct{})

		dig1 := makeDigest(t)
		dig2 := makeDigest(t)

		// this task depends on dig1 and empty round 1. dig1 will run, but we ensure this doesn't
		// start running until we get empty round 1
		wg.Add(1)
		as.Schedule(func() simplex.Digest {
			defer wg.Done()
			counter.Add(1)
			return dig2
		}, dig1, []uint64{1})

		require.Zero(t, counter.Load())

		wg.Add(1)
		fmt.Println("done scheduling second task")
		as.Schedule(func() simplex.Digest {
			defer wg.Done()
			tasks <- struct{}{}
			return dig1
		}, emptyDigest, nil)

		<-tasks
		require.Zero(t, counter.Load())
		as.ExecuteEmptyNotarizationDependents(1)
		wg.Wait()
		require.Equal(t, int32(1), counter.Load())
	})
}

func TestAsyncScheduler(t *testing.T) {
	t.Run("Executes asynchronously", func(t *testing.T) {
		as := simplex.NewScheduler(testutil.MakeLogger(t))
		defer as.Close()

		ticks := make(chan struct{})

		var wg sync.WaitGroup
		wg.Add(1)

		dig2 := makeDigest(t)

		as.Schedule(func() simplex.Digest {
			defer wg.Done()
			<-ticks
			return dig2
		}, emptyDigest, []uint64{})

		ticks <- struct{}{}
		wg.Wait()
	})

	t.Run("Does not execute when closed", func(t *testing.T) {
		as := simplex.NewScheduler(testutil.MakeLogger(t))
		ticks := make(chan struct{}, 1)

		as.Close()

		dig2 := makeDigest(t)

		as.Schedule(func() simplex.Digest {
			close(ticks)
			return dig2
		}, emptyDigest, []uint64{})

		ticks <- struct{}{}
	})

	t.Run("Executes several pending tasks concurrently in arbitrary order", func(t *testing.T) {
		as := simplex.NewScheduler(testutil.MakeLogger(t))
		defer as.Close()

		n := 9000

		var lock sync.Mutex
		finished := make(map[simplex.Digest]struct{})

		var wg sync.WaitGroup
		wg.Add(n)

		var prevTask simplex.Digest
		tasks := make([]func(), n)

		for i := 0; i < n; i++ {
			taskID := makeDigest(t)
			tasks[i] = scheduleTask(&lock, finished, prevTask, taskID, &wg, as, i)
			// Next iteration's previous task ID is current task ID
			prevTask = taskID
		}

		seed := time.Now().UnixNano()
		r := rand2.New(rand2.NewSource(seed))

		for _, index := range r.Perm(n) {
			tasks[index]()
		}

		wg.Wait()
	})
}

func scheduleTask(lock *sync.Mutex, finished map[simplex.Digest]struct{}, dependency simplex.Digest, id simplex.Digest, wg *sync.WaitGroup, as *simplex.Scheduler, i int) func() {
	var dep simplex.Digest
	copy(dep[:], dependency[:])

	return func() {
		lock.Lock()
		defer lock.Unlock()

		_, hasFinished := finished[dep]

		task := func() simplex.Digest {
			lock.Lock()
			defer lock.Unlock()
			finished[id] = struct{}{}
			wg.Done()
			return id
		}

		dep := emptyDigest
		if !hasFinished {
			dep = dependency
		}

		as.Schedule(task, dep, []uint64{})
	}
}

func makeDigest(t *testing.T) simplex.Digest {
	var dig simplex.Digest
	_, err := rand.Read(dig[:])
	require.NoError(t, err)
	return dig
}

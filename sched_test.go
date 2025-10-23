// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"crypto/rand"
	rand2 "math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"

	"github.com/stretchr/testify/require"
)

func TestDependencyTree(t *testing.T) {
	dt := simplex.NewDependencies()

	for i := 0; i < 5; i++ {
		dt.Insert(simplex.Task{F: func() simplex.Digest {
			return simplex.Digest{uint8(i + 1)}
		}, Parent: simplex.Digest{uint8(i)}})
	}

	require.Equal(t, 5, dt.Size())

	for i := 0; i < 5; i++ {
		j := dt.Remove(simplex.Digest{uint8(i)})
		require.Len(t, j, 1)
		require.Equal(t, simplex.Digest{uint8(i + 1)}, j[0].F())
	}

}

func TestAsyncScheduler(t *testing.T) {
	t.Run("Executes asynchronously", func(t *testing.T) {
		as := simplex.NewScheduler(testutil.MakeLogger(t))
		defer as.Close()

		ticks := make(chan struct{})

		var wg sync.WaitGroup
		wg.Add(1)

		dig1 := makeDigest(t)
		dig2 := makeDigest(t)

		as.Schedule(func() simplex.Digest {
			defer wg.Done()
			<-ticks
			return dig2
		}, dig1, 0, true)

		ticks <- struct{}{}
		wg.Wait()
	})

	t.Run("Does not execute when closed", func(t *testing.T) {
		as := simplex.NewScheduler(testutil.MakeLogger(t))
		ticks := make(chan struct{}, 1)

		as.Close()

		dig1 := makeDigest(t)
		dig2 := makeDigest(t)

		as.Schedule(func() simplex.Digest {
			close(ticks)
			return dig2
		}, dig1, 0, true)

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

func TestSchedulerKill(t *testing.T) {
	as := simplex.NewScheduler(testutil.MakeLogger(t))
	defer as.Close()

	var d1 simplex.Digest
	var d2 simplex.Digest
	var d3 simplex.Digest
	var d4 simplex.Digest

	_, err := rand.Read(d1[:])
	require.NoError(t, err)

	_, err = rand.Read(d2[:])
	require.NoError(t, err)

	_, err = rand.Read(d3[:])
	require.NoError(t, err)

	_, err = rand.Read(d4[:])
	require.NoError(t, err)

	var wg0 sync.WaitGroup
	wg0.Add(1)

	var task1Ran, task2Ran, task3Ran, task4Ran atomic.Bool

	// All tasks depend on the first task.
	// The first task is ready to run immediately.

	as.Schedule(func() simplex.Digest {
		task1Ran.Store(true)
		wg0.Wait()
		return d1
	}, simplex.Digest{}, 1, true)

	as.Schedule(func() simplex.Digest {
		task2Ran.Store(true)
		wg0.Wait()
		return d2
	}, d1, 2, false)

	as.Schedule(func() simplex.Digest {
		task3Ran.Store(true)
		wg0.Wait()
		return d3
	}, d1, 0, false)

	as.Schedule(func() simplex.Digest {
		task4Ran.Store(true)
		wg0.Wait()
		return d4
	}, d1, 1, false)

	require.Equal(t, 3, as.PendingCount())

	as.Kill(1)
	wg0.Done()

	require.Eventually(t, func() bool {
		return task1Ran.Load()
	}, 2*time.Second, 10*time.Millisecond)
	require.True(t, task1Ran.Load())
	require.True(t, task2Ran.Load())
	require.False(t, task3Ran.Load())
	require.False(t, task4Ran.Load())
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

		as.Schedule(task, dep, 0, i == 0 || hasFinished)
	}
}

func makeDigest(t *testing.T) simplex.Digest {
	var dig simplex.Digest
	_, err := rand.Read(dig[:])
	require.NoError(t, err)
	return dig
}

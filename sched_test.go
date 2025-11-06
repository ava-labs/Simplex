// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"crypto/rand"
	rand2 "math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"

	"github.com/stretchr/testify/require"
)

func noopPredicate() bool {
	return true
}

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

func TestExecuteDependents(t *testing.T) {
	as := simplex.NewScheduler(testutil.MakeLogger(t))
	defer as.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	var ready bool

	var executed bool

	isReady := func() bool {
		return ready
	}

	as.Schedule(func() simplex.Digest {
		defer wg.Done()
		executed = true
		return simplex.Digest{}
	}, simplex.Digest{123}, isReady)

	as.ExecuteDependents(simplex.Digest{123})
	require.False(t, executed, "Task should not have executed yet")

	ready = true
	as.ExecuteDependents(simplex.Digest{123})
	wg.Wait()
	require.True(t, executed, "Task should have executed")
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
		}, dig1, noopPredicate)

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
		}, dig1, noopPredicate)

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

		as.Schedule(task, dep, func() bool {
			return i == 0 || hasFinished
		})
	}
}

func makeDigest(t *testing.T) simplex.Digest {
	var dig simplex.Digest
	_, err := rand.Read(dig[:])
	require.NoError(t, err)
	return dig
}

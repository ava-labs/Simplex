// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"crypto/rand"
	rand2 "math/rand"
	. "simplex"
	"simplex/testutil"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDependencyTree(t *testing.T) {
	dt := NewDependencies()

	for i := 0; i < 5; i++ {
		dt.Insert(Task{F: func() Digest {
			return Digest{uint8(i + 1)}
		}, Parent: Digest{uint8(i)}})
	}

	require.Equal(t, 5, dt.Size())

	for i := 0; i < 5; i++ {
		j := dt.Remove(Digest{uint8(i)})
		require.Len(t, j, 1)
		require.Equal(t, Digest{uint8(i + 1)}, j[0].F())
	}

}

func TestAsyncScheduler(t *testing.T) {
	t.Run("Executes asynchronously", func(t *testing.T) {
		as := NewScheduler(testutil.MakeLogger(t))
		defer as.Close()

		ticks := make(chan struct{})

		var wg sync.WaitGroup
		wg.Add(1)

		dig1 := makeDigest(t)
		dig2 := makeDigest(t)

		as.Schedule(func() Digest {
			defer wg.Done()
			<-ticks
			return dig2
		}, dig1, true)

		ticks <- struct{}{}
		wg.Wait()
	})

	t.Run("Does not execute when closed", func(t *testing.T) {
		as := NewScheduler(testutil.MakeLogger(t))
		ticks := make(chan struct{}, 1)

		as.Close()

		dig1 := makeDigest(t)
		dig2 := makeDigest(t)

		as.Schedule(func() Digest {
			close(ticks)
			return dig2
		}, dig1, true)

		ticks <- struct{}{}
	})

	t.Run("Executes several pending tasks concurrently in arbitrary order", func(t *testing.T) {
		as := NewScheduler(testutil.MakeLogger(t))
		defer as.Close()

		n := 9000

		var lock sync.Mutex
		finished := make(map[Digest]struct{})

		var wg sync.WaitGroup
		wg.Add(n)

		var prevTask Digest
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

func scheduleTask(lock *sync.Mutex, finished map[Digest]struct{}, dependency Digest, id Digest, wg *sync.WaitGroup, as *Scheduler, i int) func() {
	var dep Digest
	copy(dep[:], dependency[:])

	return func() {
		lock.Lock()
		defer lock.Unlock()

		_, hasFinished := finished[dep]

		task := func() Digest {
			lock.Lock()
			defer lock.Unlock()
			finished[id] = struct{}{}
			wg.Done()
			return id
		}

		as.Schedule(task, dep, i == 0 || hasFinished)
	}
}

func makeDigest(t *testing.T) Digest {
	var dig Digest
	_, err := rand.Read(dig[:])
	require.NoError(t, err)
	return dig
}

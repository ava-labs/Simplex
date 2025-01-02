// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"crypto/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type testDependsOn []int

func (m testDependsOn) dependsOn() int {
	return m[1]
}

func (m testDependsOn) id() int {
	return m[0]
}

func TestDependencyTree(t *testing.T) {
	dt := NewDependencies[int, testDependsOn]()

	for i := 0; i < 5; i++ {
		// [0] (i+1) depends on [1] (i)
		dt.Insert([]int{i + 1, i})
	}

	require.Equal(t, 5, dt.Size())

	for i := 0; i < 5; i++ {
		j := dt.Remove(i)
		require.Len(t, j, 1)
		require.Equal(t, i+1, j[0].id())
	}

}

func TestAsyncScheduler(t *testing.T) {
	t.Run("Executes asynchronously", func(t *testing.T) {
		as := NewScheduler()
		defer as.Close()

		ticks := make(chan struct{})

		var wg sync.WaitGroup
		wg.Add(1)

		dig1 := makeDigest(t)
		dig2 := makeDigest(t)

		as.Schedule(dig2, func() {
			defer wg.Done()
			<-ticks
		}, dig1, true)

		ticks <- struct{}{}
		wg.Wait()
	})

	t.Run("Does not execute when closed", func(t *testing.T) {
		as := NewScheduler()
		ticks := make(chan struct{}, 1)

		as.Close()

		dig1 := makeDigest(t)
		dig2 := makeDigest(t)

		as.Schedule(dig2, func() {
			close(ticks)
		}, dig1, true)

		ticks <- struct{}{}
	})

	t.Run("Executes several pending tasks concurrently", func(t *testing.T) {
		as := NewScheduler()
		defer as.Close()

		n := 9000

		var lock sync.Mutex
		finished := make(map[Digest]struct{})

		var wg sync.WaitGroup
		wg.Add(n)

		var prevTask Digest

		for i := 0; i < n; i++ {
			taskID := makeDigest(t)
			scheduleTask(&lock, finished, prevTask, taskID, &wg, as, i)
			// Next iteration's previous task ID is current task ID
			prevTask = taskID
		}

		wg.Wait()
	})
}

func scheduleTask(lock *sync.Mutex, finished map[Digest]struct{}, dependency Digest, id Digest, wg *sync.WaitGroup, as *scheduler, i int) {
	lock.Lock()
	defer lock.Unlock()

	_, hasFinished := finished[dependency]

	task := func() {
		lock.Lock()
		defer lock.Unlock()
		finished[id] = struct{}{}
		wg.Done()
	}

	as.Schedule(id, task, dependency, i == 0 || hasFinished)
}

func makeDigest(t *testing.T) Digest {
	var dig Digest
	_, err := rand.Read(dig[:])
	require.NoError(t, err)
	return dig
}

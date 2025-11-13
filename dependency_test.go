// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

const (
	defaultMaxDeps = uint64(1000)
	defaultWaitDuration = 500 * time.Millisecond
)

func waitNoReceive(t *testing.T, ch <-chan struct{}) {
	select {
	case <-ch:
		t.Fatal("channel unexpectedly signaled")
	case <-time.After(defaultWaitDuration):
		// good
	}
}

func waitReceive(t *testing.T, ch <-chan struct{}) {
	select {
	case <-ch:
		// good
	case <-time.After(defaultWaitDuration):
		t.Fatal("timed out waiting for signal")
	}
}

func TestBlockVerificationScheduler(t *testing.T) {
	t.Run("Schedules immediately when no dependencies", func(t *testing.T) {
		bvs := simplex.NewBlockVerificationScheduler(testutil.MakeLogger(t), defaultMaxDeps)
		defer bvs.Close()

		wg := sync.WaitGroup{}
		wg.Add(1)

		task := func() simplex.Digest {
			defer wg.Done()
			return makeDigest(t)
		}

		require.NoError(t, bvs.ScheduleTaskWithDependencies(task, nil, nil))
		wg.Wait()
	})

	t.Run("Defers until prevBlock is satisfied (manual ExecuteBlockDependents)", func(t *testing.T) {
		bvs := simplex.NewBlockVerificationScheduler(testutil.MakeLogger(t), defaultMaxDeps)
		defer bvs.Close()

		prev := makeDigest(t)
		done := make(chan struct{}, 1)

		task := func() simplex.Digest {
			done <- struct{}{}
			return makeDigest(t)
		}

		require.NoError(t, bvs.ScheduleTaskWithDependencies(task, &prev, nil))

		// Should not run until we satisfy the dependency.
		waitNoReceive(t, done)

		bvs.ExecuteBlockDependents(prev)
		waitReceive(t, done)
	})

	t.Run("Defers until all emptyRounds are satisfied", func(t *testing.T) {
		bvs := simplex.NewBlockVerificationScheduler(testutil.MakeLogger(t), defaultMaxDeps)
		defer bvs.Close()

		done := make(chan struct{}, 1)
		task := func() simplex.Digest {
			done <- struct{}{}
			return makeDigest(t)
		}

		require.NoError(t, bvs.ScheduleTaskWithDependencies(task, nil, []uint64{11, 22}))

		// Removing one is not enough.
		bvs.ExecuteEmptyRoundDependents(11)
		waitNoReceive(t, done)

		// Removing the last one triggers scheduling.
		bvs.ExecuteEmptyRoundDependents(22)
		waitReceive(t, done)
	})

	t.Run("Defers until both prevBlock and all emptyRounds are satisfied", func(t *testing.T) {
		bvs := simplex.NewBlockVerificationScheduler(testutil.MakeLogger(t), defaultMaxDeps)
		defer bvs.Close()

		prev := makeDigest(t)
		done := make(chan struct{}, 1)
		task := func() simplex.Digest {
			done <- struct{}{}
			return makeDigest(t)
		}

		require.NoError(t, bvs.ScheduleTaskWithDependencies(task, &prev, []uint64{1, 2, 3}))

		// Satisfy rounds first — still blocked by prevBlock.
		bvs.ExecuteEmptyRoundDependents(1)
		bvs.ExecuteEmptyRoundDependents(2)
		bvs.ExecuteEmptyRoundDependents(3)
		waitNoReceive(t, done)

		// Now satisfy the prevBlock and it should schedule.
		bvs.ExecuteBlockDependents(prev)
		waitReceive(t, done)
	})

	t.Run("Chained scheduling via onTaskFinished (A finishes -> B unblocked)", func(t *testing.T) {
		bvs := simplex.NewBlockVerificationScheduler(testutil.MakeLogger(t), defaultMaxDeps)
		defer bvs.Close()

		// Task A produces digest 'Aout'
		Aout := makeDigest(t)
		aRan := make(chan struct{}, 1)
		taskA := func() simplex.Digest {
			aRan <- struct{}{}
			return Aout
		}

		// Task B depends on A's output digest
		bRan := make(chan struct{}, 1)
		taskB := func() simplex.Digest {
			bRan <- struct{}{}
			return makeDigest(t)
		}

		// Schedule B with dependency; it should not run yet.
		require.NoError(t, bvs.ScheduleTaskWithDependencies(taskB, &Aout, nil))
		waitNoReceive(t, bRan)

		// Schedule A with no dependencies; when it finishes, the scheduler's
		// onTaskFinished should call ExecuteBlockDependents(Aout), unblocking B.
		require.NoError(t, bvs.ScheduleTaskWithDependencies(taskA, nil, nil))

		waitReceive(t, aRan)
		waitReceive(t, bRan)
	})

	t.Run("Max pending limit enforced (dependencies + queued)", func(t *testing.T) {
		// Set max to 1 so a single pending item trips the limit.
		const max = uint64(1)
		bvs := simplex.NewBlockVerificationScheduler(testutil.MakeLogger(t), max)
		defer bvs.Close()

		prev := makeDigest(t)

		// First: add one task with a dependency (it will sit in ds.dependencies)
		require.NoError(t, bvs.ScheduleTaskWithDependencies(func() simplex.Digest {
			return makeDigest(t)
		}, &prev, nil))

		// Second: trying to add another should exceed the limit.
		err := bvs.ScheduleTaskWithDependencies(func() simplex.Digest {
			return makeDigest(t)
		}, &prev, nil)

		require.Error(t, err)
		require.True(t, errors.Is(err, simplex.ErrTooManyPendingVerifications), "should wrap sentinel error")
	})

	t.Run("Multiple unrelated dependency resolutions don't trigger others", func(t *testing.T) {
		bvs := simplex.NewBlockVerificationScheduler(testutil.MakeLogger(t), defaultMaxDeps)
		defer bvs.Close()

		prev1 := makeDigest(t)
		prev2 := makeDigest(t)

		done1 := make(chan struct{}, 1)
		task1 := func() simplex.Digest {
			done1 <- struct{}{}
			return makeDigest(t)
		}

		done2 := make(chan struct{}, 1)
		task2 := func() simplex.Digest {
			done2 <- struct{}{}
			return makeDigest(t)
		}

		require.NoError(t, bvs.ScheduleTaskWithDependencies(task1, &prev1, []uint64{7}))
		require.NoError(t, bvs.ScheduleTaskWithDependencies(task2, &prev2, []uint64{8}))

		// Resolve only part of task1's deps; neither should run.
		bvs.ExecuteEmptyRoundDependents(7)
		waitNoReceive(t, done1)
		waitNoReceive(t, done2)

		// Resolve prev2 but not its round; still shouldn't run.
		bvs.ExecuteBlockDependents(prev2)
		waitNoReceive(t, done2)

		// Finish task1 by resolving prev1; it should run now.
		bvs.ExecuteBlockDependents(prev1)
		waitReceive(t, done1)
		// Finally resolve task2's round; it should run now.
		bvs.ExecuteEmptyRoundDependents(8)
		waitReceive(t, done2)
	})
}

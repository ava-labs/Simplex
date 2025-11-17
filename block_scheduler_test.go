// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

const (
	defaultMaxDeps      = uint64(1000)
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
		scheduler := simplex.NewScheduler(testutil.MakeLogger(t), defaultMaxDeps)
		bvs := simplex.NewBlockVerificationScheduler(testutil.MakeLogger(t), defaultMaxDeps, scheduler)
		defer bvs.Close()

		wg := sync.WaitGroup{}
		wg.Add(1)

		task := func() simplex.Digest {
			defer wg.Done()
			return makeDigest(t)
		}

		require.NoError(t, bvs.ScheduleTaskWithDependencies(task, 0, nil, nil))
		wg.Wait()
	})

	t.Run("Defers until prevBlock is satisfied (manual ExecuteBlockDependents)", func(t *testing.T) {
		scheduler := simplex.NewScheduler(testutil.MakeLogger(t), defaultMaxDeps)
		bvs := simplex.NewBlockVerificationScheduler(testutil.MakeLogger(t), defaultMaxDeps, scheduler)
		defer bvs.Close()

		prev := makeDigest(t)
		done := make(chan struct{}, 1)

		task := func() simplex.Digest {
			done <- struct{}{}
			return makeDigest(t)
		}

		require.NoError(t, bvs.ScheduleTaskWithDependencies(task, 0, &prev, nil))

		// Should not run until we satisfy the dependency.
		waitNoReceive(t, done)

		bvs.ExecuteBlockDependents(prev)
		waitReceive(t, done)
	})

	t.Run("Defers until all emptyRounds are satisfied", func(t *testing.T) {
		scheduler := simplex.NewScheduler(testutil.MakeLogger(t), defaultMaxDeps)
		bvs := simplex.NewBlockVerificationScheduler(testutil.MakeLogger(t), defaultMaxDeps, scheduler)
		defer bvs.Close()

		done := make(chan struct{}, 1)
		task := func() simplex.Digest {
			done <- struct{}{}
			return makeDigest(t)
		}

		require.NoError(t, bvs.ScheduleTaskWithDependencies(task, 0, nil, []uint64{11, 22}))

		// Removing one is not enough.
		bvs.ExecuteEmptyRoundDependents(11)
		waitNoReceive(t, done)

		// Removing the last one triggers scheduling.
		bvs.ExecuteEmptyRoundDependents(22)
		waitReceive(t, done)
	})

	t.Run("Defers until both prevBlock and all emptyRounds are satisfied", func(t *testing.T) {
		scheduler := simplex.NewScheduler(testutil.MakeLogger(t), defaultMaxDeps)
		bvs := simplex.NewBlockVerificationScheduler(testutil.MakeLogger(t), defaultMaxDeps, scheduler)
		defer bvs.Close()

		prev := makeDigest(t)
		done := make(chan struct{}, 1)
		task := func() simplex.Digest {
			done <- struct{}{}
			return makeDigest(t)
		}

		require.NoError(t, bvs.ScheduleTaskWithDependencies(task, 0, &prev, []uint64{1, 2, 3}))

		// Satisfy rounds first — still blocked by prevBlock.
		bvs.ExecuteEmptyRoundDependents(1)
		bvs.ExecuteEmptyRoundDependents(2)
		bvs.ExecuteEmptyRoundDependents(3)
		waitNoReceive(t, done)

		// Now satisfy the prevBlock and it should schedule.
		bvs.ExecuteBlockDependents(prev)
		waitReceive(t, done)
	})

	t.Run("Defers until both prevBlock and all emptyRounds are satisfied swapped order", func(t *testing.T) {
		scheduler := simplex.NewScheduler(testutil.MakeLogger(t), defaultMaxDeps)
		bvs := simplex.NewBlockVerificationScheduler(testutil.MakeLogger(t), defaultMaxDeps, scheduler)
		defer bvs.Close()

		prev := makeDigest(t)
		done := make(chan struct{}, 1)
		task := func() simplex.Digest {
			done <- struct{}{}
			return makeDigest(t)
		}

		require.NoError(t, bvs.ScheduleTaskWithDependencies(task, 0, &prev, []uint64{1, 2, 3}))

		// first satisfy prev block dep
		bvs.ExecuteBlockDependents(prev)
		waitNoReceive(t, done)

		// next satisfy the empty rounds
		bvs.ExecuteEmptyRoundDependents(1)
		bvs.ExecuteEmptyRoundDependents(2)
		bvs.ExecuteEmptyRoundDependents(3)
		waitReceive(t, done)
	})

	t.Run("Chained scheduling via onTaskFinished (A finishes -> B unblocked)", func(t *testing.T) {
		scheduler := simplex.NewScheduler(testutil.MakeLogger(t), defaultMaxDeps)
		bvs := simplex.NewBlockVerificationScheduler(testutil.MakeLogger(t), defaultMaxDeps, scheduler)
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
		require.NoError(t, bvs.ScheduleTaskWithDependencies(taskB, 0, &Aout, nil))
		waitNoReceive(t, bRan)

		// Schedule A with no dependencies; when it finishes, the scheduler's
		// onTaskFinished should call ExecuteBlockDependents(Aout), unblocking B.
		require.NoError(t, bvs.ScheduleTaskWithDependencies(taskA, 0, nil, nil))

		waitReceive(t, aRan)
		waitReceive(t, bRan)
	})

	t.Run("Max pending limit enforced (dependencies + queued)", func(t *testing.T) {
		// Set max to 1 so a single pending item trips the limit.
		const max = uint64(1)
		limitedScheduler := simplex.NewScheduler(testutil.MakeLogger(t), defaultMaxDeps)
		noLogger := testutil.MakeLogger(t)
		noLogger.Silence() // we silence because CI fails when we get WARN logs but this is expected in this test
		bvs := simplex.NewBlockVerificationScheduler(noLogger, max, limitedScheduler)
		defer bvs.Close()

		prev := makeDigest(t)

		// First: add one task with a dependency (it will sit in ds.dependencies)
		require.NoError(t, bvs.ScheduleTaskWithDependencies(func() simplex.Digest {
			return makeDigest(t)
		}, 0, &prev, nil))

		// Second: trying to add another should exceed the limit.
		err := bvs.ScheduleTaskWithDependencies(func() simplex.Digest {
			return makeDigest(t)
		}, 0, &prev, nil)

		require.ErrorIs(t, err, simplex.ErrTooManyPendingVerifications)
	})

	t.Run("Multiple unrelated dependency resolutions don't trigger others", func(t *testing.T) {
		scheduler := simplex.NewScheduler(testutil.MakeLogger(t), defaultMaxDeps)
		bvs := simplex.NewBlockVerificationScheduler(testutil.MakeLogger(t), defaultMaxDeps, scheduler)
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

		require.NoError(t, bvs.ScheduleTaskWithDependencies(task1, 0, &prev1, []uint64{7}))
		require.NoError(t, bvs.ScheduleTaskWithDependencies(task2, 0, &prev2, []uint64{8}))

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

	t.Run("RemoveOldTasks removes tasks with blockSeq <= finalized seq", func(t *testing.T) {
		scheduler := simplex.NewScheduler(testutil.MakeLogger(t), defaultMaxDeps)
		bvs := simplex.NewBlockVerificationScheduler(testutil.MakeLogger(t), defaultMaxDeps, scheduler)

		// Both tasks depend on round 10 being cleared, so neither should run yet.
		const depRound uint64 = 10

		oldRan := make(chan struct{}, 1)
		newRan := make(chan struct{}, 1)

		taskOld := func() simplex.Digest {
			oldRan <- struct{}{}
			return makeDigest(t)
		}
		taskNew := func() simplex.Digest {
			newRan <- struct{}{}
			return makeDigest(t)
		}

		// Block sequences: old=5, new=8
		// RemoveOldTasks(seq) should drop tasks with blockSeq <= seq.
		const oldSeq uint64 = 5
		const newSeq uint64 = 8

		require.NoError(t, bvs.ScheduleTaskWithDependencies(taskOld, oldSeq, nil, []uint64{depRound}))
		require.NoError(t, bvs.ScheduleTaskWithDependencies(taskNew, newSeq, nil, []uint64{depRound}))

		// Nothing should run yet.
		waitNoReceive(t, oldRan)
		waitNoReceive(t, newRan)

		// Finalize up to seq=6 — this should remove the old task (seq=5) but keep the new one (seq=8).
		bvs.RemoveOldTasks(6)

		// Now resolve the dependency round. Only the "new" task should execute.
		bvs.ExecuteEmptyRoundDependents(depRound)

		// Old was removed, so it must not run.
		waitNoReceive(t, oldRan)
		// New remains and should be scheduled.
		waitReceive(t, newRan)
	})
}

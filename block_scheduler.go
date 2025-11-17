// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

var ErrTooManyPendingVerifications = errors.New("too many blocks being verified to ingest another one")

type Scheduler interface {
	Schedule(task Task)
	Size() int
	Close()
}

// BlockDependencyManager manages block verification tasks with dependencies on previous blocks and empty rounds.
// It schedules tasks when their dependencies are resolved.
type BlockDependencyManager struct {
	lock      sync.Mutex
	logger    Logger
	scheduler Scheduler

	dependencies []*TaskWithDependents
	maxDeps      uint64
	closed       atomic.Bool
}

type TaskWithDependents struct {
	Task Task

	blockSeq    uint64 // the seq of the block being verified
	prevBlock   *Digest
	emptyRounds map[uint64]struct{}
}

func (t *TaskWithDependents) isReady() bool {
	return t.prevBlock == nil && len(t.emptyRounds) == 0
}

func (t *TaskWithDependents) String() string {
	return fmt.Sprintf("BlockVerificationTask{blockSeq: %d, prevBlock: %v, emptyRounds: %v}", t.blockSeq, t.prevBlock, slices.Collect(maps.Keys(t.emptyRounds)))
}

func NewBlockVerificationScheduler(logger Logger, maxDeps uint64, scheduler Scheduler) *BlockDependencyManager {
	b := &BlockDependencyManager{
		logger:    logger,
		maxDeps:   maxDeps,
		scheduler: scheduler,
	}

	b.logger.Debug("Created BlockVerificationScheduler", zap.Uint64("maxDeps", maxDeps))
	return b
}

// ExecuteBlockDependents removes the given digest from dependent tasks and schedules any whose dependencies are now resolved.
func (bs *BlockDependencyManager) ExecuteBlockDependents(prev Digest) {
	bs.lock.Lock()
	defer bs.lock.Unlock()

	remainingDeps := make([]*TaskWithDependents, 0, len(bs.dependencies))

	for _, taskWithDeps := range bs.dependencies {
		if taskWithDeps.prevBlock != nil && *taskWithDeps.prevBlock == prev {
			taskWithDeps.prevBlock = nil
		}

		if taskWithDeps.isReady() {
			bs.logger.Debug("Scheduling block verification task as all dependencies are met", zap.Stringer("taskID", prev))
			bs.scheduler.Schedule(taskWithDeps.Task)
			continue
		}

		bs.logger.Debug("Block verification task has unsatisfied dependencies",
			zap.Any("prevBlock", prev),
			zap.Stringer("task", taskWithDeps),
		)

		remainingDeps = append(remainingDeps, taskWithDeps)
	}

	bs.dependencies = remainingDeps
}

// ExecuteEmptyRoundDependents removes the given empty round from dependent tasks and schedules any whose dependencies are now resolved.
func (bs *BlockDependencyManager) ExecuteEmptyRoundDependents(emptyRound uint64) {
	bs.lock.Lock()
	defer bs.lock.Unlock()

	remainingDeps := make([]*TaskWithDependents, 0, len(bs.dependencies))

	for _, taskWithDeps := range bs.dependencies {
		delete(taskWithDeps.emptyRounds, emptyRound)

		if taskWithDeps.isReady() {
			bs.logger.Debug("Scheduling block verification task as all dependencies are met", zap.Stringer("task", taskWithDeps))
			bs.scheduler.Schedule(taskWithDeps.Task)
			continue
		}

		bs.logger.Debug("Block verification task has unsatisfied dependencies",
			zap.Any("emptyRound", emptyRound),
			zap.Stringer("task", taskWithDeps),
		)
		remainingDeps = append(remainingDeps, taskWithDeps)
	}

	bs.dependencies = remainingDeps
}

func (bs *BlockDependencyManager) ScheduleTaskWithDependencies(task Task, blockSeq uint64, prev *Digest, emptyRounds []uint64) error {
	bs.lock.Lock()
	defer bs.lock.Unlock()

	if bs.closed.Load() {
		return nil
	}

	wrappedTask := func() Digest {
		id := task()
		bs.ExecuteBlockDependents(id)
		return id
	}

	totalSize := uint64(len(bs.dependencies) + bs.scheduler.Size())
	if totalSize >= bs.maxDeps {
		bs.logger.Warn("Too many blocks being verified to ingest another one", zap.Uint64("pendingBlocks", totalSize))
		return fmt.Errorf("%w: %d pending verifications (max %d)", ErrTooManyPendingVerifications, totalSize, bs.maxDeps)
	}

	if prev == nil && len(emptyRounds) == 0 {
		bs.logger.Debug("Scheduling block verification task with no dependencies", zap.Uint64("blockSeq", blockSeq))
		bs.scheduler.Schedule(wrappedTask)
		return nil
	}

	bs.logger.Debug("Adding block verification task with dependencies", zap.Any("prevBlock", prev), zap.Uint64s("emptyRounds", emptyRounds))
	emptyRoundsSet := make(map[uint64]struct{})
	for _, round := range emptyRounds {
		emptyRoundsSet[round] = struct{}{}
	}

	bs.dependencies = append(bs.dependencies, &TaskWithDependents{
		Task:        wrappedTask,
		prevBlock:   prev,
		emptyRounds: emptyRoundsSet,
		blockSeq:    blockSeq,
	})

	return nil
}

// We can remove all tasks that have an empty notarization dependency for a round that has been finalized.
func (bs *BlockDependencyManager) RemoveOldTasks(seq uint64) {
	bs.lock.Lock()
	defer bs.lock.Unlock()

	remainingDeps := make([]*TaskWithDependents, 0, len(bs.dependencies))
	for _, taskWithDeps := range bs.dependencies {
		if taskWithDeps.blockSeq <= seq {
			bs.logger.Debug("Removing block verification task as its block seq is less than or equal to finalized seq", zap.Uint64("blockSeq", taskWithDeps.blockSeq), zap.Uint64("finalizedSeq", seq))
			continue
		}
		remainingDeps = append(remainingDeps, taskWithDeps)
	}

	bs.dependencies = remainingDeps
}

func (bs *BlockDependencyManager) Close() {
	bs.closed.Store(true)
	bs.scheduler.Close()
}

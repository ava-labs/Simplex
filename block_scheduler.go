package simplex

import (
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

var ErrTooManyPendingVerifications = errors.New("too many blocks being verified to ingest another one")

type BlockVerificationScheduler struct {
	lock      sync.Mutex
	logger    Logger
	scheduler *Scheduler

	dependencies []TaskWithDependents
	maxDeps      uint64
}

type TaskWithDependents struct {
	Task Task

	prevBlock   *Digest
	emptyRounds map[uint64]struct{}
}

func NewBlockVerificationScheduler(logger Logger, maxDeps uint64) *BlockVerificationScheduler {
	b := &BlockVerificationScheduler{
		logger:  logger,
		maxDeps: maxDeps,
	}

	b.scheduler = NewScheduler(logger, maxDeps, b.ExecuteBlockDependents)
	b.logger.Debug("Created BlockVerificationScheduler", zap.Uint64("maxDeps", maxDeps))
	return b
}

// ExecuteBlockDependents removes the given digest from dependent tasks and schedules any whose dependencies are now resolved.
func (bs *BlockVerificationScheduler) ExecuteBlockDependents(digest Digest) {
	bs.lock.Lock()
	defer bs.lock.Unlock()

	var remainingDeps []TaskWithDependents

	for _, taskWithDeps := range bs.dependencies {
		if taskWithDeps.prevBlock != nil && *taskWithDeps.prevBlock == digest {
			taskWithDeps.prevBlock = nil
		}

		if len(taskWithDeps.emptyRounds) == 0 && taskWithDeps.prevBlock == nil {
			bs.logger.Debug("Scheduling block verification task as all dependencies are met", zap.Stringer("taskID", digest))
			bs.scheduler.Schedule(taskWithDeps.Task)
			continue
		}

		remainingDeps = append(remainingDeps, taskWithDeps)
	}

	bs.dependencies = remainingDeps
}

// ExecuteEmptyRoundDependents removes the given empty round from dependent tasks and schedules any whose dependencies are now resolved.
func (bs *BlockVerificationScheduler) ExecuteEmptyRoundDependents(emptyRound uint64) {
	bs.lock.Lock()
	defer bs.lock.Unlock()

	var remainingDeps []TaskWithDependents

	for _, taskWithDeps := range bs.dependencies {
		delete(taskWithDeps.emptyRounds, emptyRound)

		if len(taskWithDeps.emptyRounds) == 0 && taskWithDeps.prevBlock == nil {
			bs.logger.Debug("Scheduling block verification task as all dependencies are met")
			bs.scheduler.Schedule(taskWithDeps.Task)
			continue
		}

		remainingDeps = append(remainingDeps, taskWithDeps)
	}

	bs.dependencies = remainingDeps
}

func (bs *BlockVerificationScheduler) ScheduleTaskWithDependencies(task Task, prevBlock *Digest, emptyRounds []uint64) error {
	bs.lock.Lock()
	defer bs.lock.Unlock()

	totalSize := uint64(len(bs.dependencies) + bs.scheduler.Size())
	if totalSize >= bs.maxDeps {
		bs.logger.Warn("Too many blocks being verified to ingest another one", zap.Uint64("pendingBlocks", totalSize))
		return fmt.Errorf("%w: %d pending verifications (max %d)", ErrTooManyPendingVerifications, totalSize, bs.maxDeps)
	}

	if prevBlock == nil && len(emptyRounds) == 0 {
		// fmt.Println("No dependencies, scheduling directly:", task)
		bs.logger.Debug("Scheduling block verification task with no dependencies")
		bs.scheduler.Schedule(task)
		return nil
	}

	bs.logger.Debug("Adding block verification task with dependencies", zap.Any("prevBlock", prevBlock), zap.Uint64s("emptyRounds", emptyRounds))
	emptyRoundsSet := make(map[uint64]struct{})
	for _, round := range emptyRounds {
		emptyRoundsSet[round] = struct{}{}
	}

	bs.dependencies = append(bs.dependencies, TaskWithDependents{
		Task:        task,
		prevBlock:   prevBlock,
		emptyRounds: emptyRoundsSet,
	})

	return nil
}

func (bs *BlockVerificationScheduler) Close() {
	bs.scheduler.Close()
}

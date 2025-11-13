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
func (ds *BlockVerificationScheduler) ExecuteBlockDependents(digest Digest) {
	ds.lock.Lock()
	defer ds.lock.Unlock()

	var remainingDeps []TaskWithDependents

	for _, taskWithDeps := range ds.dependencies {
		if taskWithDeps.prevBlock != nil && *taskWithDeps.prevBlock == digest {
			taskWithDeps.prevBlock = nil
		}

		if len(taskWithDeps.emptyRounds) == 0 && taskWithDeps.prevBlock == nil {
			ds.logger.Debug("Scheduling block verification task as all dependencies are met", zap.Stringer("taskID", digest))
			ds.scheduler.Schedule(taskWithDeps.Task)
			continue
		}

		remainingDeps = append(remainingDeps, taskWithDeps)
	}

	ds.dependencies = remainingDeps
}

// ExecuteEmptyRoundDependents removes the given empty round from dependent tasks and schedules any whose dependencies are now resolved.
func (ds *BlockVerificationScheduler) ExecuteEmptyRoundDependents(emptyRound uint64) {
	ds.lock.Lock()
	defer ds.lock.Unlock()

	var remainingDeps []TaskWithDependents

	for _, taskWithDeps := range ds.dependencies {
		delete(taskWithDeps.emptyRounds, emptyRound)

		if len(taskWithDeps.emptyRounds) == 0 && taskWithDeps.prevBlock == nil {
			ds.logger.Debug("Scheduling block verification task as all dependencies are met")
			ds.scheduler.Schedule(taskWithDeps.Task)
			continue
		}

		remainingDeps = append(remainingDeps, taskWithDeps)
	}

	ds.dependencies = remainingDeps
}

func (ds *BlockVerificationScheduler) ScheduleTaskWithDependencies(task Task, prevBlock *Digest, emptyRounds []uint64) error {
	ds.lock.Lock()
	defer ds.lock.Unlock()

	totalSize := uint64(len(ds.dependencies) + ds.scheduler.Size())
	if totalSize >= ds.maxDeps {
		ds.logger.Warn("Too many blocks being verified to ingest another one", zap.Uint64("pendingBlocks", totalSize))
		return fmt.Errorf("%w: %d pending verifications (max %d)", ErrTooManyPendingVerifications, totalSize, ds.maxDeps)
	}

	if prevBlock == nil && len(emptyRounds) == 0 {
		// fmt.Println("No dependencies, scheduling directly:", task)
		ds.logger.Debug("Scheduling block verification task with no dependencies")
		ds.scheduler.Schedule(task)
		return nil
	}

	ds.logger.Debug("Adding block verification task with dependencies", zap.Any("prevBlock", prevBlock), zap.Uint64s("emptyRounds", emptyRounds))
	emptyRoundsSet := make(map[uint64]struct{})
	for _, round := range emptyRounds {
		emptyRoundsSet[round] = struct{}{}
	}

	ds.dependencies = append(ds.dependencies, TaskWithDependents{
		Task:        task,
		prevBlock:   prevBlock,
		emptyRounds: emptyRoundsSet,
	})

	return nil
}

func (ds *BlockVerificationScheduler) Close() {
	ds.scheduler.Close()
}

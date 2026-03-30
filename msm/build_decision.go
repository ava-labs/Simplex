// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// blockBuildingDecision represents the decision of whether we should build a block at the current time,
// and if so, whether we should also transition to a new epoch along the way.
type blockBuildingDecision int8

const (
	blockBuildingDecisionUndefined blockBuildingDecision = iota
	blockBuildingDecisionBuildBlock // We should build a block, and we don't need to transition to a new epoch.
	blockBuildingDecisionTransitionEpoch // We should transition to a new epoch immediately, but we don't need to build a block.
	blockBuildingDecisionBuildBlockAndTransitionEpoch // We should build a block and transition to a new epoch along the way.
	blockBuildingDecisionContextCanceled
)

// PChainProgressListener listens for changes in the P-chain height.
type PChainProgressListener interface {
	// WaitForProgress should block until either the context is cancelled, or the P-chain height has increased from the provided pChainHeight.
	WaitForProgress(ctx context.Context, pChainHeight uint64) error
}

type blockBuildingDecider struct {
	logger Logger
	maxBlockBuildingWaitTime time.Duration
	pChainlistener           PChainProgressListener
	waitForPendingBlock      func(ctx context.Context)
	shouldTransitionEpoch    func(pChainHeight uint64) (bool, error)
	getPChainHeight          func() uint64
}

// shouldBuildBlock determines whether we should build a block at the current time,
// based on the current P-chain height and whether we should transition to a new epoch.
// It returns a blockBuildingDecision, the current P-chain height sampled at the time of deciding,
// and an error if the decision cannot be made.
// The P-chain height is returned because sampling the P-chain height afterwards might be inconsistent with the decision that was made.
func (bbd *blockBuildingDecider) shouldBuildBlock(
	ctx context.Context,
) (blockBuildingDecision, uint64, error) {
	for {
		pChainHeight := bbd.getPChainHeight()

		shouldTransitionEpoch, err := bbd.shouldTransitionEpoch(pChainHeight)
		if err != nil {
			return blockBuildingDecisionUndefined, 0, err
		}

		if shouldTransitionEpoch {
			// If we should transition to a new epoch, maybe we can also build a block along the way.
			return bbd.maybeBuildBlockWithEpochTransition(ctx), pChainHeight, nil
		}

		// Else, we don't need to transition to a new epoch, but maybe we should build a block.
		// We wait for either the P-chain height to change, or for a block to be ready to be built.
		bbd.waitForPChainChangeOrPendingBlock(ctx, pChainHeight)

		// If the context was cancelled in the meantime, abandon evaluation.
		if bbd.wasContextCanceled(ctx) {
			return blockBuildingDecisionContextCanceled, 0, nil
		}

		// If we've reached here, either the P-chain height has changed, or a block is ready to be built.

		// If the P-chain height changed, re-evaluate again whether we should transition to a new epoch,
		// or continue waiting to build a block.
		if bbd.getPChainHeight() != pChainHeight {
			continue
		}

		// Else, we have reached here because a block is ready to be built, and the P-chain height has not changed,
		// which means we should build a block.

		return blockBuildingDecisionBuildBlock, pChainHeight, nil
	}
}

// waitForPChainChangeOrPendingBlock waits until either the given P-chain height changes from the provided pChainHeight,
// or a block is ready to be built.
func (bbd *blockBuildingDecider) waitForPChainChangeOrPendingBlock(ctx context.Context, pChainHeight uint64) {
	pChainAwareContext, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Add(1)

	defer wg.Wait()
	defer cancel()

	go func() {
		defer wg.Done()
		err := bbd.pChainlistener.WaitForProgress(pChainAwareContext, pChainHeight)
		if err != nil && pChainAwareContext.Err() == nil{
			bbd.logger.Warn("error while waiting for P-chain progress", zap.Error(err))
		}
		cancel()
	}()

	bbd.waitForPendingBlock(pChainAwareContext)
}

// maybeBuildBlockWithEpochTransition decides if we should build a block while transitioning to a new epoch.
// It waits up to a limited amount of time (bbd.maxBlockBuildingWaitTime) for a block to be ready to be built,
// and if no block is ready by then, it returns the decision to transition epoch without building a block.
// Otherwise, it returns the decision to build a block and transition epoch along the way.
func (bbd *blockBuildingDecider) maybeBuildBlockWithEpochTransition(ctx context.Context) blockBuildingDecision {
	impatientContext, cancel := context.WithTimeout(ctx, bbd.maxBlockBuildingWaitTime)
	defer cancel()

	// We should transition to a new epoch, so we wait some time just in case we can also build a block along the way.
	// waitForPendingBlock will return in case a block is ready to be built, or when the context times out.
	bbd.waitForPendingBlock(impatientContext)

	if impatientContext.Err() != nil {
		// Check if we have returned because the parent context was cancelled
		if bbd.wasContextCanceled(ctx) {
			return blockBuildingDecisionContextCanceled
		}
		// We have returned from waitForPendingBlock because the context has timed out, which means we don't need to build a block.
		return blockBuildingDecisionTransitionEpoch
	}

	// Block is ready to be built
	return blockBuildingDecisionBuildBlockAndTransitionEpoch
}

func (bbd *blockBuildingDecider) wasContextCanceled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

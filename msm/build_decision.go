// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"context"
	"sync"
	"time"
)

type blockBuildingDecision int8

const (
	blockBuildingDecisionUndefined blockBuildingDecision = iota
	blockBuildingDecisionBuildBlock
	blockBuildingDecisionTransitionEpoch
	blockBuildingDecisionBuildBlockAndTransitionEpoch
	blockBuildingDecisionContextCanceled
)

// PChainProgressListener listens for changes in the P-chain height.
type PChainProgressListener interface {
	// WaitForProgress should block until either the context is cancelled, or the P-chain height has increased from the provided pChainHeight.
	WaitForProgress(ctx context.Context, pChainHeight uint64)
}

type blockBuildingDecider struct {
	maxBlockBuildingWaitTime time.Duration
	pChainlistener           PChainProgressListener
	waitForPendingBlock      func(ctx context.Context)
	shouldTransitionEpoch    func() (bool, error)
	getPChainHeight          func() uint64
}

func (bbd *blockBuildingDecider) shouldBuildBlock(
	ctx context.Context,
) (blockBuildingDecision, uint64, error) {
	for {
		pChainHeight := bbd.getPChainHeight()

		shouldTransitionEpoch, err := bbd.shouldTransitionEpoch()
		if err != nil {
			return blockBuildingDecisionUndefined, 0, err
		}
		if shouldTransitionEpoch {
			// If we should transition to a new epoch, maybe we can also build a block along the way.
			return bbd.maybeBuildBlockWithEpochTransition(ctx), pChainHeight, nil
		}

		bbd.waitForPChainChangeOrPendingBlock(ctx, pChainHeight)

		// If the context was cancelled in the meantime, abandon evaluation.
		if bbd.wasContextCanceled(ctx) {
			return blockBuildingDecisionContextCanceled, 0, nil
		}

		// If the P-chain height changed, re-evaluate again whether we should transition to a new epoch,
		// or continue waiting to build a block.
		if bbd.getPChainHeight() != pChainHeight {
			continue
		}

		// Else, we have reached here because waitForPendingBlock returned without the P-chain height changing,
		// which means we should build a block.

		return blockBuildingDecisionBuildBlock, pChainHeight, nil
	}
}

func (bbd *blockBuildingDecider) waitForPChainChangeOrPendingBlock(ctx context.Context, pChainHeight uint64) {
	pChainAwareContext, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Add(1)

	defer wg.Wait()
	defer cancel()

	go func() {
		defer wg.Done()
		bbd.pChainlistener.WaitForProgress(pChainAwareContext, pChainHeight)
		cancel()
	}()

	bbd.waitForPendingBlock(pChainAwareContext)
}

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
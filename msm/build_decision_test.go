// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakePChainListener struct {
	onListen func(ctx context.Context, pChainHeight uint64)
}

func (f *fakePChainListener) WaitForProgress(ctx context.Context, pChainHeight uint64) error {
	f.onListen(ctx, pChainHeight)
	return nil // We don't do anything with the error but log it, so it's fine to always return nil here.
}

func TestShouldBuildBlock_VMSignalsBlock(t *testing.T) {
	// No epoch transition needed, VM signals a block is ready.
	bbd := &blockBuildingDecider{
		maxBlockBuildingWaitTime: time.Second,
		pChainlistener: &fakePChainListener{
			onListen: func(ctx context.Context, _ uint64) {
				<-ctx.Done()
			},
		},
		waitForPendingBlock:   func(ctx context.Context) {},
		shouldTransitionEpoch: func(uint64) (bool, error) { return false, nil },
		getPChainHeight:       func() uint64 { return 100 },
	}

	result, pChainHeight, err := bbd.shouldBuildBlock(context.Background())
	require.NoError(t, err)
	require.Equal(t, blockBuildingDecisionBuildBlock, result)
	require.Equal(t, uint64(100), pChainHeight)
}

func TestShouldBuildBlock_ContextCanceled(t *testing.T) {
	// No epoch transition, parent context is cancelled while waiting.
	ctx, cancel := context.WithCancel(context.Background())

	bbd := &blockBuildingDecider{
		maxBlockBuildingWaitTime: time.Second,
		pChainlistener: &fakePChainListener{
			onListen: func(ctx context.Context, _ uint64) {
				<-ctx.Done()
			},
		},
		waitForPendingBlock: func(ctx context.Context) {
			cancel()
			<-ctx.Done()
		},
		shouldTransitionEpoch: func(uint64) (bool, error) { return false, nil },
		getPChainHeight:       func() uint64 { return 100 },
	}

	result, _, err := bbd.shouldBuildBlock(ctx)
	require.NoError(t, err)
	require.Equal(t, blockBuildingDecisionContextCanceled, result)
}

func TestShouldBuildBlock_PChainHeightChangeTriggersEpochTransition(t *testing.T) {
	// First iteration: no epoch transition, P-chain listener fires (height changes).
	// Second iteration: shouldTransitionEpoch returns true, VM doesn't signal a block before timeout.
	var pChainHeight atomic.Uint64
	pChainHeight.Store(100)

	var calls atomic.Int32

	bbd := &blockBuildingDecider{
		maxBlockBuildingWaitTime: 10 * time.Millisecond,
		pChainlistener: &fakePChainListener{
			onListen: func(ctx context.Context, height uint64) {
				// On the first call, simulate a P-chain height change.
				if height == 100 {
					pChainHeight.Store(200)
					return
				}
				<-ctx.Done()
			},
		},
		waitForPendingBlock: func(ctx context.Context) {
			<-ctx.Done()
		},
		shouldTransitionEpoch: func(uint64) (bool, error) {
			return calls.Add(1) > 1, nil
		},
		getPChainHeight: func() uint64 { return pChainHeight.Load() },
	}

	result, resultPChainHeight, err := bbd.shouldBuildBlock(context.Background())
	require.NoError(t, err)
	require.Equal(t, blockBuildingDecisionTransitionEpoch, result)
	require.Equal(t, uint64(200), resultPChainHeight)
	require.GreaterOrEqual(t, int(calls.Load()), 2)
}

func TestShouldBuildBlock_PChainHeightChangeButNoEpochTransition(t *testing.T) {
	// P-chain height changes on first iteration but shouldTransitionEpoch stays false.
	// On second iteration, VM signals a block.
	var pChainHeight atomic.Uint64
	pChainHeight.Store(100)

	var iteration atomic.Int32

	bbd := &blockBuildingDecider{
		maxBlockBuildingWaitTime: time.Second,
		pChainlistener: &fakePChainListener{
			onListen: func(ctx context.Context, height uint64) {
				if height == 100 {
					pChainHeight.Store(200)
					return
				}
				<-ctx.Done()
			},
		},
		waitForPendingBlock: func(ctx context.Context) {
			// First iteration: block on context (P-chain listener will cancel it).
			// Second iteration: return immediately (VM has a block).
			if iteration.Add(1) == 1 {
				<-ctx.Done()
				return
			}
		},
		shouldTransitionEpoch: func(uint64) (bool, error) { return false, nil },
		getPChainHeight:       func() uint64 { return pChainHeight.Load() },
	}

	result, resultPChainHeight, err := bbd.shouldBuildBlock(context.Background())
	require.NoError(t, err)
	require.Equal(t, blockBuildingDecisionBuildBlock, result)
	require.Equal(t, uint64(200), resultPChainHeight)
}

func TestShouldBuildBlock_EpochTransitionWithVMBlock(t *testing.T) {
	// Epoch transition needed, but VM signals a block before the timeout.
	bbd := &blockBuildingDecider{
		maxBlockBuildingWaitTime: time.Second,
		pChainlistener: &fakePChainListener{
			onListen: func(ctx context.Context, _ uint64) {
				<-ctx.Done()
			},
		},
		waitForPendingBlock:   func(ctx context.Context) {},
		shouldTransitionEpoch: func(uint64) (bool, error) { return true, nil },
		getPChainHeight:       func() uint64 { return 100 },
	}

	result, pChainHeight, err := bbd.shouldBuildBlock(context.Background())
	require.NoError(t, err)
	require.Equal(t, blockBuildingDecisionBuildBlockAndTransitionEpoch, result)
	require.Equal(t, uint64(100), pChainHeight)
}

func TestShouldBuildBlock_EpochTransitionWithoutVMBlock(t *testing.T) {
	// Epoch transition needed, VM doesn't signal a block before timeout.
	bbd := &blockBuildingDecider{
		maxBlockBuildingWaitTime: 10 * time.Millisecond,
		pChainlistener: &fakePChainListener{
			onListen: func(ctx context.Context, _ uint64) {
				<-ctx.Done()
			},
		},
		waitForPendingBlock: func(ctx context.Context) {
			<-ctx.Done()
		},
		shouldTransitionEpoch: func(uint64) (bool, error) { return true, nil },
		getPChainHeight:       func() uint64 { return 100 },
	}

	result, pChainHeight, err := bbd.shouldBuildBlock(context.Background())
	require.NoError(t, err)
	require.Equal(t, blockBuildingDecisionTransitionEpoch, result)
	require.Equal(t, uint64(100), pChainHeight)
}

func TestShouldBuildBlock_EpochTransitionContextCanceled(t *testing.T) {
	// Epoch transition needed, but parent context is cancelled during the wait.
	ctx, cancel := context.WithCancel(context.Background())

	bbd := &blockBuildingDecider{
		maxBlockBuildingWaitTime: time.Second,
		pChainlistener: &fakePChainListener{
			onListen: func(ctx context.Context, _ uint64) {
				<-ctx.Done()
			},
		},
		waitForPendingBlock: func(ctx context.Context) {
			cancel()
			<-ctx.Done()
		},
		shouldTransitionEpoch: func(uint64) (bool, error) { return true, nil },
		getPChainHeight:       func() uint64 { return 100 },
	}

	result, _, err := bbd.shouldBuildBlock(ctx)
	require.NoError(t, err)
	require.Equal(t, blockBuildingDecisionContextCanceled, result)
}

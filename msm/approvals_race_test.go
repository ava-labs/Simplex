// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/simplex/common"
	"github.com/ava-labs/simplex/testutil"

	"github.com/stretchr/testify/require"
)

// TestBuildVerifyApprovalStoreDataRace reproduces the data race on
// sm.approvalStore using the real production pairing: one goroutine building a
// block while another verifies one, both against the same StateMachine.
//
// In production these run on separate scheduler goroutines (simplex/epoch.go
// creates a buildBlockScheduler and a blockVerificationScheduler, each with its
// own run goroutine) sharing one msm. The build path
// (BuildBlock -> buildBlockCollectingApprovals -> computeNewApprovals) reads
// sm.approvalStore WITHOUT sm.lock (msm.go:1125), while the verify path
// (VerifyBlock -> verifyCollectingApprovalsBlock) re-initializes it UNDER
// sm.lock (msm.go:979 -> :290). maybeInitializeApprovalStore runs before the
// verify path's signature checks, so the write happens even if verification
// ultimately rejects the block.
//
// GetValidatorSet alternates between two validator sets so both paths keep
// swapping the store, maximizing the window in which the build path's unlocked
// read overlaps the verify path's locked write.
//
// Run with the race detector:
//
//	go test -race -run TestBuildVerifyApprovalStoreDataRace ./msm/
//
// It fails (race detected on sm.approvalStore) until the read is brought under
// sm.lock.
func TestBuildVerifyApprovalStoreDataRace(t *testing.T) {
	sm, tc := newStateMachine(t)
	sm.AuxiliaryInfoApp = &noopTestAuxInfoApp{}
	sm.SignatureAggregatorCreator = func(_ []common.Node) common.SignatureAggregator {
		return concatAggregator{}
	}

	// Two validator sets that are not Equal, both containing MyNodeID.
	setA := NodeBLSMappings{
		{NodeID: nodeID(sm.MyNodeID), BLSKey: []byte{1}, Weight: 1},
		{NodeID: nodeID{0xBB}, BLSKey: []byte{2}, Weight: 1},
	}
	setB := NodeBLSMappings{
		{NodeID: nodeID(sm.MyNodeID), BLSKey: []byte{1}, Weight: 1},
		{NodeID: nodeID{0xBB}, BLSKey: []byte{2}, Weight: 1},
		{NodeID: nodeID{0xCC}, BLSKey: []byte{3}, Weight: 1},
	}
	// Stable set while we build the block that the verifier goroutine will replay.
	tc.validatorSetRetriever.result = setB

	// A parent block already mid epoch-transition (NextPChainReferenceHeight > 0)
	// so both building and verifying on top of it land in the collecting-approvals
	// state that touches sm.approvalStore.
	const parentSeq = uint64(10)
	parent := StateMachineBlock{
		InnerBlock: &testutil.InnerBlock{TS: time.Now(), BlockHeight: 1, Content: []byte{0xAA}},
		Metadata: StateMachineMetadata{
			PChainHeight: 100,
			SimplexProtocolMetadata: (&common.ProtocolMetadata{
				Seq: parentSeq, Round: 5, Epoch: 1,
			}).Bytes(),
			SimplexEpochInfo: SimplexEpochInfo{
				PChainReferenceHeight:     100,
				EpochNumber:               1,
				NextPChainReferenceHeight: 200,
				PrevVMBlockSeq:            parentSeq - 1,
			},
		},
	}
	tc.blockStore[parentSeq] = &outerBlock{block: parent}
	tc.blockBuilder.Block = &testutil.InnerBlock{TS: time.Now(), BlockHeight: 2, Content: []byte{0x01}}

	md := common.ProtocolMetadata{Seq: parentSeq + 1, Round: 6, Epoch: 1, Prev: parent.Digest()}

	// Build the collecting-approvals block the verifier goroutine will replay.
	blk, err := sm.BuildBlock(context.Background(), md, nil)
	require.NoError(t, err)

	// Sanity check the block reaches the collecting-approvals verify path (which
	// writes sm.approvalStore) before we hammer it concurrently.
	require.NoError(t, sm.VerifyBlock(context.Background(), blk))

	// Now alternate the validator set so every build and verify re-initializes
	// the store.
	var calls atomic.Uint64
	sm.GetValidatorSet = func(uint64) (NodeBLSMappings, error) {
		if calls.Add(1)%2 == 0 {
			return setA, nil
		}
		return setB, nil
	}

	const iterations = 300
	var wg sync.WaitGroup
	wg.Add(2)

	// Build goroutine: the unlocked read of sm.approvalStore (msm.go:1125).
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_, _ = sm.BuildBlock(context.Background(), md, nil)
		}
	}()

	// Verify goroutine: the locked write of sm.approvalStore (msm.go:979 -> :290).
	// Errors are irrelevant: the write precedes the verify path's signature checks.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_ = sm.VerifyBlock(context.Background(), blk)
		}
	}()

	wg.Wait()
}

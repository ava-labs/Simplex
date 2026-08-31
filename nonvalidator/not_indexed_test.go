// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ava-labs/simplex/common"
	"github.com/ava-labs/simplex/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

// decliningStorage wraps a storage that never persists one specific block, reporting the skip
// with common.ErrBlockNotIndexed. In production that block is a Telock: it takes up a sequence
// in the dying epoch to extend it until the sealing block finalizes, and is superseded by the
// first block of the next epoch.
type decliningStorage struct {
	common.Storage
	declined common.Digest
}

func (d *decliningStorage) Index(ctx context.Context, block common.VerifiedBlock, certificate common.Finalization) error {
	if block.BlockHeader().Digest == d.declined {
		return fmt.Errorf("%w: this block is never persisted", common.ErrBlockNotIndexed)
	}
	return d.Storage.Index(ctx, block, certificate)
}

// TestNonValidatorRecoversFromBlockTheStorageDeclinedToIndex asserts that a block the storage
// refuses to persist does not stop the non-validator from accepting the block that does belong
// at that sequence.
//
// removeOldSequencesAndEpochs, which only runs after a real commit, is the only thing that
// clears an entry from incompleteSequences. Leaving the refused block there makes handleBlock
// drop the real block as a duplicate and makes handleFinalization treat its finalization as
// conflicting with the one being held - which halts the non-validator permanently.
func TestNonValidatorRecoversFromBlockTheStorageDeclinedToIndex(t *testing.T) {
	const declinedSeq = uint64(3)

	tc := newSeededChain(t, testNodes, declinedSeq-1)

	// Two different blocks claim the same sequence: the one the storage will never persist,
	// and the one that really belongs there.
	unpersistable := newBlock(declinedSeq, tc.epoch, tc.digest)
	unpersistable.Data = []byte("never persisted")
	unpersistable.ComputeDigest()

	nv, err := NewNonValidator(
		Config{
			Storage:                    &decliningStorage{Storage: tc, declined: unpersistable.Digest},
			Comm:                       testutil.NewNoopComm(tc.nodes().NodeIDs()),
			Logger:                     testutil.MakeLogger(t, 1),
			SignatureAggregatorCreator: tc.signatureAggregatorCreator,
			MaxSequenceWindow:          simplex.DefaultMaxRoundWindow,
			ID:                         testNodes[0].Id,
		},
	)
	require.NoError(t, err)

	nv.Start()
	defer nv.Stop()

	block := blockMsg(t, unpersistable, testNodes)
	require.NoError(t, nv.HandleMessage(block.msg, block.from))
	finalization := finalizationMsg(t, unpersistable, testNodes)
	require.NoError(t, nv.HandleMessage(finalization.msg, finalization.from))

	// The refused block must not be left behind as what we hold for that sequence.
	require.Eventually(t, func() bool {
		nv.lock.Lock()
		defer nv.lock.Unlock()
		_, exists := nv.incompleteSequences[declinedSeq]
		return !exists
	}, 30*time.Second, 10*time.Millisecond, "the block the storage refused must be dropped, not held for its sequence")
	require.Equal(t, declinedSeq, tc.NumBlocks(), "the refused block must not be persisted")

	// The block that does belong at that sequence is still accepted and committed.
	realBlock := tc.appendBlock()
	block = blockMsg(t, realBlock, testNodes)
	require.NoError(t, nv.HandleMessage(block.msg, block.from))
	finalization = finalizationMsg(t, realBlock, testNodes)
	require.NoError(t, nv.HandleMessage(finalization.msg, finalization.from))

	require.Eventually(t, func() bool {
		return tc.NumBlocks() == declinedSeq+1
	}, 30*time.Second, 10*time.Millisecond, "the block that belongs at the sequence should have been committed")

	committed, _, err := tc.Retrieve(declinedSeq)
	require.NoError(t, err)
	require.Equal(t, realBlock.BlockHeader().Digest, committed.BlockHeader().Digest)

	nv.lock.Lock()
	defer nv.lock.Unlock()
	require.NoError(t, nv.haltedError, "the non-validator must not halt over a block the storage refused")
}

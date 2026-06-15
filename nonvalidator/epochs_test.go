// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"context"
	"testing"

	"github.com/ava-labs/simplex/common"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

// sealingTestBlock wraps testutil.TestBlock so tests can control whether the
// block reports itself as a sealing block.
type sealingTestBlock struct {
	*testutil.TestBlock
	sealingInfo *common.SealingBlockInfo
}

func (b *sealingTestBlock) SealingBlockInfo() *common.SealingBlockInfo {
	return b.sealingInfo
}

func (b *sealingTestBlock) Verify(_ context.Context) (common.VerifiedBlock, error) {
	return b, nil
}

func newSealingTestBlock(seq, epoch uint64, prev common.Digest, sealingInfo *common.SealingBlockInfo) *sealingTestBlock {
	return &sealingTestBlock{
		TestBlock: testutil.NewTestBlock(common.ProtocolMetadata{
			Seq:   seq,
			Round: seq,
			Epoch: epoch,
			Prev:  prev,
		}, common.Blacklist{}),
		sealingInfo: sealingInfo,
	}
}

type indexedBlock struct {
	seq         uint64
	round       uint64
	epoch       uint64
	sealingInfo *common.SealingBlockInfo
}

func TestNewEpochs(t *testing.T) {
	// nonSimplexBlock represents a block from before the first ever Simplex block —
	// i.e., genesis(epoch 0) or the last block produced by the prior consensus (Snowman).
	// Its epoch is 0 and it has no sealing info.
	var nonSimplexBlock = indexedBlock{seq: 0, round: 0, epoch: 0, sealingInfo: nil}

	nodes := common.Nodes{
		{Id: common.NodeID{1}},
		{Id: common.NodeID{2}},
		{Id: common.NodeID{3}},
		{Id: common.NodeID{4}},
	}

	sigAggCreator := func(n []common.Node) common.SignatureAggregator {
		return &testutil.TestSignatureAggregator{N: len(n)}
	}

	tests := []struct {
		name          string
		blocks        []indexedBlock
		expectedErr   error
		expectedEpoch uint64
		expectEmpty   bool
		expectedLen   int
	}{
		{
			name:        "no last accepted",
			blocks:      nil,
			expectedErr: errNoGenesis,
		},
		{
			// genesis is the only block — pre-Simplex, epoch 0.
			name: "last accepted is genesis",
			blocks: []indexedBlock{
				nonSimplexBlock,
			},
			expectEmpty: true,
		},
		{
			// the latest block is itself a sealing block at seq 2.
			name: "last accepted is sealing",
			blocks: []indexedBlock{
				nonSimplexBlock,
				{seq: 1, round: 1, epoch: 1, sealingInfo: &common.SealingBlockInfo{Epoch: 1, ValidatorSet: nodes}},
				{seq: 2, round: 2, epoch: 2, sealingInfo: &common.SealingBlockInfo{Epoch: 2, ValidatorSet: nodes}},
			},
			expectedEpoch: 2,
			expectedLen:   1,
		},
		{
			// the latest block is not a sealing block; epoch field points back to
			// the sealing block at seq 1.
			name: "last accepted is not sealing",
			blocks: []indexedBlock{
				nonSimplexBlock,
				{seq: 1, round: 1, epoch: 1, sealingInfo: &common.SealingBlockInfo{Epoch: 1, ValidatorSet: nodes}},
				{seq: 2, round: 2, epoch: 1, sealingInfo: nil},
				{seq: 3, round: 3, epoch: 1, sealingInfo: nil},
			},
			expectedEpoch: 1,
			expectedLen:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := testutil.NewInMemStorage()
			for _, b := range tt.blocks {
				block := newSealingTestBlock(b.seq, b.epoch, common.Digest{}, b.sealingInfo)
				require.NoError(t, storage.Index(context.Background(), block, common.Finalization{}))
			}

			epochs, err := newEpochs(storage, sigAggCreator)
			require.ErrorIs(t, err, tt.expectedErr)
			if err != nil {
				return
			}

			if tt.expectEmpty {
				require.Empty(t, epochs)
				return
			}

			require.Len(t, epochs, tt.expectedLen)
			meta, ok := epochs[tt.expectedEpoch]
			require.True(t, ok)
			require.Equal(t, tt.expectedEpoch, meta.epoch)
			require.Equal(t, nodes, meta.nodes)
			require.Len(t, meta.nodeLookup, len(nodes))
			for _, n := range nodes {
				_, ok := meta.nodeLookup[string(n.Id)]
				require.True(t, ok)
			}
			require.NotNil(t, meta.signatureAggregator)
		})
	}
}

func TestRemoveOldEpochs(t *testing.T) {
	newEpochsMap := func() epochs {
		return epochs{
			1: &epochMetadata{epoch: 1},
			2: &epochMetadata{epoch: 2},
			3: &epochMetadata{epoch: 3},
			4: &epochMetadata{epoch: 4},
		}
	}

	tests := []struct {
		name           string
		startEpoch     uint64
		expectedEpochs []uint64
	}{
		{
			name:           "remove none",
			startEpoch:     0,
			expectedEpochs: []uint64{1, 2, 3, 4},
		},
		{
			name:           "remove some",
			startEpoch:     3,
			expectedEpochs: []uint64{3, 4},
		},
		{
			name:           "remove all",
			startEpoch:     5,
			expectedEpochs: []uint64{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEpochsMap()
			e.removeOldEpochs(tt.startEpoch)

			require.Len(t, e, len(tt.expectedEpochs))
			for _, epoch := range tt.expectedEpochs {
				_, ok := e[epoch]
				require.True(t, ok, "expected epoch %d to remain", epoch)
			}
		})
	}
}

// TestCanValidate checks whether the epochs struct properly validates higher epochs
// by keeping track of received sealing blocks.
func TestCanValidate(t *testing.T) {
	tc := newSeededChain(t, testNodes, 2)
	// create our epochs
	e3, e4, e5 := tc.appendSealing(testNodes), tc.appendSealing(testNodes), tc.appendSealing(testNodes)
	b6 := tc.appendBlock()

	sigAggCreator := func(n []common.Node) common.SignatureAggregator {
		return &testutil.TestSignatureAggregator{N: len(n)}
	}

	epochs, err := newEpochs(tc, sigAggCreator)
	require.NoError(t, err)

	// should not be able to validate anything yet
	require.False(t, epochs.canValidate(b6))
	require.False(t, epochs.canValidate(e5))
	require.False(t, epochs.canValidate(e4))
	require.False(t, epochs.canValidate(e3))

	// say epoch 5 has been seen f + 1 times
	epochs[e5.sealingInfo.Epoch] = newEpochMetadata(e5.SealingBlockInfo(), sigAggCreator)

	// we should be able to validate backwards now
	require.False(t, epochs.canValidate(b6))
	require.False(t, epochs.canValidate(e5))
	require.True(t, epochs.canValidate(e4))
	require.False(t, epochs.canValidate(e3))

	epochs[e4.sealingInfo.Epoch] = newEpochMetadata(e4.SealingBlockInfo(), sigAggCreator)
	require.False(t, epochs.canValidate(b6))
	require.False(t, epochs.canValidate(e5))
	require.False(t, epochs.canValidate(e4)) // cannot validate twice
	require.True(t, epochs.canValidate(e3))
}

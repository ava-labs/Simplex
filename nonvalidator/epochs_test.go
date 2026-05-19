// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"context"
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

// sealingTestBlock wraps testutil.TestBlock so tests can control whether the
// block reports itself as a sealing block.
type sealingTestBlock struct {
	*testutil.TestBlock
	sealingInfo *simplex.SealingBlockInfo
}

func (b *sealingTestBlock) SealingBlockInfo() *simplex.SealingBlockInfo {
	return b.sealingInfo
}

func newSealingTestBlock(seq, round, epoch uint64, sealingInfo *simplex.SealingBlockInfo) *sealingTestBlock {
	return &sealingTestBlock{
		TestBlock: testutil.NewTestBlock(simplex.ProtocolMetadata{
			Seq:   seq,
			Round: round,
			Epoch: epoch,
		}, simplex.Blacklist{}),
		sealingInfo: sealingInfo,
	}
}

func TestNewEpochs(t *testing.T) {
	nodes := simplex.Nodes{
		{Node: simplex.NodeID{1}},
		{Node: simplex.NodeID{2}},
		{Node: simplex.NodeID{3}},
		{Node: simplex.NodeID{4}},
	}

	sigAggCreator := func(n []simplex.Node) simplex.SignatureAggregator {
		return &testutil.TestSignatureAggregator{N: len(n)}
	}

	type indexedBlock struct {
		seq         uint64
		round       uint64
		epoch       uint64
		sealingInfo *simplex.SealingBlockInfo
	}

	tests := []struct {
		name          string
		blocks        []indexedBlock
		expectedErr   error
		expectedEpoch uint64
	}{
		{
			name:        "no last accepted",
			blocks:      nil,
			expectedErr: errNoGenesis,
		},
		{
			// genesis is the only block, and is itself a sealing block.
			name: "last accepted is genesis",
			blocks: []indexedBlock{
				{seq: 0, round: 0, epoch: 0, sealingInfo: &simplex.SealingBlockInfo{Epoch: 0, ValidatorSet: nodes}},
			},
			expectedEpoch: 0,
		},
		{
			// the latest block is itself a sealing block at seq 2.
			name: "last accepted is sealing",
			blocks: []indexedBlock{
				{seq: 0, round: 0, epoch: 0, sealingInfo: &simplex.SealingBlockInfo{Epoch: 0, ValidatorSet: nodes}},
				{seq: 1, round: 1, epoch: 0, sealingInfo: nil},
				{seq: 2, round: 2, epoch: 2, sealingInfo: &simplex.SealingBlockInfo{Epoch: 2, ValidatorSet: nodes}},
			},
			expectedEpoch: 2,
		},
		{
			// the latest block is not a sealing block; epoch field points back to
			// the genesis sealing block at seq 0.
			name: "last accepted is not sealing",
			blocks: []indexedBlock{
				{seq: 0, round: 0, epoch: 0, sealingInfo: &simplex.SealingBlockInfo{Epoch: 0, ValidatorSet: nodes}},
				{seq: 1, round: 1, epoch: 0, sealingInfo: nil},
				{seq: 2, round: 2, epoch: 0, sealingInfo: nil},
			},
			expectedEpoch: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := testutil.NewInMemStorage()
			for _, b := range tt.blocks {
				block := newSealingTestBlock(b.seq, b.round, b.epoch, b.sealingInfo)
				require.NoError(t, storage.Index(context.Background(), block, simplex.Finalization{}))
			}

			epochs, err := newEpochs(storage, sigAggCreator)
			require.ErrorIs(t, err, tt.expectedErr)
			if err != nil {
				return
			}
			require.Len(t, epochs, 1)

			meta, ok := epochs[tt.expectedEpoch]
			require.True(t, ok)
			require.Equal(t, tt.expectedEpoch, meta.epoch)
			require.Equal(t, nodes, meta.nodes)
			require.Len(t, meta.nodeLookup, len(nodes))
			for _, n := range nodes {
				_, ok := meta.nodeLookup[string(n.Node)]
				require.True(t, ok)
			}
			require.NotNil(t, meta.signatureAggregator)
		})
	}
}

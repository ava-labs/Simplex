// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"testing"
	"time"

	"github.com/ava-labs/simplex/avalanchego"
	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/ava-labs/simplex/testutil"
	"github.com/ava-labs/simplex/wal"

	"github.com/stretchr/testify/require"
)

// newTestParsedBlock builds a ParsedBlock with round and seq set to num.
func newTestParsedBlock(num uint64, payload string) *ParsedBlock {
	return &ParsedBlock{
		StateMachineBlock: metadata.StateMachineBlock{
			Metadata: metadata.StateMachineMetadata{
				SimplexProtocolMetadata: common.ProtocolMetadata{
					Round: num,
					Seq:   num,
				},
			},
			InnerBlock: &testInnerBlock{
				Height_: num,
				TS:      time.UnixMilli(1),
				Payload: []byte(payload),
			},
		},
	}
}

// TestCachedStorageRetrieve asserts Retrieve against an indexed block at seq 0
// and a verified but not yet indexed block at seq 5. A zero digest matches on
// seq alone, a non-zero digest must match the block's digest exactly.
func TestCachedStorageRetrieve(t *testing.T) {
	cs := NewCachedStorage(NewMockStorage(t))
	indexedBlock := newTestParsedBlock(0, "indexed")
	require.NoError(t, cs.Index(t.Context(), indexedBlock, common.Finalization{}))

	verifiedBlockSeq := uint64(5)
	verifiedBlock := newTestParsedBlock(verifiedBlockSeq, "cached")
	cached := &cachedBlock{
		ParsedBlock: verifiedBlock,
		cache:       cs,
	}
	_, err := cached.Verify(t.Context(), common.OnlyVMVerifyOpt)
	require.NoError(t, err)

	tests := []struct {
		name      string
		seq       uint64
		digest    common.Digest
		wantBlock *ParsedBlock
		wantErr   error
	}{
		{
			name:      "cached block by seq with zero digest",
			seq:       verifiedBlockSeq,
			wantBlock: verifiedBlock,
		},
		{
			name:      "cached block with matching digest",
			seq:       verifiedBlockSeq,
			digest:    cached.Digest(),
			wantBlock: verifiedBlock,
		},
		{
			name:    "cached block with mismatched digest",
			seq:     verifiedBlockSeq,
			digest:  common.Digest{1, 2, 3},
			wantErr: common.ErrBlockNotFound,
		},
		{
			name:      "uncached seq falls through to storage",
			seq:       0,
			wantBlock: indexedBlock,
		},
		{
			name:    "indexed block with mismatched digest",
			seq:     0,
			digest:  common.Digest{1, 2, 3},
			wantErr: common.ErrBlockNotFound,
		},
		{
			name:    "seq not cached or in storage",
			seq:     7,
			wantErr: common.ErrBlockNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, fin, err := cs.Retrieve(tt.seq, tt.digest)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantBlock, got)
			if tt.wantBlock == verifiedBlock {
				require.Nil(t, fin)
			}
		})
	}
}

// TestCachedStorageIndexEvictsSameSeqFork asserts that once a seq is indexed,
// a zero-digest Retrieve of that seq returns the finalized block with its
// finalization, even when a verified fork at the same seq was cached.
func TestCachedStorageIndexEvictsSameSeqFork(t *testing.T) {
	cs := NewCachedStorage(NewMockStorage(t))
	require.NoError(t, cs.Index(t.Context(), newTestParsedBlock(0, "genesis"), common.Finalization{}))

	equivocatedBlock := &cachedBlock{
		ParsedBlock: newTestParsedBlock(1, "fork"),
		cache:       cs,
	}
	_, err := equivocatedBlock.Verify(t.Context(), common.OnlyVMVerifyOpt)
	require.NoError(t, err)

	finalized := newTestParsedBlock(1, "finalized")
	require.NoError(t, cs.Index(t.Context(), finalized, common.Finalization{}))

	retrievedBlock, fin, err := cs.Retrieve(1, common.Digest{})
	require.NoError(t, err)
	require.Equal(t, finalized.BlockHeader().Digest, retrievedBlock.BlockHeader().Digest)
	require.NotNil(t, fin)
}

// TestCachedStoragePopulatedByWal asserts that a block restored from the WAL on
// startup ends up in the instance's CachedStorage, retrievable by seq before it
// is finalized and indexed.
func TestCachedStoragePopulatedByWal(t *testing.T) {
	const basePChainHeight = uint64(1)

	// Four equal-weight validators; the node under test is the first.
	numNodes := 4
	validatorSet := make(metadata.NodeBLSMappings, numNodes)
	for i := range numNodes {
		validatorSet[i] = metadata.NodeBLSMapping{NodeID: avalanchego.NodeID{byte(i + 1)}, BLSKey: []byte{byte(i + 1)}, Weight: 1}
	}
	pChain := newTestPlatformChain(basePChainHeight, map[uint64]metadata.NodeBLSMappings{
		basePChainHeight: validatorSet,
	})

	vm := newTestVM()
	vm.pause()
	cops := &testCryptoOps{}
	genesisBlock := &testInnerBlock{Height_: 0, TS: time.Now(), Payload: []byte("genesis")}
	storage := newStorageWithGenesis(t, genesisBlock)
	nodeIDs := validatorSet.Nodes().NodeIDs()
	comm := testutil.NewNoopComm(nodeIDs)
	logger := testutil.MakeLogger(t, 1)
	testWAL := testutil.NewTestWAL(t)

	// The first Simplex block on top of the genesis block.
	genesis := &ParsedBlock{StateMachineBlock: metadata.StateMachineBlock{InnerBlock: genesisBlock}}
	block := newTestParsedBlock(1, "wal block")
	block.Metadata.SimplexProtocolMetadata.Epoch = 1
	block.Metadata.SimplexProtocolMetadata.Prev = genesis.BlockHeader().Digest

	blockRecord, err := common.BlockRecord(block.BlockHeader(), block.Bytes())
	require.NoError(t, err)

	// write block record to wal
	require.NoError(t, testWAL.Append(blockRecord))

	// notarize the block so restoring the WAL keeps it as the round in progress
	quorum := common.Quorum(len(nodeIDs))
	notarizationRecord, err := testutil.NewNotarizationRecord(logger, cops.CreateSignatureAggregator(validatorSet.Nodes()), block, nodeIDs[:quorum])
	require.NoError(t, err)
	require.NoError(t, testWAL.Append(notarizationRecord))

	config := Config{
		Logger:                   logger,
		ID:                       nodeIDs[0],
		VM:                       vm,
		Storage:                  storage,
		Sender:                   comm,
		Broadcaster:              comm,
		PlatformChain:            pChain,
		CryptoOps:                cops,
		LastNonSimplexInnerBlock: genesisBlock,
		WalCreator:               storage.CreateWAL,
		ParameterConfig: ParameterConfig{
			MaxNetworkDelay: 500 * time.Millisecond,
			MaxRoundWindow:  100,
			WALMaxSizeBytes: 1024,
		},
		WALs: []wal.DeletableWAL{testWAL},
	}
	instance := NewInstance(config)
	require.NoError(t, instance.Start(t.Context()))
	t.Cleanup(instance.Stop)

	// The restored block is verified asynchronously and not indexed, so poll until
	// a seq-only lookup serves it from the cache.
	require.Eventually(t, func() bool {
		got, fin, err := instance.cs.Retrieve(1, common.Digest{})
		if err != nil || fin != nil {
			return false
		}
		return got.BlockHeader().Digest == block.BlockHeader().Digest
	}, 20*time.Second, 100*time.Millisecond)
}

// TestCachedStoragePopulatedBySelfBuiltBlock asserts that a block a node builds for its
// own proposal is inserted into the CachedStorage, retrievable by seq and digest before
// it is finalized and indexed.
func TestCachedStoragePopulatedBySelfBuiltBlock(t *testing.T) {
	genesisBlock := &testInnerBlock{Height_: 0, TS: time.Now(), Payload: []byte("genesis")}
	cs := NewCachedStorage(newStorageWithGenesis(t, genesisBlock))

	msm, err := metadata.NewStateMachine(&metadata.Config{
		Logger:                   testutil.MakeLogger(t, 1),
		GetBlock:                 cs.RetrieveBlock,
		LastNonSimplexInnerBlock: genesisBlock,
		GenesisValidatorSet: metadata.NodeBLSMappings{
			{NodeID: avalanchego.NodeID{1}, BLSKey: []byte{1}, Weight: 1},
		},
		AuxiliaryInfoApp: &NoopAuxiliaryInfoApp{},
	})
	require.NoError(t, err)
	cs.msm = msm

	bw := newBlockBuilderWaiter(msm, cs, newTestVM())

	// Build a block on top of genesis
	genesis := &ParsedBlock{StateMachineBlock: metadata.StateMachineBlock{InnerBlock: genesisBlock}}
	md := common.ProtocolMetadata{Seq: 1, Prev: genesis.BlockHeader().Digest}
	vb, built := bw.BuildBlock(t.Context(), md, common.Blacklist{})
	require.True(t, built)
	require.Equal(t, md.Seq, vb.BlockHeader().Seq)

	cached, fin, err := cs.Retrieve(md.Seq, vb.BlockHeader().Digest)
	require.NoError(t, err)
	require.Nil(t, fin)
	require.Same(t, vb, cached)
}

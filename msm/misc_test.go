// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/asn1"
	"fmt"
	"maps"
	"math"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

func TestSafeAdd(t *testing.T) {
	for _, tc := range []struct {
		name string
		a, b uint64
		sum  uint64
		err  string
	}{
		{
			name: "zero plus zero",
			a:    0, b: 0,
			sum: 0,
		},
		{
			name: "normal addition",
			a:    10, b: 20,
			sum: 30,
		},
		{
			name: "max uint64 plus zero",
			a:    math.MaxUint64, b: 0,
			sum: math.MaxUint64,
		},
		{
			name: "zero plus max uint64",
			a:    0, b: math.MaxUint64,
			sum: math.MaxUint64,
		},
		{
			name: "overflow by one",
			a:    math.MaxUint64, b: 1,
			err: "overflow",
		},
		{
			name: "overflow both large",
			a:    math.MaxUint64 - 5, b: 10,
			err: "overflow",
		},
		{
			name: "max uint64 boundary no overflow",
			a:    math.MaxUint64 - 5, b: 5,
			sum: math.MaxUint64,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := safeAdd(tc.a, tc.b)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.sum, result)
			}
		})
	}
}

func TestBitmask(t *testing.T) {
	t.Run("empty bitmask", func(t *testing.T) {
		bm := bitmaskFromBytes(nil)
		require.Equal(t, 0, bm.Len())
		require.False(t, bm.Contains(0))
		require.False(t, bm.Contains(5))
	})

	t.Run("from bytes and Contains", func(t *testing.T) {
		// 0b00000111 = 7 → bits 0, 1, 2 are set
		bm := bitmaskFromBytes([]byte{7})
		require.True(t, bm.Contains(0))
		require.True(t, bm.Contains(1))
		require.True(t, bm.Contains(2))
		require.False(t, bm.Contains(3))
		require.Equal(t, 3, bm.Len())
	})

	t.Run("Add", func(t *testing.T) {
		bm := bitmaskFromBytes([]byte{1}) // bit 0
		require.True(t, bm.Contains(0))
		require.False(t, bm.Contains(3))

		bm.Add(3)
		require.True(t, bm.Contains(3))
		require.Equal(t, 2, bm.Len())
	})

	t.Run("Bytes round-trip", func(t *testing.T) {
		bm := bitmaskFromBytes([]byte{0xAB})
		bm2 := bitmaskFromBytes(bm.Bytes())
		require.Equal(t, bm.Len(), bm2.Len())
		for i := 0; i < 8; i++ {
			require.Equal(t, bm.Contains(i), bm2.Contains(i))
		}
	})

	t.Run("Difference", func(t *testing.T) {
		// bm1 = bits 0,1,2 (0b111 = 7)
		// bm2 = bits 0,1   (0b011 = 3)
		// bm1.Difference(bm2) should leave only bit 2
		bm1 := bitmaskFromBytes([]byte{7})
		bm2 := bitmaskFromBytes([]byte{3})
		bm1.Difference(&bm2)
		require.False(t, bm1.Contains(0))
		require.False(t, bm1.Contains(1))
		require.True(t, bm1.Contains(2))
		require.Equal(t, 1, bm1.Len())
	})

	t.Run("Len with multiple bytes", func(t *testing.T) {
		// 0xFF = 8 bits set, 0x01 = 1 bit set → 9 total
		bm := bitmaskFromBytes([]byte{0x01, 0xFF})
		require.Equal(t, 9, bm.Len())
	})

	t.Run("Clone produces independent copy", func(t *testing.T) {
		bm := bitmaskFromBytes([]byte{7}) // bits 0,1,2
		cloned := bm.Clone()

		// Clone matches original
		require.Equal(t, bm.Len(), cloned.Len())
		for i := 0; i < 3; i++ {
			require.Equal(t, bm.Contains(i), cloned.Contains(i))
		}

		// Mutating clone does not affect original
		cloned.Add(5)
		require.True(t, cloned.Contains(5))
		require.False(t, bm.Contains(5))

		// Mutating original does not affect clone
		bm.Add(7)
		require.True(t, bm.Contains(7))
		require.False(t, cloned.Contains(7))
	})
}

// Test helpers

type InnerBlock struct {
	TS          time.Time
	BlockHeight uint64
	Bytes       []byte
}

func (i *InnerBlock) Digest() [32]byte {
	return sha256.Sum256(i.Bytes)
}

func (i *InnerBlock) Height() uint64 {
	return i.BlockHeight
}

func (i *InnerBlock) Timestamp() time.Time {
	return i.TS
}

func (i *InnerBlock) Verify(_ context.Context) error {
	return nil
}

// fakeVMBlock is a minimal VMBlock implementation for tests.
type fakeVMBlock struct {
	height uint64
}

func (f *fakeVMBlock) Digest() [32]byte               { return [32]byte{} }
func (f *fakeVMBlock) Height() uint64                 { return f.height }
func (f *fakeVMBlock) Timestamp() time.Time           { return time.Time{} }
func (f *fakeVMBlock) Verify(_ context.Context) error { return nil }

type outerBlock struct {
	finalization *simplex.Finalization
	block        StateMachineBlock
}

type blockStore map[uint64]*outerBlock

func (bs blockStore) clone() blockStore {
	newStore := make(blockStore)
	maps.Copy(newStore, bs)
	return newStore
}

func (bs blockStore) getBlock(seq uint64, _ [32]byte) (StateMachineBlock, *simplex.Finalization, error) {
	blk, exits := bs[seq]
	if !exits {
		return StateMachineBlock{}, nil, fmt.Errorf("%w: block %d not found", simplex.ErrBlockNotFound, seq)
	}
	return blk.block, blk.finalization, nil
}

type approvalsRetriever struct {
	result ValidatorSetApprovals
}

func (a approvalsRetriever) Approvals() ValidatorSetApprovals {
	return a.result
}

type signatureVerifier struct {
	err error
}

func (sv *signatureVerifier) VerifySignature(signature []byte, message []byte, publicKey []byte) error {
	return sv.err
}

type signatureAggregator struct {
	weightByNodeID map[string]uint64
	totalWeight    uint64
}

type aggregatrdSignature struct {
	Signatures [][]byte
}

func (sv *signatureAggregator) Aggregate([]simplex.Signature) (simplex.QuorumCertificate, error) {
	panic("unused in tests")
}

func (sv *signatureAggregator) AppendSignatures(existing []byte, sigs ...[]byte) ([]byte, error) {
	all := make([][]byte, 0, len(sigs)+1)
	all = append(all, sigs...)
	if len(existing) > 0 {
		all = append(all, existing)
	}
	return asn1.Marshal(aggregatrdSignature{Signatures: all})
}

func (sv *signatureAggregator) IsQuorum(signers []simplex.NodeID) bool {
	var sum uint64
	for _, signer := range signers {
		sum += sv.weightByNodeID[string(signer)]
	}
	return sum*3 > sv.totalWeight*2
}

func newSignatureAggregatorCreator() simplex.SignatureAggregatorCreator {
	return func(weights []simplex.Node) simplex.SignatureAggregator {
		s := &signatureAggregator{weightByNodeID: make(map[string]uint64, len(weights))}
		for _, nw := range weights {
			s.weightByNodeID[string(nw.Node)] = nw.Weight
			s.totalWeight += nw.Weight
		}
		return s
	}
}

type noOpPChainListener struct{}

func (n *noOpPChainListener) WaitForProgress(ctx context.Context, _ uint64) error {
	<-ctx.Done()
	return ctx.Err()
}

type blockBuilder struct {
	block VMBlock
	err   error
}

func (bb *blockBuilder) WaitForPendingBlock(_ context.Context) {
	// Block is always ready in tests.
}

func (bb *blockBuilder) BuildBlock(_ context.Context, _ uint64) (VMBlock, error) {
	return bb.block, bb.err
}

type validatorSetRetriever struct {
	result    NodeBLSMappings
	resultMap map[uint64]NodeBLSMappings
	err       error
}

func (vsr *validatorSetRetriever) getValidatorSet(height uint64) (NodeBLSMappings, error) {
	if vsr.resultMap != nil {
		if result, ok := vsr.resultMap[height]; ok {
			return result, vsr.err
		}
	}
	return vsr.result, vsr.err
}

type keyAggregator struct{}

func (ka *keyAggregator) AggregateKeys(keys ...[]byte) ([]byte, error) {
	aggregated := make([]byte, 0)
	for _, key := range keys {
		aggregated = append(aggregated, key...)
	}
	return aggregated, nil
}

var (
	genesisBlock = StateMachineBlock{
		// Genesis block metadata has all zero values
		InnerBlock: &InnerBlock{
			TS:    time.Now(),
			Bytes: []byte{1, 2, 3},
		},
	}
)

type dynamicApprovalsRetriever struct {
	approvals *ValidatorSetApprovals
}

func (d *dynamicApprovalsRetriever) Approvals() ValidatorSetApprovals {
	return *d.approvals
}

func makeChain(t *testing.T, simplexStartHeight uint64, endHeight uint64) []StateMachineBlock {
	startTime := time.Now().Add(-time.Duration(endHeight+2) * time.Second)
	blocks := make([]StateMachineBlock, 0, endHeight+1)
	var round, seq uint64
	for h := uint64(0); h <= endHeight; h++ {
		index := len(blocks)

		if h == 0 {
			blocks = append(blocks, genesisBlock)
			continue
		}

		if h < simplexStartHeight {
			blocks = append(blocks, makeNonSimplexBlock(t, simplexStartHeight, startTime, h))
			continue
		}

		seq = uint64(index)

		blocks = append(blocks, makeNormalSimplexBlock(t, index, blocks, startTime, h, round, seq))
		round++
	}
	return blocks
}

func makeNormalSimplexBlock(t *testing.T, index int, blocks []StateMachineBlock, start time.Time, h uint64, round uint64, seq uint64) StateMachineBlock {
	content := make([]byte, 10)
	_, err := rand.Read(content)
	require.NoError(t, err)

	prev := genesisBlock.Digest()
	if index > 0 {
		prev = blocks[index-1].Digest()
	}

	return StateMachineBlock{
		InnerBlock: &InnerBlock{
			TS:          start.Add(time.Duration(h) * time.Second),
			BlockHeight: h,
			Bytes:       []byte{1, 2, 3},
		},
		Metadata: StateMachineMetadata{
			PChainHeight: 100,
			SimplexProtocolMetadata: (&simplex.ProtocolMetadata{
				Round: round,
				Seq:   seq,
				Epoch: 1,
				Prev:  prev,
			}).Bytes(),
			SimplexEpochInfo: SimplexEpochInfo{
				PrevSealingBlockHash:  [32]byte{},
				PChainReferenceHeight: 100,
				EpochNumber:           1,
				PrevVMBlockSeq:        uint64(index),
			},
		},
	}
}

func makeNonSimplexBlock(t *testing.T, startHeight uint64, start time.Time, h uint64) StateMachineBlock {
	content := make([]byte, 10)
	_, err := rand.Read(content)
	require.NoError(t, err)

	return StateMachineBlock{
		InnerBlock: &InnerBlock{
			TS:          start.Add(time.Duration(h-startHeight) * time.Second),
			BlockHeight: h,
			Bytes:       []byte{1, 2, 3},
		},
	}
}

type testConfig struct {
	blockStore            blockStore
	approvalsRetriever    approvalsRetriever
	signatureVerifier     signatureVerifier
	signatureAggregator   signatureAggregator
	blockBuilder          blockBuilder
	keyAggregator         keyAggregator
	validatorSetRetriever validatorSetRetriever
}

func newStateMachine(t *testing.T) (*StateMachine, *testConfig) {
	bs := make(blockStore)
	bs[0] = &outerBlock{block: genesisBlock}

	var testConfig testConfig
	testConfig.blockStore = bs
	testConfig.validatorSetRetriever.result = NodeBLSMappings{
		{BLSKey: []byte{1}, Weight: 1}, {BLSKey: []byte{2}, Weight: 1},
	}

	smConfig := Config{
		GenesisValidatorSet:             NodeBLSMappings{{BLSKey: []byte{1}, Weight: 1}, {BLSKey: []byte{2}, Weight: 1}},
		LastNonSimplexBlockPChainHeight: 100,
		FirstEverSimplexBlock: func() *StateMachineBlock {
			var res *StateMachineBlock
			min := uint64(math.MaxUint64)
			for seq, block := range testConfig.blockStore {
				if block.block.Metadata.SimplexEpochInfo.EpochNumber == 0 {
					continue
				}
				if seq < min {
					min = seq
					res = &block.block
				}
			}
			return res
		},
		GetTime:                    time.Now,
		TimeSkewLimit:              time.Second * 5,
		Logger:                     testutil.MakeLogger(t),
		GetBlock:                   testConfig.blockStore.getBlock,
		MaxBlockBuildingWaitTime:   time.Second,
		ApprovalsRetriever:         &testConfig.approvalsRetriever,
		SignatureVerifier:          &testConfig.signatureVerifier,
		SignatureAggregatorCreator: newSignatureAggregatorCreator(),
		BlockBuilder:               &testConfig.blockBuilder,
		KeyAggregator:              &testConfig.keyAggregator,
		GetPChainHeight: func() uint64 {
			return 100
		},
		GetUpgrades: func() any {
			return nil
		},
		GetValidatorSet:          testConfig.validatorSetRetriever.getValidatorSet,
		PChainProgressListener:   &noOpPChainListener{},
		LastNonSimplexInnerBlock: genesisBlock.InnerBlock,
	}

	sm, err := NewStateMachine(&smConfig)
	require.NoError(t, err)

	return sm, &testConfig
}

// concatAggregator concatenates signatures for easy verification in tests.
type concatAggregator struct{}

func (concatAggregator) Aggregate([]simplex.Signature) (simplex.QuorumCertificate, error) {
	panic("unused in tests")
}

func (concatAggregator) AppendSignatures(existing []byte, sigs ...[]byte) ([]byte, error) {
	result := bytes.Join(sigs, nil)
	return append(result, existing...), nil
}

func (concatAggregator) IsQuorum([]simplex.NodeID) bool {
	return false
}

type failingAggregator struct{}

func (failingAggregator) Aggregate([]simplex.Signature) (simplex.QuorumCertificate, error) {
	panic("unused in tests")
}

func (failingAggregator) AppendSignatures([]byte, ...[]byte) ([]byte, error) {
	return nil, fmt.Errorf("aggregation failed")
}

func (failingAggregator) IsQuorum([]simplex.NodeID) bool {
	return false
}

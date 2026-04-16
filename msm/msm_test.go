// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/stretchr/testify/require"
)

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
	for k, v := range bs {
		newStore[k] = v
	}
	return newStore
}

func (bs blockStore) getBlock(opts RetrievingOpts) (StateMachineBlock, *simplex.Finalization, error) {
	blk, exits := bs[opts.Height]
	if !exits {
		return StateMachineBlock{}, nil, fmt.Errorf("%w: block %d not found", simplex.ErrBlockNotFound, opts.Height)
	}
	return blk.block, blk.finalization, nil
}

func TestIdentifyCurrentState(t *testing.T) {
	bvd := &BlockValidationDescriptor{}
	for _, tc := range []struct {
		name     string
		input    SimplexEpochInfo
		expected state
	}{
		{
			name:     "epoch 0 is first simplex block",
			input:    SimplexEpochInfo{EpochNumber: 0},
			expected: stateFirstSimplexBlock,
		},
		{
			name:     "no next p-chain ref height means normal op",
			input:    SimplexEpochInfo{EpochNumber: 1, NextPChainReferenceHeight: 0},
			expected: stateBuildBlockNormalOp,
		},
		{
			name:     "has sealing block seq means epoch sealed",
			input:    SimplexEpochInfo{EpochNumber: 1, NextPChainReferenceHeight: 100, SealingBlockSeq: 5},
			expected: stateBuildBlockEpochSealed,
		},
		{
			name:     "has block validation descriptor means epoch sealed",
			input:    SimplexEpochInfo{EpochNumber: 1, NextPChainReferenceHeight: 100, BlockValidationDescriptor: bvd},
			expected: stateBuildBlockEpochSealed,
		},
		{
			name:     "next p-chain ref height > 0 without sealing means collecting approvals",
			input:    SimplexEpochInfo{EpochNumber: 1, NextPChainReferenceHeight: 100},
			expected: stateBuildCollectingApprovals,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := identifyCurrentState(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestAreNextEpochApprovalsSignersSupersetOfApprovalsOfPrevBlock(t *testing.T) {
	for _, tc := range []struct {
		name string
		prev SimplexEpochInfo
		next SimplexEpochInfo
		err  string
	}{
		{
			name: "prev has nil approvals",
			prev: SimplexEpochInfo{},
			next: SimplexEpochInfo{NextEpochApprovals: &NextEpochApprovals{NodeIDs: []byte{3}}},
		},
		{
			name: "next is superset of prev",
			prev: SimplexEpochInfo{NextEpochApprovals: &NextEpochApprovals{NodeIDs: []byte{1}}},
			next: SimplexEpochInfo{NextEpochApprovals: &NextEpochApprovals{NodeIDs: []byte{3}}},
		},
		{
			name: "next equals prev",
			prev: SimplexEpochInfo{NextEpochApprovals: &NextEpochApprovals{NodeIDs: []byte{3}}},
			next: SimplexEpochInfo{NextEpochApprovals: &NextEpochApprovals{NodeIDs: []byte{3}}},
		},
		{
			name: "next is missing a signer from prev",
			prev: SimplexEpochInfo{NextEpochApprovals: &NextEpochApprovals{NodeIDs: []byte{3}}},
			next: SimplexEpochInfo{NextEpochApprovals: &NextEpochApprovals{NodeIDs: []byte{1}}},
			err:  "some signers from parent block are missing",
		},
		{
			name: "prev has approvals but next has nil approvals",
			prev: SimplexEpochInfo{NextEpochApprovals: &NextEpochApprovals{NodeIDs: []byte{1}}},
			next: SimplexEpochInfo{},
			err:  "previous block has next epoch approvals but proposed block doesn't have next epoch approvals",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ensureNextEpochApprovalsSignersSupersetOfApprovalsOfPrevBlock(tc.prev, tc.next)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestComputePrevVMBlockSeq(t *testing.T) {
	t.Run("parent has no inner block", func(t *testing.T) {
		parent := StateMachineBlock{
			InnerBlock: nil,
			Metadata:   StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{PrevVMBlockSeq: 42}},
		}
		require.Equal(t, uint64(42), computePrevVMBlockSeq(parent, 100))
	})

	t.Run("parent has inner block", func(t *testing.T) {
		parent := StateMachineBlock{
			InnerBlock: &fakeVMBlock{height: 10},
			Metadata:   StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{PrevVMBlockSeq: 42}},
		}
		require.Equal(t, uint64(100), computePrevVMBlockSeq(parent, 100))
	})
}

func TestFindFirstSimplexBlock(t *testing.T) {
	t.Run("found at height 3", func(t *testing.T) {
		getBlock := func(opts RetrievingOpts) (StateMachineBlock, *simplex.Finalization, error) {
			if opts.Height < 3 {
				return StateMachineBlock{}, nil, nil
			}
			return StateMachineBlock{
				Metadata: StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{EpochNumber: 1}},
			}, nil, nil
		}
		result, err := findFirstSimplexBlock(getBlock, 5)
		require.NoError(t, err)
		require.Equal(t, uint64(3), result)
	})

	t.Run("no simplex blocks found", func(t *testing.T) {
		getBlock := func(opts RetrievingOpts) (StateMachineBlock, *simplex.Finalization, error) {
			return StateMachineBlock{}, nil, nil
		}
		_, err := findFirstSimplexBlock(getBlock, 5)
		require.ErrorContains(t, err, "no simplex blocks found")
	})

	t.Run("block not found errors are skipped", func(t *testing.T) {
		getBlock := func(opts RetrievingOpts) (StateMachineBlock, *simplex.Finalization, error) {
			if opts.Height < 2 {
				return StateMachineBlock{}, nil, simplex.ErrBlockNotFound
			}
			return StateMachineBlock{
				Metadata: StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{EpochNumber: 1}},
			}, nil, nil
		}
		result, err := findFirstSimplexBlock(getBlock, 5)
		require.NoError(t, err)
		require.Equal(t, uint64(2), result)
	})

	t.Run("retrieval error propagated", func(t *testing.T) {
		getBlock := func(opts RetrievingOpts) (StateMachineBlock, *simplex.Finalization, error) {
			return StateMachineBlock{}, nil, fmt.Errorf("disk error")
		}
		_, err := findFirstSimplexBlock(getBlock, 5)
		require.ErrorContains(t, err, "disk error")
	})
}

func TestComputeTotalWeight(t *testing.T) {
	t.Run("valid weights", func(t *testing.T) {
		validators := NodeBLSMappings{
			{Weight: 100},
			{Weight: 200},
			{Weight: 300},
		}
		total, err := computeTotalWeight(validators)
		require.NoError(t, err)
		require.Equal(t, int64(600), total)
	})

	t.Run("zero total weight", func(t *testing.T) {
		validators := NodeBLSMappings{{Weight: 0}}
		_, err := computeTotalWeight(validators)
		require.ErrorContains(t, err, "total weight of validators is 0")
	})

	t.Run("empty validators", func(t *testing.T) {
		_, err := computeTotalWeight(NodeBLSMappings{})
		require.ErrorContains(t, err, "total weight of validators is 0")
	})
}

func TestComputeApprovingWeight(t *testing.T) {
	validators := NodeBLSMappings{
		{Weight: 100},
		{Weight: 200},
		{Weight: 300},
	}

	t.Run("all approving", func(t *testing.T) {
		bm := bitmaskFromBytes([]byte{7})
		weight, err := computeApprovingWeight(validators, &bm)
		require.NoError(t, err)
		require.Equal(t, int64(600), weight)
	})

	t.Run("partial approving", func(t *testing.T) {
		bm := bitmaskFromBytes([]byte{5})
		weight, err := computeApprovingWeight(validators, &bm)
		require.NoError(t, err)
		require.Equal(t, int64(400), weight)
	})

	t.Run("none approving", func(t *testing.T) {
		bm := bitmaskFromBytes(nil)
		weight, err := computeApprovingWeight(validators, &bm)
		require.NoError(t, err)
		require.Equal(t, int64(0), weight)
	})

	t.Run("single validator approving", func(t *testing.T) {
		bm := bitmaskFromBytes([]byte{2})
		weight, err := computeApprovingWeight(validators, &bm)
		require.NoError(t, err)
		require.Equal(t, int64(200), weight)
	})
}

func TestSanitizeApprovals(t *testing.T) {
	node0 := nodeID{0}
	node1 := nodeID{1}
	node2 := nodeID{2}
	node3 := nodeID{3}

	nodeID2Index := map[nodeID]int{
		node0: 0,
		node1: 1,
		node2: 2,
	}

	t.Run("filters by p-chain height", func(t *testing.T) {
		approvals := ValidatorSetApprovals{
			{NodeID: node0, PChainHeight: 100},
			{NodeID: node1, PChainHeight: 200},
		}
		oldApproving := bitmaskFromBytes(nil)
		result := sanitizeApprovals(approvals, 100, nodeID2Index, oldApproving)
		require.Len(t, result, 1)
		require.Equal(t, node0, result[0].NodeID)
	})

	t.Run("filters out already approved", func(t *testing.T) {
		approvals := ValidatorSetApprovals{
			{NodeID: node0, PChainHeight: 100},
			{NodeID: node1, PChainHeight: 100},
		}
		oldApproving := bitmaskFromBytes([]byte{1})
		result := sanitizeApprovals(approvals, 100, nodeID2Index, oldApproving)
		require.Len(t, result, 1)
		require.Equal(t, node1, result[0].NodeID)
	})

	t.Run("filters out nodes not in validator set", func(t *testing.T) {
		approvals := ValidatorSetApprovals{
			{NodeID: node3, PChainHeight: 100},
			{NodeID: node2, PChainHeight: 100},
		}
		oldApproving := bitmaskFromBytes(nil)
		result := sanitizeApprovals(approvals, 100, nodeID2Index, oldApproving)
		require.Len(t, result, 1)
		require.Equal(t, node2, result[0].NodeID)
	})

	t.Run("deduplicates by node ID", func(t *testing.T) {
		approvals := ValidatorSetApprovals{
			{NodeID: node0, PChainHeight: 100},
			{NodeID: node0, PChainHeight: 100},
		}
		oldApproving := bitmaskFromBytes(nil)
		result := sanitizeApprovals(approvals, 100, nodeID2Index, oldApproving)
		require.Len(t, result, 1)
	})
}

func TestIdentifyBlockType(t *testing.T) {
	bvd := &BlockValidationDescriptor{}

	for _, tc := range []struct {
		name     string
		nextMD   StateMachineMetadata
		prevMD   StateMachineMetadata
		prevSeq  uint64
		expected BlockType
	}{
		{
			name:     "next block has BlockValidationDescriptor",
			nextMD:   StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{BlockValidationDescriptor: bvd}},
			prevMD:   StateMachineMetadata{},
			expected: BlockTypeSealing,
		},
		{
			name:   "prev is zero-epoch block (epoch 1, NextPChainReferenceHeight 0)",
			nextMD: StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{EpochNumber: 1}},
			prevMD: StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{
				EpochNumber:               1,
				NextPChainReferenceHeight: 0,
			}},
			expected: BlockTypeNormal,
		},
		{
			name:   "prev is sealing block and next epoch matches prevSeq",
			nextMD: StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{EpochNumber: 10}},
			prevMD: StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{
				BlockValidationDescriptor: bvd,
				EpochNumber:               1,
				NextPChainReferenceHeight: 200,
			}},
			prevSeq:  10,
			expected: BlockTypeNewEpoch,
		},
		{
			name:   "prev is sealing block and next epoch does not match prevSeq (Telock)",
			nextMD: StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{EpochNumber: 1}},
			prevMD: StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{
				BlockValidationDescriptor: bvd,
				EpochNumber:               1,
				NextPChainReferenceHeight: 200,
			}},
			prevSeq:  10,
			expected: BlockTypeTelock,
		},
		{
			name:   "same epoch with non-zero SealingBlockSeq (Telock)",
			nextMD: StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{EpochNumber: 5}},
			prevMD: StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{
				EpochNumber:     5,
				SealingBlockSeq: 8,
			}},
			expected: BlockTypeTelock,
		},
		{
			name:   "epoch number matches prev SealingBlockSeq (NewEpoch)",
			nextMD: StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{EpochNumber: 8}},
			prevMD: StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{
				EpochNumber:     5,
				SealingBlockSeq: 8,
			}},
			expected: BlockTypeNewEpoch,
		},
		{
			name:   "normal block in the middle of an epoch",
			nextMD: StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{EpochNumber: 5}},
			prevMD: StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{
				EpochNumber: 5,
			}},
			expected: BlockTypeNormal,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := IdentifyBlockType(tc.nextMD, tc.prevMD, tc.prevSeq)
			require.Equal(t, tc.expected, result)
		})
	}
}

// concatAggregator concatenates signatures for easy verification in tests.
type concatAggregator struct{}

func (concatAggregator) AggregateSignatures(sigs ...[]byte) ([]byte, error) {
	return bytes.Join(sigs, nil), nil
}

type failingAggregator struct{}

func (failingAggregator) AggregateSignatures(sigs ...[]byte) ([]byte, error) {
	return nil, fmt.Errorf("aggregation failed")
}

func TestComputeNewApproverSignaturesAndSigners(t *testing.T) {
	node0 := nodeID{0}
	node1 := nodeID{1}
	node2 := nodeID{2}

	nodeID2Index := map[nodeID]int{
		node0: 0,
		node1: 1,
		node2: 2,
	}

	t.Run("new approvals with no previous", func(t *testing.T) {
		prevApprovals := &NextEpochApprovals{}
		oldApproving := bitmaskFromBytes(nil)

		peers := ValidatorSetApprovals{
			{NodeID: node0, Signature: []byte("sig0")},
			{NodeID: node1, Signature: []byte("sig1")},
		}

		aggSig, newApproving, err := computeNewApproverSignaturesAndSigners(prevApprovals, peers, oldApproving, nodeID2Index, concatAggregator{})
		require.NoError(t, err)
		require.True(t, newApproving.Contains(0))
		require.True(t, newApproving.Contains(1))
		require.False(t, newApproving.Contains(2))
		require.Equal(t, []byte("sig0sig1"), aggSig)
	})

	t.Run("new approvals added to existing", func(t *testing.T) {
		prevApprovals := &NextEpochApprovals{
			NodeIDs:   []byte{1}, // bit 0
			Signature: []byte("existing"),
		}
		oldApproving := bitmaskFromBytes([]byte{1}) // node0 already approved

		peers := ValidatorSetApprovals{
			{NodeID: node2, Signature: []byte("sig2")},
		}

		aggSig, newApproving, err := computeNewApproverSignaturesAndSigners(prevApprovals, peers, oldApproving, nodeID2Index, concatAggregator{})
		require.NoError(t, err)
		require.True(t, newApproving.Contains(0))  // preserved from old
		require.True(t, newApproving.Contains(2))  // newly added
		require.False(t, newApproving.Contains(1)) // not approved
		require.Equal(t, []byte("sig2existing"), aggSig)
	})

	t.Run("no new approvals with existing signature", func(t *testing.T) {
		prevApprovals := &NextEpochApprovals{
			NodeIDs:   []byte{1},
			Signature: []byte("existing"),
		}
		oldApproving := bitmaskFromBytes([]byte{1})

		aggSig, newApproving, err := computeNewApproverSignaturesAndSigners(prevApprovals, nil, oldApproving, nodeID2Index, concatAggregator{})
		require.NoError(t, err)
		require.True(t, newApproving.Contains(0))
		require.Equal(t, []byte("existing"), aggSig)
	})

	t.Run("peer not in validator set is skipped", func(t *testing.T) {
		prevApprovals := &NextEpochApprovals{}
		oldApproving := bitmaskFromBytes(nil)
		unknownNode := nodeID{99}

		peers := ValidatorSetApprovals{
			{NodeID: unknownNode, Signature: []byte("unknown")},
			{NodeID: node0, Signature: []byte("sig0")},
		}

		aggSig, newApproving, err := computeNewApproverSignaturesAndSigners(prevApprovals, peers, oldApproving, nodeID2Index, concatAggregator{})
		require.NoError(t, err)
		require.True(t, newApproving.Contains(0))
		require.Equal(t, 1, newApproving.Len())
		require.Equal(t, []byte("sig0"), aggSig)
	})

	t.Run("aggregation error propagated", func(t *testing.T) {
		prevApprovals := &NextEpochApprovals{}
		oldApproving := bitmaskFromBytes(nil)
		peers := ValidatorSetApprovals{
			{NodeID: node0, Signature: []byte("sig0")},
		}

		_, _, err := computeNewApproverSignaturesAndSigners(prevApprovals, peers, oldApproving, nodeID2Index, failingAggregator{})
		require.ErrorContains(t, err, "aggregation failed")
	})
}

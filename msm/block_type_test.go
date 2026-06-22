// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"testing"

	"github.com/ava-labs/simplex/common"
	"github.com/stretchr/testify/require"
)

func TestIdentifyBlockType(t *testing.T) {
	bvd := &common.BlockValidationDescriptor{}

	for _, tc := range []struct {
		name    string
		nextMD  common.StateMachineMetadata
		prevMD  common.StateMachineMetadata
		prevSeq uint64
		expected BlockType
	}{
		{
			name:     "next block has BlockValidationDescriptor",
			nextMD:   common.StateMachineMetadata{SimplexEpochInfo: common.SimplexEpochInfo{BlockValidationDescriptor: bvd}},
			prevMD:   common.StateMachineMetadata{},
			expected: BlockTypeSealing,
		},
		{
			name:   "prev is zero-epoch block (epoch 1, NextPChainReferenceHeight 0)",
			nextMD: common.StateMachineMetadata{SimplexEpochInfo: common.SimplexEpochInfo{EpochNumber: 1}},
			prevMD: common.StateMachineMetadata{SimplexEpochInfo: common.SimplexEpochInfo{
				EpochNumber:               1,
				NextPChainReferenceHeight: 0,
			}},
			expected: BlockTypeNormal,
		},
		{
			name:   "prev is sealing block and next epoch matches prevSeq",
			nextMD: common.StateMachineMetadata{SimplexEpochInfo: common.SimplexEpochInfo{EpochNumber: 10}},
			prevMD: common.StateMachineMetadata{SimplexEpochInfo: common.SimplexEpochInfo{
				BlockValidationDescriptor: bvd,
				EpochNumber:               1,
				NextPChainReferenceHeight: 200,
			}},
			prevSeq:  10,
			expected: BlockTypeNewEpoch,
		},
		{
			name:   "prev is sealing block and next epoch does not match prevSeq (Telock)",
			nextMD: common.StateMachineMetadata{SimplexEpochInfo: common.SimplexEpochInfo{EpochNumber: 1}},
			prevMD: common.StateMachineMetadata{SimplexEpochInfo: common.SimplexEpochInfo{
				BlockValidationDescriptor: bvd,
				EpochNumber:               1,
				NextPChainReferenceHeight: 200,
			}},
			prevSeq:  10,
			expected: BlockTypeTelock,
		},
		{
			name:   "same epoch with non-zero SealingBlockSeq (Telock)",
			nextMD: common.StateMachineMetadata{SimplexEpochInfo: common.SimplexEpochInfo{EpochNumber: 5}},
			prevMD: common.StateMachineMetadata{SimplexEpochInfo: common.SimplexEpochInfo{
				EpochNumber:     5,
				SealingBlockSeq: 8,
			}},
			expected: BlockTypeTelock,
		},
		{
			name:   "epoch number matches prev SealingBlockSeq (NewEpoch)",
			nextMD: common.StateMachineMetadata{SimplexEpochInfo: common.SimplexEpochInfo{EpochNumber: 8}},
			prevMD: common.StateMachineMetadata{SimplexEpochInfo: common.SimplexEpochInfo{
				EpochNumber:     5,
				SealingBlockSeq: 8,
			}},
			expected: BlockTypeNewEpoch,
		},
		{
			name:   "normal block in the middle of an epoch",
			nextMD: common.StateMachineMetadata{SimplexEpochInfo: common.SimplexEpochInfo{EpochNumber: 5}},
			prevMD: common.StateMachineMetadata{SimplexEpochInfo: common.SimplexEpochInfo{
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

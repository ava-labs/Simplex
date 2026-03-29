// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/stretchr/testify/require"
)

func TestICMEpochInfoVerifier(t *testing.T) {
	now := time.Now()

	for _, tc := range []struct {
		name      string
		prevMD    StateMachineMetadata
		nextMD    StateMachineMetadata
		blockTime time.Time
		err       string
	}{
		{
			name:   "matching ICM epoch info without inner block",
			prevMD: StateMachineMetadata{},
			nextMD: StateMachineMetadata{},
		},
		{
			name:      "matching ICM epoch info with inner block",
			prevMD:    StateMachineMetadata{},
			nextMD:    StateMachineMetadata{},
			blockTime: now,
		},
		{
			name:   "mismatching ICM epoch info",
			prevMD: StateMachineMetadata{},
			nextMD: StateMachineMetadata{
				ICMEpochInfo: ICMEpochInfo{EpochNumber: 99},
			},
			err: "expected ICM epoch info to be {0 0 0 {0}} but got {0 99 0 {0}}",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &icmEpochInfoVerifier{
				getUpdates: func() any { return nil },
				computeICMEpoch: func(_ any, input ICMEpochInput) ICMEpoch {
					return input.ParentEpoch
				},
			}
			err := v.Verify(verificationInput{
				prevMD:                 tc.prevMD,
				proposedBlockMD:        tc.nextMD,
				proposedBlockTimestamp: tc.blockTime,
			})
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPChainHeightVerifier(t *testing.T) {
	for _, tc := range []struct {
		name         string
		pChainHeight uint64
		prevHeight   uint64
		nextHeight   uint64
		err          string
	}{
		{
			name:         "valid height",
			pChainHeight: 200,
			prevHeight:   100,
			nextHeight:   150,
		},
		{
			name:         "height equal to current",
			pChainHeight: 200,
			prevHeight:   100,
			nextHeight:   200,
		},
		{
			name:         "height too big",
			pChainHeight: 100,
			prevHeight:   50,
			nextHeight:   150,
			err:          "invalid P-chain reference height (150) is too big, expected to be ≤ 100",
		},
		{
			name:         "height smaller than parent",
			pChainHeight: 200,
			prevHeight:   150,
			nextHeight:   100,
			err:          "invalid P-chain height (100) is smaller than parent inner block's P-chain height (150)",
		},
		{
			name:         "height equal to parent",
			pChainHeight: 200,
			prevHeight:   100,
			nextHeight:   100,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &pChainHeightVerifier{
				getPChainHeight: func() uint64 { return tc.pChainHeight },
			}
			err := v.Verify(verificationInput{
				prevMD:          StateMachineMetadata{PChainHeight: tc.prevHeight},
				proposedBlockMD: StateMachineMetadata{PChainHeight: tc.nextHeight},
			})
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestTimestampVerifier(t *testing.T) {
	now := time.Now()

	timeSkewLimit := 5 * time.Second

	for _, tc := range []struct {
		name            string
		blockTime       time.Time
		timestamp       uint64
		parentTimestamp uint64
		err             string
	}{
		{
			name:      "matching timestamp",
			blockTime: now,
			timestamp: uint64(now.Unix()),
		},
		{
			name:      "mismatching timestamp",
			blockTime: now,
			timestamp: uint64(now.Unix()) + 100,
			err:       fmt.Sprintf("expected timestamp to be %d but got %d", now.Unix(), int64(uint64(now.Unix())+100)),
		},
		{
			name:      "timestamp too far in the future",
			blockTime: now.Add(10 * time.Second),
			timestamp: uint64(now.Add(10 * time.Second).Unix()),
			err:       fmt.Sprintf("proposed block timestamp is too far in the future, current time is %s but got %s", now.String(), now.Add(10*time.Second).String()),
		},
		{
			name:            "timestamp older than parent",
			blockTime:       now,
			timestamp:       uint64(now.Unix()),
			parentTimestamp: uint64(now.Unix()) + 10,
			err:             fmt.Sprintf("proposed block timestamp is older than parent block's timestamp, parent timestamp is %d but got %d", uint64(now.Unix())+10, uint64(now.Unix())),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &timestampVerifier{
				getTime:       func() time.Time { return now },
				timeSkewLimit: timeSkewLimit,
			}
			err := v.Verify(verificationInput{
				proposedBlockTimestamp: tc.blockTime,
				proposedBlockMD:        StateMachineMetadata{Timestamp: tc.timestamp},
				prevMD:                 StateMachineMetadata{Timestamp: tc.parentTimestamp},
			})
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPChainReferenceHeightVerifier(t *testing.T) {
	for _, tc := range []struct {
		name          string
		nextBlockType BlockType
		prev          SimplexEpochInfo
		next          SimplexEpochInfo
		err           string
	}{
		{
			name:          "new epoch block matching prev NextPChainReferenceHeight",
			nextBlockType: BlockTypeNewEpoch,
			prev:          SimplexEpochInfo{NextPChainReferenceHeight: 200, SealingBlockSeq: 5},
			next:          SimplexEpochInfo{PChainReferenceHeight: 200},
		},
		{
			name:          "new epoch block not matching prev NextPChainReferenceHeight",
			nextBlockType: BlockTypeNewEpoch,
			prev:          SimplexEpochInfo{NextPChainReferenceHeight: 200, SealingBlockSeq: 5},
			next:          SimplexEpochInfo{PChainReferenceHeight: 100},
			err:           "expected P-chain reference height of the first inner block of epoch 5 to be 200 but got 100",
		},
		{
			name:          "normal block matching prev PChainReferenceHeight",
			nextBlockType: BlockTypeNormal,
			prev:          SimplexEpochInfo{PChainReferenceHeight: 100},
			next:          SimplexEpochInfo{PChainReferenceHeight: 100},
		},
		{
			name:          "normal block not matching prev PChainReferenceHeight",
			nextBlockType: BlockTypeNormal,
			prev:          SimplexEpochInfo{PChainReferenceHeight: 100},
			next:          SimplexEpochInfo{PChainReferenceHeight: 200},
			err:           "expected P-chain reference height to be 100 but got 200",
		},
		{
			name:          "sealing block matching prev PChainReferenceHeight",
			nextBlockType: BlockTypeSealing,
			prev:          SimplexEpochInfo{PChainReferenceHeight: 100},
			next:          SimplexEpochInfo{PChainReferenceHeight: 100},
		},
		{
			name:          "telock block matching prev PChainReferenceHeight",
			nextBlockType: BlockTypeTelock,
			prev:          SimplexEpochInfo{PChainReferenceHeight: 100},
			next:          SimplexEpochInfo{PChainReferenceHeight: 100},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &pChainReferenceHeightVerifier{}
			err := v.Verify(verificationInput{
				nextBlockType:   tc.nextBlockType,
				prevMD:          StateMachineMetadata{SimplexEpochInfo: tc.prev},
				proposedBlockMD: StateMachineMetadata{SimplexEpochInfo: tc.next},
			})
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestEpochNumberVerifier(t *testing.T) {
	for _, tc := range []struct {
		name          string
		nextBlockType BlockType
		prevBlockSeq  uint64
		prev          SimplexEpochInfo
		next          SimplexEpochInfo
		err           string
	}{
		{
			name:          "prev epoch 0 with wrong next epoch",
			nextBlockType: BlockTypeNormal,
			prev:          SimplexEpochInfo{EpochNumber: 0},
			next:          SimplexEpochInfo{EpochNumber: 5},
			err:           "expected epoch number of the first inner block created to be 1 but got 5",
		},
		{
			name:          "new epoch block matching sealing seq",
			nextBlockType: BlockTypeNewEpoch,
			prevBlockSeq:  10,
			prev:          SimplexEpochInfo{EpochNumber: 1},
			next:          SimplexEpochInfo{EpochNumber: 10},
		},
		{
			name:          "new epoch block not matching sealing seq",
			nextBlockType: BlockTypeNewEpoch,
			prevBlockSeq:  10,
			prev:          SimplexEpochInfo{EpochNumber: 1},
			next:          SimplexEpochInfo{EpochNumber: 5},
			err:           "expected epoch number to be 10 but got 5",
		},
		{
			name:          "normal block same epoch",
			nextBlockType: BlockTypeNormal,
			prev:          SimplexEpochInfo{EpochNumber: 3},
			next:          SimplexEpochInfo{EpochNumber: 3},
		},
		{
			name:          "normal block different epoch",
			nextBlockType: BlockTypeNormal,
			prev:          SimplexEpochInfo{EpochNumber: 3},
			next:          SimplexEpochInfo{EpochNumber: 4},
			err:           "expected epoch number to be 3 but got 4",
		},
		{
			name:          "sealing block same epoch",
			nextBlockType: BlockTypeSealing,
			prev:          SimplexEpochInfo{EpochNumber: 2},
			next:          SimplexEpochInfo{EpochNumber: 2},
		},
		{
			name:          "telock block same epoch",
			nextBlockType: BlockTypeTelock,
			prev:          SimplexEpochInfo{EpochNumber: 2},
			next:          SimplexEpochInfo{EpochNumber: 2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &epochNumberVerifier{}
			err := v.Verify(verificationInput{
				nextBlockType:   tc.nextBlockType,
				prevBlockSeq:    tc.prevBlockSeq,
				prevMD:          StateMachineMetadata{SimplexEpochInfo: tc.prev},
				proposedBlockMD: StateMachineMetadata{SimplexEpochInfo: tc.next},
			})
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPrevSealingBlockHashVerifier(t *testing.T) {
	// A simplex block (EpochNumber > 0) so findFirstSimplexBlock can locate it.
	firstSimplexBlock := StateMachineBlock{
		InnerBlock: &testVMBlock{bytes: []byte{1, 2, 3}},
		Metadata:   StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{EpochNumber: 1}},
	}
	firstSimplexBlockHash := firstSimplexBlock.Digest()

	// A block used for epoch >1 sealing lookups.
	prevSealingBlock := StateMachineBlock{
		InnerBlock: &testVMBlock{bytes: []byte{4, 5, 6}},
		Metadata:   StateMachineMetadata{SimplexEpochInfo: SimplexEpochInfo{EpochNumber: 5}},
	}
	prevSealingBlockHash := prevSealingBlock.Digest()

	bs := make(testBlockStore)
	bs[1] = firstSimplexBlock
	bs[5] = prevSealingBlock
	latestPersisted := uint64(1)

	for _, tc := range []struct {
		name          string
		nextBlockType BlockType
		prev          SimplexEpochInfo
		next          SimplexEpochInfo
		err           string
	}{
		{
			name:          "epoch 1 sealing block with correct hash",
			nextBlockType: BlockTypeSealing,
			prev:          SimplexEpochInfo{EpochNumber: 1},
			next: SimplexEpochInfo{
				PrevSealingBlockHash: firstSimplexBlockHash,
			},
		},
		{
			name:          "epoch 1 sealing block with wrong hash",
			nextBlockType: BlockTypeSealing,
			prev:          SimplexEpochInfo{EpochNumber: 1},
			next: SimplexEpochInfo{
				PrevSealingBlockHash: [32]byte{9, 9, 9},
			},
			err: fmt.Sprintf("expected prev sealing inner block hash of the first ever simplex inner block to be %x but got %x", firstSimplexBlockHash, [32]byte{9, 9, 9}),
		},
		{
			name:          "epoch >1 sealing block with correct hash",
			nextBlockType: BlockTypeSealing,
			prev:          SimplexEpochInfo{EpochNumber: 5},
			next: SimplexEpochInfo{
				PrevSealingBlockHash: prevSealingBlockHash,
			},
		},
		{
			name:          "epoch >1 sealing block with wrong hash",
			nextBlockType: BlockTypeSealing,
			prev:          SimplexEpochInfo{EpochNumber: 5},
			next: SimplexEpochInfo{
				PrevSealingBlockHash: [32]byte{9, 9, 9},
			},
			err: fmt.Sprintf("expected prev sealing block hash to be %x but got %x", prevSealingBlockHash, [32]byte{9, 9, 9}),
		},
		{
			name:          "non-sealing block with empty hash",
			nextBlockType: BlockTypeNormal,
			prev:          SimplexEpochInfo{EpochNumber: 1},
			next:          SimplexEpochInfo{},
		},
		{
			name:          "non-sealing block with non-empty hash",
			nextBlockType: BlockTypeNormal,
			prev:          SimplexEpochInfo{EpochNumber: 1},
			next: SimplexEpochInfo{
				PrevSealingBlockHash: [32]byte{1},
			},
			err: fmt.Sprintf("expected prev sealing block hash of a non sealing block to be empty but got %x", [32]byte{1}),
		},
		{
			name:          "telock block with empty hash",
			nextBlockType: BlockTypeTelock,
			prev:          SimplexEpochInfo{EpochNumber: 2},
			next:          SimplexEpochInfo{},
		},
		{
			name:          "new epoch block with empty hash",
			nextBlockType: BlockTypeNewEpoch,
			prev:          SimplexEpochInfo{EpochNumber: 2},
			next:          SimplexEpochInfo{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &prevSealingBlockHashVerifier{
				getBlock:              bs.getBlock,
				latestPersistedHeight: &latestPersisted,
			}
			err := v.Verify(verificationInput{
				nextBlockType:   tc.nextBlockType,
				prevMD:          StateMachineMetadata{SimplexEpochInfo: tc.prev},
				proposedBlockMD: StateMachineMetadata{SimplexEpochInfo: tc.next},
			})
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNextPChainReferenceHeightVerifier(t *testing.T) {
	validators1 := NodeBLSMappings{{BLSKey: []byte{1}, Weight: 1}}
	validators2 := NodeBLSMappings{{BLSKey: []byte{2}, Weight: 1}}

	for _, tc := range []struct {
		name          string
		nextBlockType BlockType
		prev          SimplexEpochInfo
		prevPChainRef uint64
		next          SimplexEpochInfo
		getValidator  ValidatorSetRetriever
		pChainHeight  uint64
		err           string
	}{
		{
			name:          "telock block matching height",
			nextBlockType: BlockTypeTelock,
			prev:          SimplexEpochInfo{NextPChainReferenceHeight: 200},
			next:          SimplexEpochInfo{NextPChainReferenceHeight: 200},
		},
		{
			name:          "telock block mismatched height",
			nextBlockType: BlockTypeTelock,
			prev:          SimplexEpochInfo{NextPChainReferenceHeight: 200},
			next:          SimplexEpochInfo{NextPChainReferenceHeight: 300},
			err:           "expected P-chain reference height to be 200 but got 300",
		},
		{
			name:          "sealing block matching height",
			nextBlockType: BlockTypeSealing,
			prev:          SimplexEpochInfo{NextPChainReferenceHeight: 200},
			next:          SimplexEpochInfo{NextPChainReferenceHeight: 200},
		},
		{
			name:          "sealing block mismatched height",
			nextBlockType: BlockTypeSealing,
			prev:          SimplexEpochInfo{NextPChainReferenceHeight: 200},
			next:          SimplexEpochInfo{NextPChainReferenceHeight: 100},
			err:           "expected P-chain reference height to be 200 but got 100",
		},
		{
			name:          "normal block prev already has next height set",
			nextBlockType: BlockTypeNormal,
			prev:          SimplexEpochInfo{NextPChainReferenceHeight: 200},
			next:          SimplexEpochInfo{NextPChainReferenceHeight: 200},
		},
		{
			name:          "normal block prev already has next height set mismatch",
			nextBlockType: BlockTypeNormal,
			prev:          SimplexEpochInfo{NextPChainReferenceHeight: 200},
			next:          SimplexEpochInfo{NextPChainReferenceHeight: 300},
			err:           "expected P-chain reference height to be 200 but got 300",
		},
		{
			name:          "normal block next p-chain reference height less than current",
			nextBlockType: BlockTypeNormal,
			prev:          SimplexEpochInfo{PChainReferenceHeight: 200},
			next:          SimplexEpochInfo{NextPChainReferenceHeight: 100},
			err:           "expected P-chain reference height to be non-decreasing, but the previous P-chain reference height is 200 and the proposed P-chain reference height is 100",
		},
		{
			name:          "normal block same validator set with non-zero next height",
			nextBlockType: BlockTypeNormal,
			prev:          SimplexEpochInfo{PChainReferenceHeight: 100},
			next:          SimplexEpochInfo{NextPChainReferenceHeight: 200},
			getValidator:  func(h uint64) (NodeBLSMappings, error) { return validators1, nil },
			err:           "validator set at proposed next P-chain reference height 200 is the same as validator set at previous block's P-chain reference height 100,so expected next P-chain reference height to remain the same but got 200",
		},
		{
			name:          "normal block no validator change and next height is zero",
			nextBlockType: BlockTypeNormal,
			prev:          SimplexEpochInfo{PChainReferenceHeight: 100},
			next:          SimplexEpochInfo{NextPChainReferenceHeight: 0},
			getValidator:  func(h uint64) (NodeBLSMappings, error) { return validators1, nil },
		},
		{
			name:          "normal block validator change detected and p-chain height reached",
			nextBlockType: BlockTypeNormal,
			prev:          SimplexEpochInfo{PChainReferenceHeight: 100},
			next:          SimplexEpochInfo{NextPChainReferenceHeight: 200},
			getValidator: func(h uint64) (NodeBLSMappings, error) {
				if h == 200 {
					return validators2, nil
				}
				return validators1, nil
			},
			pChainHeight: 200,
		},
		{
			name:          "normal block validator change but p-chain height not reached",
			nextBlockType: BlockTypeNormal,
			prev:          SimplexEpochInfo{PChainReferenceHeight: 100},
			next:          SimplexEpochInfo{NextPChainReferenceHeight: 200},
			getValidator: func(h uint64) (NodeBLSMappings, error) {
				if h == 200 {
					return validators2, nil
				}
				return validators1, nil
			},
			pChainHeight: 150,
			err:          "haven't reached P-chain height 200 yet, current P-chain height is only 150",
		},
		{
			name:          "new epoch block with zero next height",
			nextBlockType: BlockTypeNewEpoch,
			next:          SimplexEpochInfo{NextPChainReferenceHeight: 0},
		},
		{
			name:          "new epoch block with non-zero next height",
			nextBlockType: BlockTypeNewEpoch,
			next:          SimplexEpochInfo{NextPChainReferenceHeight: 100},
			err:           "expected P-chain reference height to be 0 but got 100",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &nextPChainReferenceHeightVerifier{
				getValidatorSet: tc.getValidator,
				getPChainHeight: func() uint64 { return tc.pChainHeight },
			}
			err := v.Verify(verificationInput{
				nextBlockType:   tc.nextBlockType,
				prevMD:          StateMachineMetadata{SimplexEpochInfo: tc.prev},
				proposedBlockMD: StateMachineMetadata{SimplexEpochInfo: tc.next},
			})
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestVMBlockSeqVerifier(t *testing.T) {
	prevMDBytes := (&simplex.ProtocolMetadata{Seq: 5, Prev: [32]byte{1}}).Bytes()
	proposedMDBytes := (&simplex.ProtocolMetadata{Seq: 6, Prev: [32]byte{2}}).Bytes()

	blockWithInner := StateMachineBlock{
		InnerBlock: &testVMBlock{bytes: []byte{1}},
	}
	blockWithoutInner := StateMachineBlock{}

	for _, tc := range []struct {
		name         string
		prev         SimplexEpochInfo
		prevMD       StateMachineMetadata
		next         SimplexEpochInfo
		prevBlockSeq uint64
		block        StateMachineBlock
		err          string
	}{
		{
			name:         "first simplex block matching seq",
			prev:         SimplexEpochInfo{EpochNumber: 0},
			next:         SimplexEpochInfo{PrevVMBlockSeq: 42},
			prevBlockSeq: 42,
		},
		{
			name:         "first simplex block wrong seq",
			prev:         SimplexEpochInfo{EpochNumber: 0},
			next:         SimplexEpochInfo{PrevVMBlockSeq: 10},
			prevBlockSeq: 42,
			err:          "expected PrevVMBlockSeq to be 42 but got 10",
		},
		{
			name:         "prev block has inner block",
			prev:         SimplexEpochInfo{EpochNumber: 1, PrevVMBlockSeq: 3},
			prevMD:       StateMachineMetadata{SimplexProtocolMetadata: prevMDBytes, SimplexEpochInfo: SimplexEpochInfo{EpochNumber: 1, PrevVMBlockSeq: 3}},
			next:         SimplexEpochInfo{PrevVMBlockSeq: 4},
			prevBlockSeq: 4,
			block:        blockWithInner,
		},
		{
			name:         "prev block has inner block wrong seq",
			prev:         SimplexEpochInfo{EpochNumber: 1, PrevVMBlockSeq: 3},
			prevMD:       StateMachineMetadata{SimplexProtocolMetadata: prevMDBytes, SimplexEpochInfo: SimplexEpochInfo{EpochNumber: 1, PrevVMBlockSeq: 3}},
			next:         SimplexEpochInfo{PrevVMBlockSeq: 99},
			prevBlockSeq: 4,
			block:        blockWithInner,
			err:          "expected PrevVMBlockSeq to be 4 but got 99",
		},
		{
			name:         "prev block has no inner block uses parent PrevVMBlockSeq",
			prev:         SimplexEpochInfo{EpochNumber: 1, PrevVMBlockSeq: 3},
			prevMD:       StateMachineMetadata{SimplexProtocolMetadata: prevMDBytes, SimplexEpochInfo: SimplexEpochInfo{EpochNumber: 1, PrevVMBlockSeq: 3}},
			next:         SimplexEpochInfo{PrevVMBlockSeq: 3},
			prevBlockSeq: 4,
			block:        blockWithoutInner,
		},
		{
			name:         "prev block has no inner block wrong seq",
			prev:         SimplexEpochInfo{EpochNumber: 1, PrevVMBlockSeq: 3},
			prevMD:       StateMachineMetadata{SimplexProtocolMetadata: prevMDBytes, SimplexEpochInfo: SimplexEpochInfo{EpochNumber: 1, PrevVMBlockSeq: 3}},
			next:         SimplexEpochInfo{PrevVMBlockSeq: 99},
			prevBlockSeq: 4,
			block:        blockWithoutInner,
			err:          "expected PrevVMBlockSeq to be 3 but got 99",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bs := make(testBlockStore)
			bs[tc.prevBlockSeq] = tc.block

			v := &vmBlockSeqVerifier{
				getBlock: bs.getBlock,
			}

			prevMD := tc.prevMD
			if prevMD.SimplexEpochInfo.EpochNumber == 0 && tc.prev.EpochNumber == 0 {
				prevMD.SimplexEpochInfo = tc.prev
			}

			err := v.Verify(verificationInput{
				prevMD:          prevMD,
				proposedBlockMD: StateMachineMetadata{SimplexEpochInfo: tc.next, SimplexProtocolMetadata: proposedMDBytes},
				prevBlockSeq:    tc.prevBlockSeq,
			})
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidationDescriptorVerifier(t *testing.T) {
	validators := NodeBLSMappings{
		{BLSKey: []byte{1}, Weight: 1},
		{BLSKey: []byte{2}, Weight: 1},
	}

	otherValidators := NodeBLSMappings{
		{BLSKey: []byte{3}, Weight: 1},
	}

	for _, tc := range []struct {
		name          string
		nextBlockType BlockType
		next          SimplexEpochInfo
		getValidator  ValidatorSetRetriever
		err           string
	}{
		{
			name:          "sealing block with matching validators",
			nextBlockType: BlockTypeSealing,
			next: SimplexEpochInfo{
				NextPChainReferenceHeight: 100,
				BlockValidationDescriptor: &BlockValidationDescriptor{
					AggregatedMembership: AggregatedMembership{Members: validators},
				},
			},
			getValidator: func(h uint64) (NodeBLSMappings, error) { return validators, nil },
		},
		{
			name:          "sealing block with mismatching validators",
			nextBlockType: BlockTypeSealing,
			next: SimplexEpochInfo{
				NextPChainReferenceHeight: 100,
				BlockValidationDescriptor: &BlockValidationDescriptor{
					AggregatedMembership: AggregatedMembership{Members: otherValidators},
				},
			},
			getValidator: func(h uint64) (NodeBLSMappings, error) { return validators, nil },
			err:          "expected validator set specified at P-chain height 100 does not match validator set encoded in new inner block",
		},
		{
			name:          "sealing block with validator retrieval error",
			nextBlockType: BlockTypeSealing,
			next: SimplexEpochInfo{
				NextPChainReferenceHeight: 100,
				BlockValidationDescriptor: &BlockValidationDescriptor{},
			},
			getValidator: func(h uint64) (NodeBLSMappings, error) { return nil, fmt.Errorf("unavailable") },
			err:          "unavailable",
		},
		{
			name:          "normal block with nil descriptor",
			nextBlockType: BlockTypeNormal,
			next:          SimplexEpochInfo{},
		},
		{
			name:          "normal block with non-nil descriptor",
			nextBlockType: BlockTypeNormal,
			next: SimplexEpochInfo{
				BlockValidationDescriptor: &BlockValidationDescriptor{},
			},
			err: "inner block validation descriptor should be nil but got &{{[] {0}} {0}}",
		},
		{
			name:          "telock block with nil descriptor",
			nextBlockType: BlockTypeTelock,
			next:          SimplexEpochInfo{},
		},
		{
			name:          "new epoch block with nil descriptor",
			nextBlockType: BlockTypeNewEpoch,
			next:          SimplexEpochInfo{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &validationDescriptorVerifier{
				getValidatorSet: tc.getValidator,
			}
			err := v.Verify(verificationInput{
				nextBlockType:   tc.nextBlockType,
				proposedBlockMD: StateMachineMetadata{SimplexEpochInfo: tc.next},
			})
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNextEpochApprovalsVerifier(t *testing.T) {
	validators := NodeBLSMappings{
		{BLSKey: []byte{1}, Weight: 1},
		{BLSKey: []byte{2}, Weight: 1},
		{BLSKey: []byte{3}, Weight: 1},
	}

	for _, tc := range []struct {
		name          string
		nextBlockType BlockType
		prev          SimplexEpochInfo
		next          SimplexEpochInfo
		auxInfo       *AuxiliaryInfo
		getValidator  ValidatorSetRetriever
		sigVerifier   SignatureVerifier
		keyAggregator KeyAggregator
		err           string
	}{
		{
			name:          "sealing block with nil approvals",
			nextBlockType: BlockTypeSealing,
			next:          SimplexEpochInfo{},
			err:           "next epoch approvals should not be nil for a sealing inner block",
		},
		{
			name:          "sealing block with validator retrieval error",
			nextBlockType: BlockTypeSealing,
			next: SimplexEpochInfo{
				NextPChainReferenceHeight: 100,
				NextEpochApprovals:        &NextEpochApprovals{NodeIDs: []byte{7}, Signature: []byte("sig")},
			},
			getValidator: func(h uint64) (NodeBLSMappings, error) { return nil, fmt.Errorf("unavailable") },
			err:          "unavailable",
		},
		{
			name:          "sealing block not enough approvals",
			nextBlockType: BlockTypeSealing,
			next: SimplexEpochInfo{
				NextPChainReferenceHeight: 100,
				NextEpochApprovals:        &NextEpochApprovals{NodeIDs: []byte{1}, Signature: []byte("sig")},
			},
			getValidator:  func(h uint64) (NodeBLSMappings, error) { return validators, nil },
			sigVerifier:   &testSigVerifier{},
			keyAggregator: &testKeyAggregator{},
			err:           "not enough approvals to seal inner block",
		},
		{
			name:          "sealing block enough approvals",
			nextBlockType: BlockTypeSealing,
			next: SimplexEpochInfo{
				NextPChainReferenceHeight: 100,
				NextEpochApprovals:        &NextEpochApprovals{NodeIDs: []byte{7}, Signature: []byte("sig")},
			},
			getValidator:  func(h uint64) (NodeBLSMappings, error) { return validators, nil },
			sigVerifier:   &testSigVerifier{},
			keyAggregator: &testKeyAggregator{},
		},
		{
			name:          "normal block no validator change",
			nextBlockType: BlockTypeNormal,
			prev:          SimplexEpochInfo{NextPChainReferenceHeight: 0},
			next:          SimplexEpochInfo{},
		},
		{
			name:          "normal block collecting approvals with nil approvals",
			nextBlockType: BlockTypeNormal,
			prev:          SimplexEpochInfo{NextPChainReferenceHeight: 100},
			next:          SimplexEpochInfo{NextPChainReferenceHeight: 100},
			err:           "next epoch approvals should not be nil when collecting approvals",
		},
		{
			name:          "normal block collecting approvals valid",
			nextBlockType: BlockTypeNormal,
			prev: SimplexEpochInfo{
				NextPChainReferenceHeight: 100,
				PChainReferenceHeight:     50,
			},
			next: SimplexEpochInfo{
				NextPChainReferenceHeight: 100,
				NextEpochApprovals:        &NextEpochApprovals{NodeIDs: []byte{1}, Signature: []byte("sig")},
			},
			getValidator:  func(h uint64) (NodeBLSMappings, error) { return validators, nil },
			sigVerifier:   &testSigVerifier{},
			keyAggregator: &testKeyAggregator{},
		},
		{
			name:          "normal block collecting approvals signers not superset of prev",
			nextBlockType: BlockTypeNormal,
			prev: SimplexEpochInfo{
				NextPChainReferenceHeight: 100,
				PChainReferenceHeight:     50,
				NextEpochApprovals:        &NextEpochApprovals{NodeIDs: []byte{3}, Signature: []byte("sig")}, // bits 0,1
			},
			next: SimplexEpochInfo{
				NextPChainReferenceHeight: 100,
				NextEpochApprovals:        &NextEpochApprovals{NodeIDs: []byte{1}, Signature: []byte("sig")}, // bit 0 only
			},
			getValidator:  func(h uint64) (NodeBLSMappings, error) { return validators, nil },
			sigVerifier:   &testSigVerifier{},
			keyAggregator: &testKeyAggregator{},
			err:           "some signers from parent inner block are missing from next epoch approvals of proposed inner block",
		},
		{
			name:          "telock block with nil approvals",
			nextBlockType: BlockTypeTelock,
			next:          SimplexEpochInfo{},
		},
		{
			name:          "telock block with non-nil approvals",
			nextBlockType: BlockTypeTelock,
			next: SimplexEpochInfo{
				NextEpochApprovals: &NextEpochApprovals{},
			},
			err: "next epoch approvals should be nil but got &{[] [] {0}}",
		},
		{
			name:          "new epoch block with nil approvals",
			nextBlockType: BlockTypeNewEpoch,
			next:          SimplexEpochInfo{},
		},
		{
			name:          "new epoch block with non-nil approvals",
			nextBlockType: BlockTypeNewEpoch,
			next: SimplexEpochInfo{
				NextEpochApprovals: &NextEpochApprovals{},
			},
			err: "next epoch approvals should be nil but got &{[] [] {0}}",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &nextEpochApprovalsVerifier{
				sigVerifier:     tc.sigVerifier,
				getValidatorSet: tc.getValidator,
				keyAggregator:   tc.keyAggregator,
			}
			err := v.Verify(verificationInput{
				nextBlockType:   tc.nextBlockType,
				prevMD:          StateMachineMetadata{SimplexEpochInfo: tc.prev},
				proposedBlockMD: StateMachineMetadata{SimplexEpochInfo: tc.next, AuxiliaryInfo: tc.auxInfo},
			})
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSealingBlockSeqVerifier(t *testing.T) {
	prevProtocolMD := (&simplex.ProtocolMetadata{Seq: 5}).Bytes()

	for _, tc := range []struct {
		name          string
		nextBlockType BlockType
		prev          SimplexEpochInfo
		prevMD        StateMachineMetadata
		next          SimplexEpochInfo
		err           string
	}{
		{
			name:          "normal block with zero sealing seq",
			nextBlockType: BlockTypeNormal,
			next:          SimplexEpochInfo{SealingBlockSeq: 0},
		},
		{
			name:          "normal block with non-zero sealing seq",
			nextBlockType: BlockTypeNormal,
			next:          SimplexEpochInfo{SealingBlockSeq: 5},
			err:           "expected sealing block sequence number to be 0 but got 5",
		},
		{
			name:          "new epoch block with zero sealing seq",
			nextBlockType: BlockTypeNewEpoch,
			next:          SimplexEpochInfo{SealingBlockSeq: 0},
		},
		{
			name:          "new epoch block with non-zero sealing seq",
			nextBlockType: BlockTypeNewEpoch,
			next:          SimplexEpochInfo{SealingBlockSeq: 3},
			err:           "expected sealing block sequence number to be 0 but got 3",
		},
		{
			name:          "telock block matching prev sealing seq",
			nextBlockType: BlockTypeTelock,
			prev:          SimplexEpochInfo{SealingBlockSeq: 10},
			next:          SimplexEpochInfo{SealingBlockSeq: 10},
		},
		{
			name:          "telock block mismatching prev sealing seq",
			nextBlockType: BlockTypeTelock,
			prev:          SimplexEpochInfo{SealingBlockSeq: 10},
			next:          SimplexEpochInfo{SealingBlockSeq: 11},
			err:           "expected sealing block sequence number to be 10 but got 11",
		},
		{
			name:          "sealing block with zero seq",
			nextBlockType: BlockTypeSealing,
			prevMD:        StateMachineMetadata{SimplexProtocolMetadata: prevProtocolMD},
			next:          SimplexEpochInfo{SealingBlockSeq: 0},
		},
		{
			name:          "sealing block with non-zero seq",
			nextBlockType: BlockTypeSealing,
			prevMD:        StateMachineMetadata{SimplexProtocolMetadata: prevProtocolMD},
			next:          SimplexEpochInfo{SealingBlockSeq: 10},
			err:           "expected sealing inner block sequence number to be 0 but got 10",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &sealingBlockSeqVerifier{}
			prevMD := tc.prevMD
			prevMD.SimplexEpochInfo = tc.prev
			err := v.Verify(verificationInput{
				nextBlockType:   tc.nextBlockType,
				prevMD:          prevMD,
				proposedBlockMD: StateMachineMetadata{SimplexEpochInfo: tc.next},
			})
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Test helpers

type testBlockStore map[uint64]StateMachineBlock

func (bs testBlockStore) getBlock(opts RetrievingOpts) (StateMachineBlock, *simplex.Finalization, error) {
	blk, ok := bs[opts.Height]
	if !ok {
		return StateMachineBlock{}, nil, fmt.Errorf("%w: block %d", simplex.ErrBlockNotFound, opts.Height)
	}
	return blk, nil, nil
}

type testVMBlock struct {
	bytes  []byte
	height uint64
}

func (b *testVMBlock) Digest() [32]byte {
	return sha256.Sum256(b.bytes)
}

func (b *testVMBlock) Height() uint64 {
	return b.height
}

func (b *testVMBlock) Timestamp() time.Time {
	return time.Now()
}

func (b *testVMBlock) Verify(_ context.Context) error {
	return nil
}

type testSigVerifier struct {
	err error
}

func (sv *testSigVerifier) VerifySignature(_, _, _ []byte) error {
	return sv.err
}

type testKeyAggregator struct {
	err error
}

func (ka *testKeyAggregator) AggregateKeys(keys ...[]byte) ([]byte, error) {
	if ka.err != nil {
		return nil, ka.err
	}
	var agg []byte
	for _, k := range keys {
		agg = append(agg, k...)
	}
	return agg, nil
}

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

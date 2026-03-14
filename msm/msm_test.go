// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/asn1"
	"fmt"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

type outerBlock struct {
	finalization *simplex.Finalization
	block        metadata.StateMachineBlock
}

type blockStore map[uint64]*outerBlock

func (bs blockStore) getBlock(height uint64) (metadata.StateMachineBlock, *simplex.Finalization, error) {
	blk, exits := bs[height]
	if !exits {
		return metadata.StateMachineBlock{}, nil, fmt.Errorf("block %d not found", height)
	}
	return blk.block, blk.finalization, nil
}

type approvalsRetriever struct {
	result metadata.ValidatorSetApprovals
}

func (a approvalsRetriever) RetrieveApprovals() metadata.ValidatorSetApprovals {
	return a.result
}

type signatureVerifier struct {
	err error
}

func (sv *signatureVerifier) VerifySignature(signature []byte, message []byte, publicKey []byte) error {
	return sv.err
}

type signatureAggregator struct {
}

type aggregatrdSignature struct {
	Signatures [][]byte
}

func (sv *signatureAggregator) AggregateSignatures(signatures ...[]byte) ([]byte, error) {
	bytes, err := asn1.Marshal(aggregatrdSignature{Signatures: signatures})
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

type blockBuilder struct {
	block metadata.VMBlock
	err   error
}

func (bb *blockBuilder) BuildBlock(_ context.Context, _ uint64) (metadata.VMBlock, error) {
	return bb.block, bb.err
}

type validatorSetRetriever struct {
	result    metadata.NodeBLSMappings
	resultMap map[uint64]metadata.NodeBLSMappings
	err       error
}

func (vsr *validatorSetRetriever) getValidatorSet(height uint64) (metadata.NodeBLSMappings, error) {
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
	genesisBlock = metadata.StateMachineBlock{
		// Genesis block metadata has all zero values
		InnerBlock: &innerBlock{
			ts:    time.Now(),
			bytes: []byte{1, 2, 3},
		},
	}

	notGenesisBlock = metadata.StateMachineBlock{
		InnerBlock: &innerBlock{
			ts:     time.Now(),
			height: 9,
			bytes:  []byte{1, 2, 3},
		},
		Metadata: metadata.StateMachineMetadata{
			PChainHeight: 100,
			SimplexProtocolMetadata: (&simplex.ProtocolMetadata{
				Round: 8,
				Seq:   9,
				Epoch: 1,
				Prev:  [32]byte{1, 2, 3},
			}).Bytes(),
			SimplexEpochInfo: metadata.SimplexEpochInfo{
				PrevSealingBlockHash:  [32]byte{1, 1, 1, 1},
				PChainReferenceHeight: 100,
				EpochNumber:           1,
				PrevVMBlockSeq:        7,
				BlockValidationDescriptor: &metadata.BlockValidationDescriptor{
					AggregatedMembership: metadata.AggregatedMembership{
						Members: metadata.NodeBLSMappings{
							{BLSKey: []byte{1}, Weight: 1}, {BLSKey: []byte{2}, Weight: 1},
							{BLSKey: []byte{3}, Weight: 1}, {BLSKey: []byte{4}, Weight: 1},
						},
					},
				},
			},
		},
	}
)

type innerBlock struct {
	ts     time.Time
	height uint64
	bytes  []byte
}

func (i *innerBlock) Digest() [32]byte {
	return sha256.Sum256(i.bytes)
}

func (i *innerBlock) Height() uint64 {
	return i.height
}

func (i *innerBlock) Timestamp() time.Time {
	return i.ts
}

func (i *innerBlock) Verify(ctx context.Context) error {
	return nil
}

func TestMSMFirstBlockAfterGenesis(t *testing.T) {
	validMD := simplex.ProtocolMetadata{
		Round: 0,
		Seq:   1,
		Epoch: 1,
		Prev:  genesisBlock.Digest(),
	}

	for _, testCase := range []struct {
		name        string
		md          simplex.ProtocolMetadata
		err         string
		configure   func(*metadata.StateMachine, *testConfig)
		mutateBlock func(*metadata.StateMachineBlock)
	}{
		{
			name: "correct information",
			md:   validMD,
		},
		{
			name: "trying to build a genesis block",
			md:   validMD,
			mutateBlock: func(block *metadata.StateMachineBlock) {
				md, err := simplex.ProtocolMetadataFromBytes(block.Metadata.SimplexProtocolMetadata)
				require.NoError(t, err)
				md.Seq = 0
				block.Metadata.SimplexProtocolMetadata = md.Bytes()
			},
			err: "attempted to build a genesis inner block",
		},
		{
			name: "previous block not found",
			md:   validMD,
			configure: func(_ *metadata.StateMachine, tc *testConfig) {
				delete(tc.blockStore, 0)
			},
			err: "failed to retrieve previous (0) inner block",
		},
		{
			name: "parent has no inner block",
			md:   validMD,
			configure: func(_ *metadata.StateMachine, tc *testConfig) {
				tc.blockStore[0] = &outerBlock{
					block: metadata.StateMachineBlock{},
				}
			},
			err: "parent inner block (",
		},
		{
			name: "wrong epoch number",
			md:   validMD,
			mutateBlock: func(block *metadata.StateMachineBlock) {
				block.Metadata.SimplexEpochInfo.EpochNumber = 2
			},
			err: "invalid epoch number (2), should be 1",
		},
		{
			name: "P-chain height too big",
			md:   validMD,
			mutateBlock: func(block *metadata.StateMachineBlock) {
				block.Metadata.PChainHeight = 110
			},
			err: "invalid P-chain height (110) is too big",
		},
		{
			name: "P-chain height smaller than parent",
			md:   validMD,
			configure: func(_ *metadata.StateMachine, tc *testConfig) {
				tc.blockStore[0] = &outerBlock{
					block: metadata.StateMachineBlock{
						InnerBlock: &innerBlock{ts: time.Now(), bytes: []byte{1, 2, 3}},
						Metadata:   metadata.StateMachineMetadata{PChainHeight: 110},
					},
				}
			},
			err: "invalid P-chain height (100) is smaller than parent InnerBlock's P-chain height (110)",
		},
		{
			name: "validator set retrieval fails",
			md:   validMD,
			configure: func(_ *metadata.StateMachine, tc *testConfig) {
				tc.validatorSetRetriever.err = fmt.Errorf("validator set unavailable")
			},
			err: "failed to retrieve validator set",
		},
		{
			name: "nil BlockValidationDescriptor",
			md:   validMD,
			mutateBlock: func(block *metadata.StateMachineBlock) {
				block.Metadata.SimplexEpochInfo.BlockValidationDescriptor = nil
			},
			err: "invalid BlockValidationDescriptor: should not be nil",
		},
		{
			name: "membership mismatch",
			md:   validMD,
			configure: func(_ *metadata.StateMachine, tc *testConfig) {
				tc.validatorSetRetriever.result = metadata.NodeBLSMappings{
					{BLSKey: []byte{1}, Weight: 1},
				}
			},
			err: "invalid BlockValidationDescriptor: should match validator set",
		},
		{
			name: "SimplexEpochInfo mismatch",
			md:   validMD,
			mutateBlock: func(block *metadata.StateMachineBlock) {
				block.Metadata.SimplexEpochInfo.PrevVMBlockSeq = 999
			},
			err: "invalid SimplexEpochInfo",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			sm1, testConfig1 := newStateMachine(t)
			sm2, testConfig2 := newStateMachine(t)

			testConfig1.blockStore[0] = &outerBlock{
				block: genesisBlock,
			}

			testConfig2.blockStore[0] = &outerBlock{
				block: genesisBlock,
			}

			if testCase.configure != nil {
				testCase.configure(&sm2, testConfig2)
			}

			block, err := sm1.BuildBlock(context.Background(), genesisBlock, testCase.md, nil)
			require.NoError(t, err)
			require.NotNil(t, block)

			if testCase.mutateBlock != nil {
				testCase.mutateBlock(block)
			}

			err = sm2.VerifyBlock(context.Background(), block)
			if testCase.err != "" {
				require.ErrorContains(t, err, testCase.err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestMSMFirstSimplexBlockAfterPreSimplexBlocks(t *testing.T) {
	preSimplexParent := metadata.StateMachineBlock{
		InnerBlock: &innerBlock{
			ts:     time.Now(),
			height: 42,
			bytes:  []byte{4, 5, 6},
		},
		// Zero-valued metadata means this is a pre-Simplex block or a genesis block.
		// But since the height is 42, it can't be a genesis block, so it must be a pre-Simplex block.
		Metadata: metadata.StateMachineMetadata{},
	}

	md := simplex.ProtocolMetadata{
		Round: 0,
		Seq:   43,
		Epoch: 1,
		Prev:  preSimplexParent.Digest(),
	}

	sm1, testConfig1 := newStateMachine(t)
	sm2, testConfig2 := newStateMachine(t)

	testConfig1.blockStore[42] = &outerBlock{block: preSimplexParent}
	testConfig2.blockStore[42] = &outerBlock{block: preSimplexParent}

	testConfig1.blockBuilder.block = &innerBlock{
		ts:     time.Now(),
		height: 43,
		bytes:  []byte{7, 8, 9},
	}

	block, err := sm1.BuildBlock(context.Background(), preSimplexParent, md, nil)
	require.NoError(t, err)
	require.NotNil(t, block)

	require.NoError(t, sm2.VerifyBlock(context.Background(), block))

	require.Equal(t, &metadata.StateMachineBlock{
		InnerBlock: &innerBlock{
			ts:     testConfig1.blockBuilder.block.Timestamp(),
			height: 43,
			bytes:  []byte{7, 8, 9},
		},
		Metadata: metadata.StateMachineMetadata{
			Timestamp:               uint64(testConfig1.blockBuilder.block.Timestamp().Unix()),
			PChainHeight:            100,
			SimplexProtocolMetadata: md.Bytes(),
			SimplexEpochInfo: metadata.SimplexEpochInfo{
				PChainReferenceHeight: 100,
				EpochNumber:           1,
				PrevVMBlockSeq:        42,
				BlockValidationDescriptor: &metadata.BlockValidationDescriptor{
					AggregatedMembership: metadata.AggregatedMembership{
						Members: testConfig1.validatorSetRetriever.result,
					},
				},
			},
		},
	}, block)
}

func TestMSMNormalOp(t *testing.T) {
	newPChainHeight := uint64(200)
	newValidatorSet := metadata.NodeBLSMappings{
		{BLSKey: []byte{5}, Weight: 1}, {BLSKey: []byte{6}, Weight: 1}, {BLSKey: []byte{7}, Weight: 1},
	}

	for _, testCase := range []struct {
		name                      string
		setup                     func(*metadata.StateMachine, *testConfig)
		mutateBlock               func(*metadata.StateMachineBlock)
		err                       string
		expectedPChainHeight      uint64
		expectedNextPChainRefHeight uint64
	}{
		{
			name:                 "correct information",
			expectedPChainHeight: 100,
		},
		{
			name: "trying to build a genesis block",
			mutateBlock: func(block *metadata.StateMachineBlock) {
				md, err := simplex.ProtocolMetadataFromBytes(block.Metadata.SimplexProtocolMetadata)
				require.NoError(t, err)
				md.Seq = 0
				block.Metadata.SimplexProtocolMetadata = md.Bytes()
			},
			err: "attempted to build a genesis inner block",
		},
		{
			name: "previous block not found",
			mutateBlock: func(block *metadata.StateMachineBlock) {
				md, err := simplex.ProtocolMetadataFromBytes(block.Metadata.SimplexProtocolMetadata)
				require.NoError(t, err)
				md.Seq = 999
				block.Metadata.SimplexProtocolMetadata = md.Bytes()
			},
			err: "failed to retrieve previous (998) inner block",
		},
		{
			name: "P-chain height too big",
			mutateBlock: func(block *metadata.StateMachineBlock) {
				block.Metadata.PChainHeight = 110
			},
			err: "invalid P-chain reference height (110) is too big",
		},
		{
			name: "P-chain height smaller than parent",
			mutateBlock: func(block *metadata.StateMachineBlock) {
				block.Metadata.PChainHeight = 0
			},
			err: "invalid P-chain height (0) is smaller than parent inner block's P-chain height (100)",
		},
		{
			name: "wrong epoch number",
			mutateBlock: func(block *metadata.StateMachineBlock) {
				block.Metadata.SimplexEpochInfo.EpochNumber = 2
			},
			err: "expected epoch number to be 1 but got 2",
		},
		{
			name: "non-nil BlockValidationDescriptor",
			mutateBlock: func(block *metadata.StateMachineBlock) {
				block.Metadata.SimplexEpochInfo.BlockValidationDescriptor = &metadata.BlockValidationDescriptor{}
			},
			err: "failed to find first Simplex inner block",
		},
		{
			name: "non-zero sealing block seq",
			mutateBlock: func(block *metadata.StateMachineBlock) {
				block.Metadata.SimplexEpochInfo.SealingBlockSeq = 5
			},
			err: "expected sealing inner block sequence number to be 0 but got 5",
		},
		{
			name: "wrong PChainReferenceHeight",
			mutateBlock: func(block *metadata.StateMachineBlock) {
				block.Metadata.SimplexEpochInfo.PChainReferenceHeight = 50
			},
			err: "expected P-chain reference height to be 100 but got 50",
		},
		{
			name: "non-empty PrevSealingBlockHash",
			mutateBlock: func(block *metadata.StateMachineBlock) {
				block.Metadata.SimplexEpochInfo.PrevSealingBlockHash = [32]byte{1, 2, 3}
			},
			err: "expected prev sealing inner block hash of a non sealing inner block to be empty",
		},
		{
			name: "wrong PrevVMBlockSeq",
			mutateBlock: func(block *metadata.StateMachineBlock) {
				block.Metadata.SimplexEpochInfo.PrevVMBlockSeq = 999
			},
			err: "expected PrevVMBlockSeq to be",
		},
		{
			name: "validator set change detected",
			setup: func(sm *metadata.StateMachine, tc *testConfig) {
				tc.validatorSetRetriever.resultMap = map[uint64]metadata.NodeBLSMappings{
					newPChainHeight: newValidatorSet,
				}
				sm.GetPChainHeight = func() uint64 { return newPChainHeight }
			},
			expectedPChainHeight:        newPChainHeight,
			expectedNextPChainRefHeight: newPChainHeight,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			chain := makeChain(t, 5, 10)
			sm1, testConfig1 := newStateMachine(t)
			sm2, testConfig2 := newStateMachine(t)

			for i, block := range chain {
				testConfig1.blockStore[uint64(i)] = &outerBlock{block: block}
				testConfig2.blockStore[uint64(i)] = &outerBlock{block: block}
			}

			lastBlock := chain[len(chain)-1]
			md, err := simplex.ProtocolMetadataFromBytes(lastBlock.Metadata.SimplexProtocolMetadata)
			require.NoError(t, err)

			md.Seq++
			md.Round++
			md.Prev = lastBlock.Digest()

			var blacklist simplex.Blacklist
			blacklist.NodeCount = 4

			blockTime := lastBlock.InnerBlock.Timestamp().Add(time.Second)

			var icmEpochInvokeCount int

			sm1.ComputeICMEpoch = func(_ any, input metadata.ICMEpochInput) metadata.ICMEpoch {
				icmEpochInvokeCount++
				require.Equal(t, metadata.ICMEpochInput{
					ParentPChainHeight: 100,
					ChildTimestamp: blockTime,
					ParentTimestamp: time.Unix(int64(lastBlock.Metadata.Timestamp), 0),
					ParentEpoch: metadata.ICMEpoch{},
				}, input)
				return input.ParentEpoch
			}

			content := make([]byte, 10)
			_, err = rand.Read(content)
			require.NoError(t, err)

			testConfig1.blockBuilder.block = &innerBlock{
				ts:     blockTime,
				height: lastBlock.InnerBlock.Height(),
				bytes:  content,
			}

			if testCase.setup != nil {
				testCase.setup(&sm1, testConfig1)
				testCase.setup(&sm2, testConfig2)
			}

			block1, err := sm1.BuildBlock(context.Background(), lastBlock, *md, &blacklist)
			require.NoError(t, err)
			require.NotNil(t, block1)

			require.Equal(t, 1, icmEpochInvokeCount, "ComputeICMEpoch should have been invoked exactly once")

			if testCase.mutateBlock != nil {
				testCase.mutateBlock(block1)
			}

			err = sm2.VerifyBlock(context.Background(), block1)
			if testCase.err != "" {
				require.ErrorContains(t, err, testCase.err)
				return
			}
			require.NoError(t, err)

			require.Equal(t, &metadata.StateMachineBlock{
				InnerBlock: &innerBlock{
					ts:     blockTime,
					height: lastBlock.InnerBlock.Height(),
					bytes:  content,
				},
				Metadata: metadata.StateMachineMetadata{
					SimplexBlacklist:        blacklist.Bytes(),
					Timestamp:               uint64(blockTime.Unix()),
					PChainHeight:            testCase.expectedPChainHeight,
					SimplexProtocolMetadata: md.Bytes(),
					SimplexEpochInfo: metadata.SimplexEpochInfo{
						PChainReferenceHeight:     100,
						EpochNumber:               1,
						PrevVMBlockSeq:            lastBlock.InnerBlock.Height(),
						NextPChainReferenceHeight: testCase.expectedNextPChainRefHeight,
					},
				},
			}, block1)
		})
	}
}

func TestMSMFullEpochLifecycle(t *testing.T) {
	// Validator sets: epoch 1 uses validatorSet1, epoch 2 uses validatorSet2.
	node1 := [20]byte{1}
	node2 := [20]byte{2}
	node3 := [20]byte{3}

	validatorSet1 := metadata.NodeBLSMappings{
		{NodeID: node1, BLSKey: []byte{1}, Weight: 1},
		{NodeID: node2, BLSKey: []byte{2}, Weight: 1},
		{NodeID: node3, BLSKey: []byte{3}, Weight: 1},
	}
	validatorSet2 := metadata.NodeBLSMappings{
		{NodeID: node1, BLSKey: []byte{1}, Weight: 1},
		{NodeID: node2, BLSKey: []byte{4}, Weight: 1},
		{NodeID: node3, BLSKey: []byte{5}, Weight: 1},
	}

	pChainHeight1 := uint64(100)
	pChainHeight2 := uint64(200)

	currentPChainHeight := pChainHeight1
	bs := make(blockStore)

	var approvalsResult metadata.ValidatorSetApprovals

	sm := metadata.StateMachine{
		LatestPersistedHeight:    1,
		Logger:                   testutil.MakeLogger(t),
		GetBlock:                 bs.getBlock,
		MaxBlockBuildingWaitTime: time.Second,
		ApprovalsRetriever:       &approvalsRetriever{},
		SignatureVerifier:        &signatureVerifier{},
		SignatureAggregator:      &signatureAggregator{},
		BlockBuilder:             &blockBuilder{},
		KeyAggregator:            &keyAggregator{},
		ComputeICMEpoch: func(_ any, input metadata.ICMEpochInput) metadata.ICMEpoch {
			return input.ParentEpoch
		},
		GetPChainHeight: func() uint64 {
			return currentPChainHeight
		},
		GetUpgrades: func() any {
			return nil
		},
		GetValidatorSet: func(height uint64) (metadata.NodeBLSMappings, error) {
			if height == pChainHeight2 {
				return validatorSet2, nil
			}
			return validatorSet1, nil
		},
	}

	// Override ApprovalsRetriever to use our dynamic approvals.
	sm.ApprovalsRetriever = &dynamicApprovalsRetriever{approvals: &approvalsResult}

	startTime := time.Now()
	seq := uint64(0)
	round := uint64(0)

	// Helper to build next protocol metadata.
	nextMD := func(parent *metadata.StateMachineBlock) simplex.ProtocolMetadata {
		seq++
		round++
		return simplex.ProtocolMetadata{
			Seq:   seq,
			Round: round,
			Epoch: 1,
			Prev:  parent.Digest(),
		}
	}

	nextBlock := func(height uint64) *innerBlock {
		return &innerBlock{
			ts:     startTime.Add(time.Duration(seq+1) * time.Second),
			height: height,
			bytes:  []byte{byte(seq + 1)},
		}
	}

	// ----- Step 0: Genesis block -----
	genesis := metadata.StateMachineBlock{
		InnerBlock: &innerBlock{
			ts:    startTime,
			bytes: []byte{0},
		},
	}
	bs[0] = &outerBlock{block: genesis}

	// ----- Step 1: Build zero epoch block (first simplex block) -----
	sm.BlockBuilder = &blockBuilder{block: nextBlock(1)}
	md := nextMD(&genesis)
	block1, err := sm.BuildBlock(context.Background(), genesis, md, nil)
	require.NoError(t, err)
	require.NotNil(t, block1)
	require.Equal(t, uint64(1), block1.Metadata.SimplexEpochInfo.EpochNumber)
	require.NotNil(t, block1.Metadata.SimplexEpochInfo.BlockValidationDescriptor)
	require.Equal(t, pChainHeight1, block1.Metadata.SimplexEpochInfo.PChainReferenceHeight)
	bs[seq] = &outerBlock{block: *block1}

	// Verify it
	smVerify := sm // Use the same SM for verification since it shares GetBlock.
	require.NoError(t, smVerify.VerifyBlock(context.Background(), block1))

	// ----- Step 2: Build a normal block (no validator set change) -----
	sm.BlockBuilder = &blockBuilder{block: nextBlock(2)}
	md = nextMD(block1)
	block2, err := sm.BuildBlock(context.Background(), *block1, md, nil)
	require.NoError(t, err)
	require.NotNil(t, block2)
	require.Equal(t, uint64(1), block2.Metadata.SimplexEpochInfo.EpochNumber)
	require.Equal(t, uint64(0), block2.Metadata.SimplexEpochInfo.NextPChainReferenceHeight)
	require.Nil(t, block2.Metadata.SimplexEpochInfo.BlockValidationDescriptor)
	bs[seq] = &outerBlock{block: *block2}

	require.NoError(t, smVerify.VerifyBlock(context.Background(), block2))

	// ----- Step 3: Build a normal block that detects a validator set change -----
	// Advance P-chain height so that GetValidatorSet returns a different set.
	currentPChainHeight = pChainHeight2

	sm.BlockBuilder = &blockBuilder{block: nextBlock(3)}
	md = nextMD(block2)
	block3, err := sm.BuildBlock(context.Background(), *block2, md, nil)
	require.NoError(t, err)
	require.NotNil(t, block3)
	require.Equal(t, uint64(1), block3.Metadata.SimplexEpochInfo.EpochNumber)
	require.Equal(t, pChainHeight2, block3.Metadata.SimplexEpochInfo.NextPChainReferenceHeight)
	require.Nil(t, block3.Metadata.SimplexEpochInfo.BlockValidationDescriptor)
	bs[seq] = &outerBlock{block: *block3}

	require.NoError(t, smVerify.VerifyBlock(context.Background(), block3))

	// ----- Step 4: First collecting block (1/3 approvals, not enough to seal) -----
	approvalsResult = metadata.ValidatorSetApprovals{
		{
			NodeID:       node1,
			PChainHeight: pChainHeight2,
			Signature:    []byte("sig1"),
		},
	}

	sm.BlockBuilder = &blockBuilder{block: nextBlock(4)}
	md = nextMD(block3)
	block4, err := sm.BuildBlock(context.Background(), *block3, md, nil)
	require.NoError(t, err)
	require.NotNil(t, block4)
	require.Equal(t, uint64(1), block4.Metadata.SimplexEpochInfo.EpochNumber)
	require.Equal(t, uint64(0), block4.Metadata.SimplexEpochInfo.SealingBlockSeq)
	require.Nil(t, block4.Metadata.SimplexEpochInfo.BlockValidationDescriptor)
	require.NotNil(t, block4.Metadata.SimplexEpochInfo.NextEpochApprovals)
	bs[seq] = &outerBlock{block: *block4}

	require.NoError(t, smVerify.VerifyBlock(context.Background(), block4))

	// ----- Step 5: Second collecting block (2/3 approvals, still not enough since threshold is strictly > 2/3) -----
	approvalsResult = metadata.ValidatorSetApprovals{
		{
			NodeID:       node2,
			PChainHeight: pChainHeight2,
			Signature:    []byte("sig2"),
		},
	}

	sm.BlockBuilder = &blockBuilder{block: nextBlock(5)}
	md = nextMD(block4)
	block5, err := sm.BuildBlock(context.Background(), *block4, md, nil)
	require.NoError(t, err)
	require.NotNil(t, block5)
	require.Equal(t, uint64(1), block5.Metadata.SimplexEpochInfo.EpochNumber)
	require.Equal(t, uint64(0), block5.Metadata.SimplexEpochInfo.SealingBlockSeq)
	require.Nil(t, block5.Metadata.SimplexEpochInfo.BlockValidationDescriptor)
	require.NotNil(t, block5.Metadata.SimplexEpochInfo.NextEpochApprovals)
	bs[seq] = &outerBlock{block: *block5}

	require.NoError(t, smVerify.VerifyBlock(context.Background(), block5))

	// ----- Step 6: Sealing block (3/3 approvals, enough to seal) -----
	approvalsResult = metadata.ValidatorSetApprovals{
		{
			NodeID:       node3,
			PChainHeight: pChainHeight2,
			Signature:    []byte("sig3"),
		},
	}

	sm.BlockBuilder = &blockBuilder{block: nextBlock(6)}
	md = nextMD(block5)
	block6, err := sm.BuildBlock(context.Background(), *block5, md, nil)
	require.NoError(t, err)
	require.NotNil(t, block6)
	require.Equal(t, uint64(1), block6.Metadata.SimplexEpochInfo.EpochNumber)
	require.NotEqual(t, uint64(0), block6.Metadata.SimplexEpochInfo.SealingBlockSeq)
	require.NotNil(t, block6.Metadata.SimplexEpochInfo.BlockValidationDescriptor)
	bs[seq] = &outerBlock{block: *block6}

	require.NoError(t, smVerify.VerifyBlock(context.Background(), block6))

	// ----- Step 7: Build a new epoch block (sealing block is finalized) -----
	sealingSeq := block6.Metadata.SimplexEpochInfo.SealingBlockSeq
	bs[sealingSeq] = &outerBlock{
		block:        bs[sealingSeq].block,
		finalization: &simplex.Finalization{},
	}

	sm.BlockBuilder = &blockBuilder{block: nextBlock(7)}
	md = nextMD(block6)
	block7, err := sm.BuildBlock(context.Background(), *block6, md, nil)
	require.NoError(t, err)
	require.NotNil(t, block7)
	require.Equal(t, sealingSeq, block7.Metadata.SimplexEpochInfo.EpochNumber)
	require.Equal(t, pChainHeight2, block7.Metadata.SimplexEpochInfo.PChainReferenceHeight)
	require.Equal(t, uint64(0), block7.Metadata.SimplexEpochInfo.NextPChainReferenceHeight)
	require.Nil(t, block7.Metadata.SimplexEpochInfo.BlockValidationDescriptor)
	bs[seq] = &outerBlock{block: *block7}

	require.NoError(t, smVerify.VerifyBlock(context.Background(), block7))
}

type dynamicApprovalsRetriever struct {
	approvals *metadata.ValidatorSetApprovals
}

func (d *dynamicApprovalsRetriever) RetrieveApprovals() metadata.ValidatorSetApprovals {
	return *d.approvals
}

func makeChain(t *testing.T, simplexStartHeight uint64, endHeight uint64) []metadata.StateMachineBlock {
	startTime := time.Now()
	blocks := make([]metadata.StateMachineBlock, 0, endHeight+1)
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

func makeNormalSimplexBlock(t *testing.T, index int, blocks []metadata.StateMachineBlock, start time.Time, h uint64, round uint64, seq uint64) metadata.StateMachineBlock {
	content := make([]byte, 10)
	_, err := rand.Read(content)
	require.NoError(t, err)

	prev := genesisBlock.Digest()
	if index > 0 {
		prev = blocks[index-1].Digest()
	}

	return metadata.StateMachineBlock{
		InnerBlock: &innerBlock{
			ts:     start.Add(time.Duration(h) * time.Second),
			height: h,
			bytes:  []byte{1, 2, 3},
		},
		Metadata: metadata.StateMachineMetadata{
			PChainHeight: 100,
			SimplexProtocolMetadata: (&simplex.ProtocolMetadata{
				Round: round,
				Seq:   seq,
				Epoch: 1,
				Prev:  prev,
			}).Bytes(),
			SimplexEpochInfo: metadata.SimplexEpochInfo{
				PrevSealingBlockHash:  [32]byte{},
				PChainReferenceHeight: 100,
				EpochNumber:           1,
				PrevVMBlockSeq:        uint64(index),
			},
		},
	}
}

func makeNonSimplexBlock(t *testing.T, startHeight uint64, start time.Time, h uint64) metadata.StateMachineBlock {
	content := make([]byte, 10)
	_, err := rand.Read(content)
	require.NoError(t, err)

	return metadata.StateMachineBlock{
		InnerBlock: &innerBlock{
			ts:     start.Add(time.Duration(h-startHeight) * time.Second),
			height: h,
			bytes:  []byte{1, 2, 3},
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

func newStateMachine(t *testing.T) (metadata.StateMachine, *testConfig) {
	bs := make(blockStore)

	var testConfig testConfig
	testConfig.blockStore = bs
	testConfig.validatorSetRetriever.result = metadata.NodeBLSMappings{
		{BLSKey: []byte{1}, Weight: 1}, {BLSKey: []byte{2}, Weight: 1},
	}

	sm := metadata.StateMachine{
		LatestPersistedHeight:    1,
		Logger:                   testutil.MakeLogger(t),
		GetBlock:                 bs.getBlock,
		MaxBlockBuildingWaitTime: time.Second,
		ApprovalsRetriever:       &testConfig.approvalsRetriever,
		SignatureVerifier:        &testConfig.signatureVerifier,
		SignatureAggregator:      &testConfig.signatureAggregator,
		BlockBuilder:             &testConfig.blockBuilder,
		KeyAggregator:            &testConfig.keyAggregator,
		ComputeICMEpoch: func(_ any, input metadata.ICMEpochInput) metadata.ICMEpoch {
			return input.ParentEpoch
		},
		GetPChainHeight: func() uint64 {
			return 100
		},
		GetUpgrades: func() any {
			return nil
		},
		GetValidatorSet: testConfig.validatorSetRetriever.getValidatorSet,
	}
	return sm, &testConfig
}

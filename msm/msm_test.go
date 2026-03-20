// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata_test

import (
	"context"
	"crypto/rand"
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

func (bs blockStore) clone() blockStore {
	newStore := make(blockStore)
	for k, v := range bs {
		newStore[k] = v
	}
	return newStore
}

func (bs blockStore) getBlock(opts metadata.RetrievingOpts) (metadata.StateMachineBlock, *simplex.Finalization, error) {
	blk, exits := bs[opts.Height]
	if !exits {
		return metadata.StateMachineBlock{}, nil, fmt.Errorf("block %d not found", opts.Height)
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

func (bb *blockBuilder) WaitForPendingBlock(ctx context.Context) {
	//TODO implement me
	panic("implement me")
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
		InnerBlock: &metadata.InnerBlock{
			TS:    time.Now(),
			Bytes:[]byte{1, 2, 3},
		},
	}
)

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
						InnerBlock: &metadata.InnerBlock{TS: time.Now(), Bytes:[]byte{1, 2, 3}},
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
		InnerBlock: &metadata.InnerBlock{
			TS:     time.Now(),
			BlockHeight: 42,
			Bytes: []byte{4, 5, 6},
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

	testConfig1.blockBuilder.block = &metadata.InnerBlock{
		TS:     time.Now(),
		BlockHeight: 43,
		Bytes: []byte{7, 8, 9},
	}

	block, err := sm1.BuildBlock(context.Background(), preSimplexParent, md, nil)
	require.NoError(t, err)
	require.NotNil(t, block)

	require.NoError(t, sm2.VerifyBlock(context.Background(), block))

	require.Equal(t, &metadata.StateMachineBlock{
		InnerBlock: &metadata.InnerBlock{
			TS:     testConfig1.blockBuilder.block.Timestamp(),
			BlockHeight: 43,
			Bytes: []byte{7, 8, 9},
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
		name                        string
		setup                       func(*metadata.StateMachine, *testConfig)
		mutateBlock                 func(*metadata.StateMachineBlock)
		err                         string
		expectedPChainHeight        uint64
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
					ChildTimestamp:     blockTime,
					ParentTimestamp:    time.Unix(int64(lastBlock.Metadata.Timestamp), 0),
					ParentEpoch:        metadata.ICMEpoch{},
				}, input)
				return input.ParentEpoch
			}

			content := make([]byte, 10)
			_, err = rand.Read(content)
			require.NoError(t, err)

			testConfig1.blockBuilder.block = &metadata.InnerBlock{
				TS:     blockTime,
				BlockHeight: lastBlock.InnerBlock.Height(),
				Bytes: content,
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
				InnerBlock: &metadata.InnerBlock{
					TS:     blockTime,
					BlockHeight: lastBlock.InnerBlock.Height(),
					Bytes: content,
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

	startTime := time.Now()

	nextBlock := func(height uint64) *metadata.InnerBlock {
		return &metadata.InnerBlock{
			TS:     startTime.Add(time.Duration(height) * time.Millisecond),
			BlockHeight: height,
			Bytes: []byte{byte(height)},
		}
	}

	// ----- Step 0: Building on top of genesis or upgrading to Simplex-----
	genesis := metadata.StateMachineBlock{
		InnerBlock: &metadata.InnerBlock{
			BlockHeight: 0, // Genesis block has height 0
			TS:     startTime,
			Bytes: []byte{0},
		},
	}

	notGenesis := metadata.StateMachineBlock{
		InnerBlock: &metadata.InnerBlock{
			BlockHeight: 42,
			TS:     startTime,
			Bytes: []byte{0},
		},
	}
	for _, testCase := range []struct {
		name                    string
		firstBlockBeforeSimplex metadata.StateMachineBlock
	}{
		{
			name:                    "building on top of genesis",
			firstBlockBeforeSimplex: genesis,
		},
		{
			name:                    "upgrading to Simplex from pre-Simplex blocks",
			firstBlockBeforeSimplex: notGenesis,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {

			currentPChainHeight := pChainHeight1

			getValidatorSet := func(height uint64) (metadata.NodeBLSMappings, error) {
				if height >= pChainHeight2 {
					return validatorSet2, nil
				}
				return validatorSet1, nil
			}
			getPChainHeight := func() uint64 {
				return currentPChainHeight
			}

			// Create fresh state machine instances for each iteration.
			sm, tc := newStateMachine(t)
			sm.GetValidatorSet = getValidatorSet
			sm.GetPChainHeight = getPChainHeight

			smVerify, tcVerify := newStateMachine(t)
			smVerify.GetValidatorSet = getValidatorSet
			smVerify.GetPChainHeight = getPChainHeight

			// addBlock adds a block to both block stores so builder and verifier stay in sync.
			addBlock := func(seq uint64, block metadata.StateMachineBlock, fin *simplex.Finalization) {
				tc.blockStore[seq] = &outerBlock{block: block, finalization: fin}
				tcVerify.blockStore[seq] = &outerBlock{block: block, finalization: fin}
			}

			baseSeq := testCase.firstBlockBeforeSimplex.InnerBlock.Height()
			addBlock(baseSeq, testCase.firstBlockBeforeSimplex, nil)

			aggr := &signatureAggregator{}

			// ----- Step 1: Build zero epoch block (first simplex block) -----
			tc.blockBuilder.block = nextBlock(1)
			md := simplex.ProtocolMetadata{
				Seq:   baseSeq + 1,
				Round: 0,
				Epoch: 1,
				Prev:  testCase.firstBlockBeforeSimplex.Digest(),
			}

			block1, err := sm.BuildBlock(context.Background(), testCase.firstBlockBeforeSimplex, md, nil)
			require.NoError(t, err)
			require.Equal(t, &metadata.StateMachineBlock{
				InnerBlock: nextBlock(1),
				Metadata: metadata.StateMachineMetadata{
					Timestamp:               uint64(startTime.Add(1 * time.Millisecond).Unix()),
					PChainHeight:            pChainHeight1,
					SimplexProtocolMetadata: md.Bytes(),
					SimplexEpochInfo: metadata.SimplexEpochInfo{
						PChainReferenceHeight: pChainHeight1,
						EpochNumber:           1,
						PrevVMBlockSeq:        baseSeq,
						BlockValidationDescriptor: &metadata.BlockValidationDescriptor{
							AggregatedMembership: metadata.AggregatedMembership{
								Members: validatorSet1,
							},
						},
					},
				},
			}, block1)
			addBlock(md.Seq, *block1, nil)

			require.NoError(t, smVerify.VerifyBlock(context.Background(), block1))

			// After we build the first block, the StateMachine should consider it as the latest persisted height.
			sm.LatestPersistedHeight = baseSeq + 1
			smVerify.LatestPersistedHeight = baseSeq + 1

			// ----- Step 2: Build a normal block (no validator set change) -----
			tc.blockBuilder.block = nextBlock(2)
			md = simplex.ProtocolMetadata{Seq: baseSeq + 2, Round: 1, Epoch: 1, Prev: block1.Digest()}
			block2, err := sm.BuildBlock(context.Background(), *block1, md, nil)
			require.NoError(t, err)
			require.Equal(t, &metadata.StateMachineBlock{
				InnerBlock: nextBlock(2),
				Metadata: metadata.StateMachineMetadata{
					Timestamp:               uint64(startTime.Add(2 * time.Millisecond).Unix()),
					PChainHeight:            pChainHeight1,
					SimplexProtocolMetadata: md.Bytes(),
					SimplexEpochInfo: metadata.SimplexEpochInfo{
						PChainReferenceHeight: pChainHeight1,
						EpochNumber:           1,
						PrevVMBlockSeq:        baseSeq + 1,
					},
				},
			}, block2)
			addBlock(md.Seq, *block2, nil)

			require.NoError(t, smVerify.VerifyBlock(context.Background(), block2))

			// ----- Step 3: Build a normal block that detects a validator set change -----
			// Advance P-chain height so that GetValidatorSet returns a different set.
			currentPChainHeight = pChainHeight2

			tc.blockBuilder.block = nextBlock(3)
			md = simplex.ProtocolMetadata{Seq: baseSeq + 3, Round: 2, Epoch: 1, Prev: block2.Digest()}
			block3, err := sm.BuildBlock(context.Background(), *block2, md, nil)
			require.NoError(t, err)
			require.Equal(t, &metadata.StateMachineBlock{
				InnerBlock: nextBlock(3),
				Metadata: metadata.StateMachineMetadata{
					Timestamp:               uint64(startTime.Add(3 * time.Millisecond).Unix()),
					PChainHeight:            pChainHeight2,
					SimplexProtocolMetadata: md.Bytes(),
					SimplexEpochInfo: metadata.SimplexEpochInfo{
						PChainReferenceHeight:     pChainHeight1,
						EpochNumber:               1,
						PrevVMBlockSeq:            baseSeq + 2,
						NextPChainReferenceHeight: pChainHeight2,
					},
				},
			}, block3)
			addBlock(md.Seq, *block3, nil)

			require.NoError(t, smVerify.VerifyBlock(context.Background(), block3))

			// ----- Step 4: First collecting block (1/3 approvals, not enough to seal) -----

			// Override ApprovalsRetriever to use our dynamic approvals.
			var approvalsResult metadata.ValidatorSetApprovals
			sm.ApprovalsRetriever = &dynamicApprovalsRetriever{approvals: &approvalsResult}

			approvalsResult = metadata.ValidatorSetApprovals{
				{
					NodeID:       node1,
					PChainHeight: pChainHeight2,
					Signature:    []byte("sig1"),
				},
			}

			// node1 is at index 0 in validatorSet2 → bitmask bit 0 → {1}
			bitmask := []byte{1}
			sig, err := aggr.AggregateSignatures([]byte("sig1"), nil)
			require.NoError(t, err)

			tc.blockBuilder.block = nextBlock(4)
			md = simplex.ProtocolMetadata{Seq: baseSeq + 4, Round: 3, Epoch: 1, Prev: block3.Digest()}
			block4, err := sm.BuildBlock(context.Background(), *block3, md, nil)
			require.NoError(t, err)
			require.Equal(t, &metadata.StateMachineBlock{
				InnerBlock: nextBlock(4),
				Metadata: metadata.StateMachineMetadata{
					Timestamp:               uint64(startTime.Add(4 * time.Millisecond).Unix()),
					PChainHeight:            pChainHeight2,
					SimplexProtocolMetadata: md.Bytes(),
					SimplexEpochInfo: metadata.SimplexEpochInfo{
						PChainReferenceHeight:     pChainHeight1,
						EpochNumber:               1,
						PrevVMBlockSeq:            baseSeq + 3,
						NextPChainReferenceHeight: pChainHeight2,
						NextEpochApprovals: &metadata.NextEpochApprovals{
							NodeIDs:   bitmask,
							Signature: sig,
						},
					},
				},
			}, block4)
			addBlock(md.Seq, *block4, nil)

			require.NoError(t, smVerify.VerifyBlock(context.Background(), block4))

			// ----- Step 5: Second collecting block (2/3 approvals, still not enough since threshold is strictly > 2/3) -----
			approvalsResult = metadata.ValidatorSetApprovals{
				{
					NodeID:       node2,
					PChainHeight: pChainHeight2,
					Signature:    []byte("sig2"),
				},
			}

			// node2 is at index 1 → bitmask bits 0,1 → {3}
			sig, err = aggr.AggregateSignatures([]byte("sig2"), sig)
			require.NoError(t, err)
			bitmask = []byte{3}

			tc.blockBuilder.block = nextBlock(5)
			md = simplex.ProtocolMetadata{Seq: baseSeq + 5, Round: 4, Epoch: 1, Prev: block4.Digest()}
			block5, err := sm.BuildBlock(context.Background(), *block4, md, nil)
			require.NoError(t, err)
			require.Equal(t, &metadata.StateMachineBlock{
				InnerBlock: nextBlock(5),
				Metadata: metadata.StateMachineMetadata{
					Timestamp:               uint64(startTime.Add(5 * time.Millisecond).Unix()),
					PChainHeight:            pChainHeight2,
					SimplexProtocolMetadata: md.Bytes(),
					SimplexEpochInfo: metadata.SimplexEpochInfo{
						PChainReferenceHeight:     pChainHeight1,
						EpochNumber:               1,
						PrevVMBlockSeq:            baseSeq + 4,
						NextPChainReferenceHeight: pChainHeight2,
						NextEpochApprovals: &metadata.NextEpochApprovals{
							NodeIDs:   bitmask,
							Signature: sig,
						},
					},
				},
			}, block5)
			addBlock(md.Seq, *block5, nil)

			require.NoError(t, smVerify.VerifyBlock(context.Background(), block5))

			// ----- Step 6: Sealing block (3/3 approvals, enough to seal) -----
			approvalsResult = metadata.ValidatorSetApprovals{
				{
					NodeID:       node3,
					PChainHeight: pChainHeight2,
					Signature:    []byte("sig3"),
				},
			}

			// node3 is at index 2 → bitmask bits 0,1,2 → {7}
			sig6, err := aggr.AggregateSignatures([]byte("sig3"), sig)
			require.NoError(t, err)
			bitmask = []byte{7}

			tc.blockBuilder.block = nextBlock(6)
			md = simplex.ProtocolMetadata{Seq: baseSeq + 6, Round: 5, Epoch: 1, Prev: block5.Digest()}
			block6, err := sm.BuildBlock(context.Background(), *block5, md, nil)
			require.NoError(t, err)
			require.Equal(t, &metadata.StateMachineBlock{
				InnerBlock: nextBlock(6),
				Metadata: metadata.StateMachineMetadata{
					Timestamp:               uint64(startTime.Add(6 * time.Millisecond).Unix()),
					PChainHeight:            pChainHeight2,
					SimplexProtocolMetadata: md.Bytes(),
					SimplexEpochInfo: metadata.SimplexEpochInfo{
						PChainReferenceHeight:     pChainHeight1,
						EpochNumber:               1,
						PrevVMBlockSeq:            baseSeq + 5,
						NextPChainReferenceHeight: pChainHeight2,
						SealingBlockSeq:           baseSeq + 6,
						PrevSealingBlockHash:      block1.Digest(),
						BlockValidationDescriptor: &metadata.BlockValidationDescriptor{
							AggregatedMembership: metadata.AggregatedMembership{
								Members: validatorSet2,
							},
						},
						NextEpochApprovals: &metadata.NextEpochApprovals{
							NodeIDs:   bitmask,
							Signature: sig6,
						},
					},
				},
			}, block6)
			addBlock(md.Seq, *block6, nil)

			require.NoError(t, smVerify.VerifyBlock(context.Background(), block6))

			sealingSeq := block6.Metadata.SimplexEpochInfo.SealingBlockSeq

			backupStoreTC := tc.blockStore.clone()
			backupStoreTCVerify := tcVerify.blockStore.clone()

			for _, subTestCase := range []struct{
				name string
				setup func()
			}{
				{
					name: "sealing block not finalized yet",
					setup: func() {
						addBlock(sealingSeq, tc.blockStore[sealingSeq].block, nil)
					},
				},
				{
					name: "sealing block immediately finalized",
					setup: func() {
						addBlock(sealingSeq, tc.blockStore[sealingSeq].block, &simplex.Finalization{})
					},
				},
			}{
				testName := fmt.Sprintf("%s-%s", testCase.name, subTestCase.name)
				t.Run(testName, func(t *testing.T) {
					tc.blockStore = backupStoreTC.clone()
					sm.GetBlock = tc.blockStore.getBlock
					tcVerify.blockStore = backupStoreTCVerify.clone()
					smVerify.GetBlock = tcVerify.blockStore.getBlock

					subTestCase.setup()

					tc.blockBuilder.block = nextBlock(7)
					md = simplex.ProtocolMetadata{Seq: baseSeq + 7, Round: 6, Epoch: 1, Prev: block6.Digest()}

					// If the sealing block isn't finalized yet, we expect to build a Telock.
					// However, despite the fact that the block builder is willing to build a new block,
					// a Telock shouldn't contain an inner block.
					if tc.blockStore[sealingSeq].finalization == nil {
						telock, err := sm.BuildBlock(context.Background(), *block6, md, nil)
						require.NoError(t, err)

						require.Equal(t, &metadata.StateMachineBlock{
							InnerBlock: nil,
							Metadata: metadata.StateMachineMetadata{
								Timestamp:               uint64(startTime.Add(6 * time.Millisecond).Unix()),
								PChainHeight:            pChainHeight2,
								SimplexProtocolMetadata: md.Bytes(),
								SimplexEpochInfo: metadata.SimplexEpochInfo{
									PChainReferenceHeight:     pChainHeight1,
									EpochNumber:               1,
									NextPChainReferenceHeight: pChainHeight2,
									PrevVMBlockSeq:            baseSeq + 6,
									SealingBlockSeq:           sealingSeq,
								},
							},
						}, telock)

						// Next, finalize the sealing block after we have built a Telock.
						addBlock(sealingSeq, tc.blockStore[sealingSeq].block, &simplex.Finalization{})
					}

					// ----- Step 7: Build a new epoch block (sealing block is finalized) -----


					block7, err := sm.BuildBlock(context.Background(), *block6, md, nil)
					require.NoError(t, err)
					require.Equal(t, &metadata.StateMachineBlock{
						InnerBlock: nextBlock(7),
						Metadata: metadata.StateMachineMetadata{
							Timestamp:               uint64(startTime.Add(7 * time.Millisecond).Unix()),
							PChainHeight:            pChainHeight2,
							SimplexProtocolMetadata: md.Bytes(),
							SimplexEpochInfo: metadata.SimplexEpochInfo{
								PChainReferenceHeight: pChainHeight2,
								EpochNumber:           sealingSeq,
								PrevVMBlockSeq:        baseSeq + 6,
							},
						},
					}, block7)
					addBlock(md.Seq, *block7, nil)

					require.NoError(t, smVerify.VerifyBlock(context.Background(), block7))
				})
			}
		})
	}
}

type dynamicApprovalsRetriever struct {
	approvals *metadata.ValidatorSetApprovals
}

func (d *dynamicApprovalsRetriever) RetrieveApprovals() metadata.ValidatorSetApprovals {
	return *d.approvals
}

func makeChain(t *testing.T, simplexStartHeight uint64, endHeight uint64) []metadata.StateMachineBlock {
	startTime := time.Now().Add(-time.Duration(endHeight+2) * time.Second)
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
		InnerBlock: &metadata.InnerBlock{
			TS:     start.Add(time.Duration(h) * time.Second),
			BlockHeight: h,
			Bytes: []byte{1, 2, 3},
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
		InnerBlock: &metadata.InnerBlock{
			TS:     start.Add(time.Duration(h-startHeight) * time.Second),
			BlockHeight: h,
			Bytes: []byte{1, 2, 3},
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
		GetTime: time.Now,
		TimeSkewLimit: time.Second * 5,
		Logger:                   testutil.MakeLogger(t),
		GetBlock:                 testConfig.blockStore.getBlock,
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

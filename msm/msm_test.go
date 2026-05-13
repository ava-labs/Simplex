// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

func TestMSMBuildAndVerifyBlocksAfterGenesis(t *testing.T) {
	validMD := simplex.ProtocolMetadata{
		Round: 1,
		Seq:   1,
		Epoch: 1,
		Prev:  genesisBlock.Digest(),
	}

	for _, testCase := range []struct {
		name        string
		md          simplex.ProtocolMetadata
		err         string
		configure   func(*StateMachine, *testConfig)
		mutateBlock func(*StateMachineBlock)
	}{
		{
			name: "correct information",
			md:   validMD,
		},
		{
			name: "verifying a genesis block",
			md:   validMD,
			mutateBlock: func(block *StateMachineBlock) {
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
			configure: func(_ *StateMachine, tc *testConfig) {
				delete(tc.blockStore, 0)
			},
			err: "failed to retrieve previous (0) inner block",
		},
		{
			name: "parent has no inner block",
			md:   validMD,
			configure: func(_ *StateMachine, tc *testConfig) {
				tc.blockStore[0] = &outerBlock{
					block: StateMachineBlock{},
				}
			},
			err: "parent inner block (",
		},
		{
			name: "wrong epoch number",
			md:   validMD,
			mutateBlock: func(block *StateMachineBlock) {
				block.Metadata.SimplexEpochInfo.EpochNumber = 2
			},
			err: "invalid epoch number (2), should be 1",
		},
		{
			name: "P-chain height too big",
			md:   validMD,
			mutateBlock: func(block *StateMachineBlock) {
				block.Metadata.PChainHeight = 110
			},
			err: "invalid P-chain height (110), expected to be 100",
		},
		{
			name: "P-chain height smaller than parent",
			md:   validMD,
			configure: func(sm *StateMachine, tc *testConfig) {
				sm.LastNonSimplexBlockPChainHeight = 99
			},
			err: "invalid P-chain height (100), expected to be 99",
		},
		{
			name: "nil BlockValidationDescriptor",
			md:   validMD,
			mutateBlock: func(block *StateMachineBlock) {
				block.Metadata.SimplexEpochInfo.BlockValidationDescriptor = nil
			},
			err: "invalid BlockValidationDescriptor: should not be nil",
		},
		{
			name: "membership mismatch",
			md:   validMD,
			configure: func(sm *StateMachine, tc *testConfig) {
				sm.GenesisValidatorSet = NodeBLSMappings{
					{BLSKey: []byte{1}, Weight: 1},
				}
			},
			err: "invalid BlockValidationDescriptor: should match validator set",
		},
		{
			name: "SimplexEpochInfo mismatch",
			md:   validMD,
			mutateBlock: func(block *StateMachineBlock) {
				block.Metadata.SimplexEpochInfo.PrevVMBlockSeq = 999
			},
			err: "invalid SimplexEpochInfo",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			sm1, _ := newStateMachine(t)
			sm2, testConfig2 := newStateMachine(t)

			if testCase.configure != nil {
				testCase.configure(sm2, testConfig2)
			}

			block, err := sm1.BuildBlock(context.Background(), testCase.md, nil)
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
	preSimplexParent := StateMachineBlock{
		InnerBlock: &InnerBlock{
			TS:          time.Now(),
			BlockHeight: 42,
			Bytes:       []byte{4, 5, 6},
		},
		// Zero-valued metadata means this is a pre-Simplex block or a genesis block.
		// But since the height is 42, it can't be a genesis block, so it must be a pre-Simplex block.
		Metadata: StateMachineMetadata{},
	}

	md := simplex.ProtocolMetadata{
		Round: 0,
		Seq:   43,
		Epoch: 1,
		Prev:  preSimplexParent.Digest(),
	}

	sm1, testConfig1 := newStateMachine(t)
	sm2, testConfig2 := newStateMachine(t)

	testConfig1.blockStore[0] = &outerBlock{
		block: preSimplexParent,
	}

	testConfig1.blockStore[42] = &outerBlock{block: preSimplexParent}
	testConfig2.blockStore[42] = &outerBlock{block: preSimplexParent}

	sm1.LastNonSimplexInnerBlock = testConfig1.blockStore[42].block.InnerBlock
	sm2.LastNonSimplexInnerBlock = testConfig1.blockStore[42].block.InnerBlock

	testConfig1.blockBuilder.block = &InnerBlock{
		TS:          time.Now(),
		BlockHeight: 43,
		Bytes:       []byte{7, 8, 9},
	}

	block, err := sm1.BuildBlock(context.Background(), md, nil)
	require.NoError(t, err)
	require.NotNil(t, block)

	require.NoError(t, sm2.VerifyBlock(context.Background(), block))

	require.Equal(t, &StateMachineBlock{
		Metadata: StateMachineMetadata{
			Timestamp:               uint64(preSimplexParent.InnerBlock.Timestamp().UnixMilli()),
			PChainHeight:            100,
			SimplexProtocolMetadata: md.Bytes(),
			SimplexEpochInfo: SimplexEpochInfo{
				PChainReferenceHeight: 100,
				EpochNumber:           1,
				PrevVMBlockSeq:        42,
				BlockValidationDescriptor: &BlockValidationDescriptor{
					AggregatedMembership: AggregatedMembership{
						Members: testConfig1.validatorSetRetriever.result,
					},
				},
			},
		},
	}, block)
}

func TestMSMNormalOp(t *testing.T) {
	newPChainHeight := uint64(200)
	newValidatorSet := NodeBLSMappings{
		{BLSKey: []byte{5}, Weight: 1}, {BLSKey: []byte{6}, Weight: 1}, {BLSKey: []byte{7}, Weight: 1},
	}

	for _, testCase := range []struct {
		name                        string
		setup                       func(*StateMachine, *testConfig)
		expectedPChainHeight        uint64
		expectedNextPChainRefHeight uint64
	}{
		{
			name:                 "correct information",
			expectedPChainHeight: 100,
		},
		{
			name: "validator set change detected",
			setup: func(sm *StateMachine, tc *testConfig) {
				tc.validatorSetRetriever.resultMap = map[uint64]NodeBLSMappings{
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

			for i, block := range chain {
				testConfig1.blockStore[uint64(i)] = &outerBlock{block: block}
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

			content := make([]byte, 10)
			_, err = rand.Read(content)
			require.NoError(t, err)

			testConfig1.blockBuilder.block = &InnerBlock{
				TS:          blockTime,
				BlockHeight: lastBlock.InnerBlock.Height(),
				Bytes:       content,
			}

			if testCase.setup != nil {
				testCase.setup(sm1, testConfig1)
			}

			block1, err := sm1.BuildBlock(context.Background(), *md, &blacklist)
			require.NoError(t, err)
			require.NotNil(t, block1)

			require.Equal(t, &StateMachineBlock{
				InnerBlock: &InnerBlock{
					TS:          blockTime,
					BlockHeight: lastBlock.InnerBlock.Height(),
					Bytes:       content,
				},
				Metadata: StateMachineMetadata{
					SimplexBlacklist:        blacklist.Bytes(),
					Timestamp:               uint64(blockTime.UnixMilli()),
					PChainHeight:            testCase.expectedPChainHeight,
					SimplexProtocolMetadata: md.Bytes(),
					SimplexEpochInfo: SimplexEpochInfo{
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

	validatorSet1 := NodeBLSMappings{
		{NodeID: node1, BLSKey: []byte{1}, Weight: 1},
		{NodeID: node2, BLSKey: []byte{2}, Weight: 1},
		{NodeID: node3, BLSKey: []byte{3}, Weight: 1},
	}
	validatorSet2 := NodeBLSMappings{
		{NodeID: node1, BLSKey: []byte{1}, Weight: 1},
		{NodeID: node2, BLSKey: []byte{4}, Weight: 1},
		{NodeID: node3, BLSKey: []byte{5}, Weight: 1},
	}

	pChainHeight1 := uint64(100)
	pChainHeight2 := uint64(200)

	startTime := time.Now()

	nextBlock := func(height uint64) *InnerBlock {
		return &InnerBlock{
			TS:          startTime.Add(time.Duration(height) * time.Millisecond),
			BlockHeight: height,
			Bytes:       []byte{byte(height)},
		}
	}

	// ----- Step 0: Building on top of genesis or upgrading to Simplex-----
	genesis := StateMachineBlock{
		InnerBlock: &InnerBlock{
			BlockHeight: 0, // Genesis block has height 0
			TS:          startTime,
			Bytes:       []byte{0},
		},
	}

	notGenesis := StateMachineBlock{
		InnerBlock: &InnerBlock{
			BlockHeight: 42,
			TS:          startTime,
			Bytes:       []byte{0},
		},
	}
	for _, testCase := range []struct {
		name                    string
		firstBlockBeforeSimplex StateMachineBlock
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

			getValidatorSet := func(height uint64) (NodeBLSMappings, error) {
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
			tc.blockStore[0] = &outerBlock{block: genesis}
			tc.blockStore[42] = &outerBlock{block: notGenesis}

			sm.LastNonSimplexInnerBlock = testCase.firstBlockBeforeSimplex.InnerBlock
			sm.GenesisValidatorSet = validatorSet1
			sm.LastNonSimplexBlockPChainHeight = pChainHeight1

			smVerify, tcVerify := newStateMachine(t)
			smVerify.GetValidatorSet = getValidatorSet
			smVerify.GetPChainHeight = getPChainHeight

			smVerify.LastNonSimplexInnerBlock = testCase.firstBlockBeforeSimplex.InnerBlock
			smVerify.GenesisValidatorSet = validatorSet1
			smVerify.LastNonSimplexBlockPChainHeight = pChainHeight1

			// addBlock adds a block to both block stores so builder and verifier stay in sync.
			addBlock := func(seq uint64, block StateMachineBlock, fin *simplex.Finalization) {
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

			block1, err := sm.BuildBlock(context.Background(), md, nil)
			require.NoError(t, err)
			require.Equal(t, &StateMachineBlock{
				Metadata: StateMachineMetadata{
					Timestamp:               uint64(startTime.UnixMilli()),
					PChainHeight:            pChainHeight1,
					SimplexProtocolMetadata: md.Bytes(),
					SimplexEpochInfo: SimplexEpochInfo{
						PChainReferenceHeight: pChainHeight1,
						EpochNumber:           1,
						PrevVMBlockSeq:        baseSeq,
						BlockValidationDescriptor: &BlockValidationDescriptor{
							AggregatedMembership: AggregatedMembership{
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
			block2, err := sm.BuildBlock(context.Background(), md, nil)
			require.NoError(t, err)
			require.Equal(t, &StateMachineBlock{
				InnerBlock: nextBlock(2),
				Metadata: StateMachineMetadata{
					Timestamp:               uint64(startTime.Add(2 * time.Millisecond).UnixMilli()),
					PChainHeight:            pChainHeight1,
					SimplexProtocolMetadata: md.Bytes(),
					SimplexEpochInfo: SimplexEpochInfo{
						PChainReferenceHeight: pChainHeight1,
						EpochNumber:           1,
						PrevVMBlockSeq:        baseSeq,
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
			block3, err := sm.BuildBlock(context.Background(), md, nil)
			require.NoError(t, err)
			require.Equal(t, &StateMachineBlock{
				InnerBlock: nextBlock(3),
				Metadata: StateMachineMetadata{
					Timestamp:               uint64(startTime.Add(3 * time.Millisecond).UnixMilli()),
					PChainHeight:            pChainHeight2,
					SimplexProtocolMetadata: md.Bytes(),
					SimplexEpochInfo: SimplexEpochInfo{
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
			var approvalsResult ValidatorSetApprovals
			sm.ApprovalsRetriever = &dynamicApprovalsRetriever{approvals: &approvalsResult}

			approvalsResult = ValidatorSetApprovals{
				{
					NodeID:       node1,
					PChainHeight: pChainHeight2,
					Signature:    []byte("sig1"),
				},
			}

			// node1 is at index 0 in validatorSet2 → bitmask bit 0 → {1}
			bitmask := []byte{1}
			sig, err := aggr.AppendSignatures(nil, []byte("sig1"))
			require.NoError(t, err)

			tc.blockBuilder.block = nextBlock(4)
			md = simplex.ProtocolMetadata{Seq: baseSeq + 4, Round: 3, Epoch: 1, Prev: block3.Digest()}
			block4, err := sm.BuildBlock(context.Background(), md, nil)
			require.NoError(t, err)
			require.Equal(t, &StateMachineBlock{
				InnerBlock: nextBlock(4),
				Metadata: StateMachineMetadata{
					Timestamp:               uint64(startTime.Add(4 * time.Millisecond).UnixMilli()),
					PChainHeight:            pChainHeight2,
					SimplexProtocolMetadata: md.Bytes(),
					SimplexEpochInfo: SimplexEpochInfo{
						PChainReferenceHeight:     pChainHeight1,
						EpochNumber:               1,
						PrevVMBlockSeq:            baseSeq + 3,
						NextPChainReferenceHeight: pChainHeight2,
						NextEpochApprovals: &NextEpochApprovals{
							NodeIDs:   bitmask,
							Signature: sig,
						},
					},
				},
			}, block4)
			addBlock(md.Seq, *block4, nil)

			require.NoError(t, smVerify.VerifyBlock(context.Background(), block4))

			// ----- Step 5: Second collecting block (2/3 approvals, still not enough since threshold is strictly > 2/3) -----
			approvalsResult = ValidatorSetApprovals{
				{
					NodeID:       node2,
					PChainHeight: pChainHeight2,
					Signature:    []byte("sig2"),
				},
			}

			// node2 is at index 1 → bitmask bits 0,1 → {3}
			sig, err = aggr.AppendSignatures(sig, []byte("sig2"))
			require.NoError(t, err)
			bitmask = []byte{3}

			tc.blockBuilder.block = nextBlock(5)
			md = simplex.ProtocolMetadata{Seq: baseSeq + 5, Round: 4, Epoch: 1, Prev: block4.Digest()}
			block5, err := sm.BuildBlock(context.Background(), md, nil)
			require.NoError(t, err)
			require.Equal(t, &StateMachineBlock{
				InnerBlock: nextBlock(5),
				Metadata: StateMachineMetadata{
					Timestamp:               uint64(startTime.Add(5 * time.Millisecond).UnixMilli()),
					PChainHeight:            pChainHeight2,
					SimplexProtocolMetadata: md.Bytes(),
					SimplexEpochInfo: SimplexEpochInfo{
						PChainReferenceHeight:     pChainHeight1,
						EpochNumber:               1,
						PrevVMBlockSeq:            baseSeq + 4,
						NextPChainReferenceHeight: pChainHeight2,
						NextEpochApprovals: &NextEpochApprovals{
							NodeIDs:   bitmask,
							Signature: sig,
						},
					},
				},
			}, block5)
			addBlock(md.Seq, *block5, nil)

			require.NoError(t, smVerify.VerifyBlock(context.Background(), block5))

			// ----- Step 6: Sealing block (3/3 approvals, enough to seal) -----
			approvalsResult = ValidatorSetApprovals{
				{
					NodeID:       node3,
					PChainHeight: pChainHeight2,
					Signature:    []byte("sig3"),
				},
			}

			// node3 is at index 2 → bitmask bits 0,1,2 → {7}
			sig6, err := aggr.AppendSignatures(sig, []byte("sig3"))
			require.NoError(t, err)
			bitmask = []byte{7}

			tc.blockBuilder.block = nextBlock(6)
			md = simplex.ProtocolMetadata{Seq: baseSeq + 6, Round: 5, Epoch: 1, Prev: block5.Digest()}
			block6, err := sm.BuildBlock(context.Background(), md, nil)
			require.NoError(t, err)
			require.Equal(t, &StateMachineBlock{
				InnerBlock: nextBlock(6),
				Metadata: StateMachineMetadata{
					Timestamp:               uint64(startTime.Add(6 * time.Millisecond).UnixMilli()),
					PChainHeight:            pChainHeight2,
					SimplexProtocolMetadata: md.Bytes(),
					SimplexEpochInfo: SimplexEpochInfo{
						PChainReferenceHeight:     pChainHeight1,
						EpochNumber:               1,
						PrevVMBlockSeq:            baseSeq + 5,
						NextPChainReferenceHeight: pChainHeight2,
						SealingBlockSeq:           0,
						PrevSealingBlockHash:      block1.Digest(),
						BlockValidationDescriptor: &BlockValidationDescriptor{
							AggregatedMembership: AggregatedMembership{
								Members: validatorSet2,
							},
						},
						NextEpochApprovals: &NextEpochApprovals{
							NodeIDs:   bitmask,
							Signature: sig6,
						},
					},
				},
			}, block6)
			addBlock(md.Seq, *block6, nil)

			require.NoError(t, smVerify.VerifyBlock(context.Background(), block6))

			sealingSeq := baseSeq + 6 // The sealing block's sequence (md.Seq from step 6)

			backupStoreTC := tc.blockStore.clone()
			backupStoreTCVerify := tcVerify.blockStore.clone()

			for _, subTestCase := range []struct {
				name  string
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
			} {
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
						telock, err := sm.BuildBlock(context.Background(), md, nil)
						require.NoError(t, err)

						require.Equal(t, &StateMachineBlock{
							InnerBlock: nil,
							Metadata: StateMachineMetadata{
								Timestamp:               uint64(startTime.Add(6 * time.Millisecond).UnixMilli()),
								PChainHeight:            pChainHeight2,
								SimplexProtocolMetadata: md.Bytes(),
								SimplexEpochInfo: SimplexEpochInfo{
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

					block7, err := sm.BuildBlock(context.Background(), md, nil)
					require.NoError(t, err)
					require.Equal(t, &StateMachineBlock{
						InnerBlock: nextBlock(7),
						Metadata: StateMachineMetadata{
							Timestamp:               uint64(startTime.Add(7 * time.Millisecond).UnixMilli()),
							PChainHeight:            pChainHeight2,
							SimplexProtocolMetadata: md.Bytes(),
							SimplexEpochInfo: SimplexEpochInfo{
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
			result := tc.input.NextState()
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestAreNextEpochApprovalsSignersSupersetOfApprovalsOfPrevBlock(t *testing.T) {
	for _, tc := range []struct {
		name string
		prev SimplexEpochInfo
		next SimplexEpochInfo
		err  error
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
			err:  errSignerSetShrinked,
		},
		{
			name: "prev has approvals but next has nil approvals",
			prev: SimplexEpochInfo{NextEpochApprovals: &NextEpochApprovals{NodeIDs: []byte{1}}},
			next: SimplexEpochInfo{},
			err:  errNextEpochApprovalsShrinked,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ensureNextEpochApprovalsSignersSupersetOfApprovalsOfPrevBlock(tc.prev, tc.next)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
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

	logger := testutil.MakeLogger(t)

	t.Run("filters by p-chain height", func(t *testing.T) {
		approvals := ValidatorSetApprovals{
			{NodeID: node0, PChainHeight: 100},
			{NodeID: node1, PChainHeight: 200},
		}
		oldApproving := bitmaskFromBytes(nil)
		result := sanitizeApprovals(approvals, 100, nodeID2Index, oldApproving, logger)
		require.Len(t, result, 1)
		require.Equal(t, node0, result[0].NodeID)
	})

	t.Run("filters out already approved", func(t *testing.T) {
		approvals := ValidatorSetApprovals{
			{NodeID: node0, PChainHeight: 100},
			{NodeID: node1, PChainHeight: 100},
		}
		oldApproving := bitmaskFromBytes([]byte{1})
		result := sanitizeApprovals(approvals, 100, nodeID2Index, oldApproving, logger)
		require.Len(t, result, 1)
		require.Equal(t, node1, result[0].NodeID)
	})

	t.Run("filters out nodes not in validator set", func(t *testing.T) {
		approvals := ValidatorSetApprovals{
			{NodeID: node3, PChainHeight: 100},
			{NodeID: node2, PChainHeight: 100},
		}
		oldApproving := bitmaskFromBytes(nil)
		result := sanitizeApprovals(approvals, 100, nodeID2Index, oldApproving, logger)
		require.Len(t, result, 1)
		require.Equal(t, node2, result[0].NodeID)
	})

	t.Run("deduplicates by node ID", func(t *testing.T) {
		approvals := ValidatorSetApprovals{
			{NodeID: node0, PChainHeight: 100},
			{NodeID: node0, PChainHeight: 100},
		}
		oldApproving := bitmaskFromBytes(nil)
		result := sanitizeApprovals(approvals, 100, nodeID2Index, oldApproving, logger)
		require.Len(t, result, 1)
	})
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

	logger := testutil.MakeLogger(t)

	t.Run("duplicate peer with already-approved node does not double-aggregate", func(t *testing.T) {
		// node0 is already in the previous approvals (bit 0 set). A duplicate peer
		// entry for node0 must not append node0's signature to the new aggregate
		// (the prior aggregate already covers it via prevApprovals.Signature).
		prevApprovals := &NextEpochApprovals{
			NodeIDs:   []byte{1}, // bit 0
			Signature: []byte("existing"),
		}
		oldApproving := bitmaskFromBytes([]byte{1})

		peers := ValidatorSetApprovals{
			{NodeID: node0, Signature: []byte("sig0")},
			{NodeID: node0, Signature: []byte("sig0")},
		}

		aggSig, newApproving, err := computeNewApproverSignaturesAndSigners(prevApprovals, peers, oldApproving, nodeID2Index, concatAggregator{}, logger)
		require.NoError(t, err)
		require.True(t, newApproving.Contains(0))
		require.Equal(t, 1, newApproving.Len())
		// Only the existing aggregate should remain; node0's sig is already covered by it.
		require.Equal(t, []byte("existing"), aggSig)
	})

	t.Run("nil approvals", func(t *testing.T) {
		oldApproving := bitmaskFromBytes(nil)

		peers := ValidatorSetApprovals{
			{NodeID: node0, Signature: []byte("sig0")},
			{NodeID: node1, Signature: []byte("sig1")},
		}

		_, _, err := computeNewApproverSignaturesAndSigners(nil, peers, oldApproving, nodeID2Index, concatAggregator{}, logger)
		require.Error(t, err)
	})

	t.Run("new approvals with no previous", func(t *testing.T) {
		prevApprovals := &NextEpochApprovals{}
		oldApproving := bitmaskFromBytes(nil)

		peers := ValidatorSetApprovals{
			{NodeID: node0, Signature: []byte("sig0")},
			{NodeID: node1, Signature: []byte("sig1")},
		}

		aggSig, newApproving, err := computeNewApproverSignaturesAndSigners(prevApprovals, peers, oldApproving, nodeID2Index, concatAggregator{}, logger)
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

		aggSig, newApproving, err := computeNewApproverSignaturesAndSigners(prevApprovals, peers, oldApproving, nodeID2Index, concatAggregator{}, logger)
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

		aggSig, newApproving, err := computeNewApproverSignaturesAndSigners(prevApprovals, nil, oldApproving, nodeID2Index, concatAggregator{}, logger)
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

		aggSig, newApproving, err := computeNewApproverSignaturesAndSigners(prevApprovals, peers, oldApproving, nodeID2Index, concatAggregator{}, logger)
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

		_, _, err := computeNewApproverSignaturesAndSigners(prevApprovals, peers, oldApproving, nodeID2Index, failingAggregator{}, logger)
		require.ErrorContains(t, err, "aggregation failed")
	})
}

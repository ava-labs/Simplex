// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"context"
	"errors"
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

var testNodes = []simplex.NodeID{{1}, {2}, {3}, {4}}

type errQC struct{}

func (errQC) Signers() []simplex.NodeID { return nil }
func (errQC) Verify([]byte) error       { return errors.New("verification failed") }
func (errQC) Bytes() []byte             { return nil }

func newStorageWithGenesis(t *testing.T) *testutil.InMemStorage {
	storage := testutil.NewInMemStorage()
	genesis := testutil.NewTestBlock(simplex.ProtocolMetadata{
		Seq:   0,
		Round: 0,
		Epoch: 0,
	}, simplex.Blacklist{})
	require.NoError(t, storage.Index(context.Background(), genesis, simplex.Finalization{}))
	return storage
}

// newStorageWithFirstEpoch returns a storage indexed up to and including lastSeq:
// seq 0 is genesis, seq 1 is the sealing block that opens epoch 1, and seqs 2..lastSeq
// are additional epoch-1 blocks. lastSeq must be >= 1.
func newStorageWithFirstEpoch(t *testing.T, lastSeq uint64) *testutil.InMemStorage {
	require.GreaterOrEqual(t, lastSeq, uint64(1), "lastSeq must be >= 1 (0 is genesis, 1 is the first epoch's sealing block)")

	storage := newStorageWithGenesis(t)

	validatorSet := make(simplex.Nodes, len(testNodes))
	for i, n := range testNodes {
		validatorSet[i] = simplex.Node{Node: n}
	}

	sealingBlock := newSealingTestBlock(1, 1, 1, &simplex.SealingBlockInfo{
		Epoch:        1,
		ValidatorSet: validatorSet,
	})
	require.NoError(t, storage.Index(context.Background(), sealingBlock, simplex.Finalization{}))

	for seq := uint64(2); seq <= lastSeq; seq++ {
		block := newSealingTestBlock(seq, seq, 1, nil)
		require.NoError(t, storage.Index(context.Background(), block, simplex.Finalization{}))
	}

	return storage
}

type TestConfig struct {
	nodes   []simplex.NodeID
	storage simplex.Storage
}

// PoA Non-Validator
func newTestNonValidator(t *testing.T, testConfig TestConfig) *NonValidator {
	var storage simplex.Storage = newStorageWithGenesis(t)
	if testConfig.storage != nil {
		storage = testConfig.storage
	}

	nodes := testNodes
	if len(testConfig.nodes) > 0 {
		nodes = testConfig.nodes
	}

	config := Config{
		Storage:        storage,
		Comm:           testutil.NewNoopComm(nodes),
		Logger:         testutil.MakeLogger(t, 1),
		MaxRoundWindow: simplex.DefaultMaxRoundWindow,
		SignatureAggregatorCreator: func(n []simplex.Node) simplex.SignatureAggregator {
			return &testutil.TestSignatureAggregator{N: len(n)}
		},
	}

	nonValidator, err := NewNonValidator(config)
	require.NoError(t, err)

	return nonValidator
}

func blockMessage(t *testing.T, block simplex.Block, from simplex.NodeID) *simplex.Message {
	vote, err := testutil.NewTestVote(block, from)
	require.NoError(t, err)
	return &simplex.Message{
		BlockMessage: &simplex.BlockMessage{
			Block: block,
			Vote:  *vote,
		},
	}
}

func newBlock(seq, epoch uint64, prev simplex.Digest) *testutil.TestBlock {
	return testutil.NewTestBlock(simplex.ProtocolMetadata{
		Round: seq,
		Seq:   seq,
		Epoch: epoch,
		Prev:  prev,
	}, simplex.Blacklist{})
}

func newFinalization(t *testing.T, block *testutil.TestBlock, nodes []simplex.NodeID) *simplex.Finalization {
	logger := testutil.MakeLogger(t, 1)

	finalization, _ := testutil.NewFinalizationRecord(t, logger, &testutil.TestSignatureAggregator{}, block, nodes)
	return &finalization
}

func AnyToMessage(t *testing.T, msg any, nodes []simplex.NodeID) *messageInfo {
	switch m := msg.(type) {
	case *testutil.TestBlock:
		leader := simplex.LeaderForRound(nodes, m.BlockHeader().Round)
		return &messageInfo{
			msg:  blockMessage(t, m, leader),
			from: leader,
		}
	case *simplex.Finalization:
		return &messageInfo{
			msg:  &simplex.Message{Finalization: m},
			from: nodes[0],
		}
	default:
		t.Fatal("not a proper msg")
	}

	return nil
}

type messageInfo struct {
	msg  *simplex.Message
	from simplex.NodeID
}

func TestHandleMessages(t *testing.T) {
	// initialize storage with
	// block 0 is genesis
	// block 1 is first simplex block
	// block 2 is next indexed block

	// Test these order of operations of messages received
	// 1. block 3, finalization 3

	// 2. block 3(sealing block), finalization 3(sealing block)
	//    block 4(next epoch), finalization 4(next epoch)

	// 3. block 4(next epoch), finalization 4(next epoch)
	//    block 3 = finalization (ensure 4 don't get indexed)
	storage := newStorageWithFirstEpoch(t, 2)
	nonValidator := newTestNonValidator(t, TestConfig{
		storage: storage,
	})
	block2, _, err := storage.Retrieve(2)
	require.NoError(t, err)

	block3 := newBlock(3, 1, block2.BlockHeader().Digest)
	block4 := newBlock(4, 1, block3.Digest)
	block5 := newBlock(5, 1, block4.Digest)
	finalization3 := newFinalization(t, block3, testNodes)
	finalization4 := newFinalization(t, block4, testNodes)
	finalization5 := newFinalization(t, block5, testNodes)

	msgs := []*messageInfo{
		AnyToMessage(t, block3, testNodes),
		AnyToMessage(t, block4, testNodes),
		AnyToMessage(t, block5, testNodes),
		AnyToMessage(t, finalization5, testNodes),
		AnyToMessage(t, finalization4, testNodes),
		AnyToMessage(t, finalization3, testNodes),
	}

	for _, msg := range msgs {
		nonValidator.HandleMessage(msg.msg, msg.from)
	}

	// If the blocks were indexed they were verified in order
	storage.WaitForBlockCommit(5)
	require.Equal(t, uint64(6), nonValidator.Storage.NumBlocks())
}

// // TestValidatedNextEpoch tests that blocks and finalizations can be verified & indexed for the next epoch
// // we have validated. A validated epoch means one where we have confirmed the sealing block that created
// func TestValidatedNextEpoch(t *testing.T) {
// 	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}

// 	tests := []struct {
// 		name string
// 		// sendBlock controls whether a block message is sent before the finalization.
// 		msgs              []simplex.Message
// 		blockSender       simplex.NodeID
// 		expectVerified    bool
// 		expectedNumBlocks uint64
// 	}{
// 		{
// 			name: "Finalization Only No Block",
// 		},
// 		{
// 			name:        "Block From Non-Leader Then Finalization",
// 			sendBlock:   true,
// 			blockSender: nodes[1],
// 		},
// 		{
// 			name:              "Block From Leader Then Finalization",
// 			sendBlock:         true,
// 			blockSender:       nodes[0],
// 			expectVerified:    true,
// 			expectedNumBlocks: 1,
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			var lastVerified simplex.Block
// 			v := newTestNonValidator(t, nodes, lastVerified)

// 			var verified atomic.Bool
// 			blockToSend := testutil.NewTestBlock(simplex.ProtocolMetadata{
// 				Round: 0,
// 				Seq:   0,
// 				Epoch: 0,
// 			}, simplex.Blacklist{})
// 			blockToSend.OnVerify = func() {
// 				verified.Store(true)
// 			}

// 			if tt.sendBlock {
// 				err := v.HandleMessage(blockMessage(t, blockToSend, nodes[0]), tt.blockSender)
// 				require.NoError(t, err)
// 			}

// 			finalization, _ := testutil.NewFinalizationRecord(t, v.Logger, &testutil.TestSignatureAggregator{}, blockToSend, nodes)
// 			err := v.HandleMessage(&simplex.Message{Finalization: &finalization}, nodes[0])
// 			require.NoError(t, err)

// 			if tt.expectVerified {
// 				require.Eventually(t, verified.Load, 2*time.Second, 20*time.Millisecond)
// 			} else {
// 				require.Never(t, verified.Load, 2*time.Second, 20*time.Millisecond)
// 			}
// 			require.Equal(t, tt.expectedNumBlocks, v.Storage.NumBlocks())
// 		})
// 	}
// }

// // func TestHandleBlockDigestMismatch(t *testing.T) {
// // 	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
// // 	v := newTestNonValidator(t, nodes, nil)

// // 	metadata := simplex.ProtocolMetadata{Seq: 0, Epoch: 0, Round: 0}
// // 	blockA := testutil.NewTestBlock(metadata, simplex.Blacklist{})
// // 	blockB := testutil.NewTestBlock(metadata, simplex.Blacklist{})
// // 	blockB.Data = []byte("different")
// // 	blockB.ComputeDigest()

// // 	// send finalization for blockB
// // 	finalization, _ := testutil.NewFinalizationRecord(t, v.Logger, &testutil.TestSignatureAggregator{}, blockB, nodes)
// // 	err := v.HandleMessage(&simplex.Message{Finalization: &finalization}, nodes[0])
// // 	require.NoError(t, err)

// // 	// send block message for blockA — digest differs from stored finalization
// // 	err = v.HandleMessage(blockMessage(t, blockA, nodes[0]), nodes[0])
// // 	require.NoError(t, err)

// // 	require.Equal(t, uint64(0), v.Storage.NumBlocks())
// // }

// // func TestHandleFinalizationDigestMismatch(t *testing.T) {
// // 	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
// // 	v := newTestNonValidator(t, nodes, nil)

// // 	metadata := simplex.ProtocolMetadata{Seq: 0, Epoch: 0, Round: 0}
// // 	blockA := testutil.NewTestBlock(metadata, simplex.Blacklist{})
// // 	blockB := testutil.NewTestBlock(metadata, simplex.Blacklist{})
// // 	blockB.Data = []byte("different")
// // 	blockB.ComputeDigest()

// // 	// send block message for blockA from leader
// // 	err := v.HandleMessage(blockMessage(t, blockA, nodes[0]), nodes[0])
// // 	require.NoError(t, err)

// // 	// send finalization for blockB — digest differs from stored block
// // 	finalization, _ := testutil.NewFinalizationRecord(t, v.Logger, &testutil.TestSignatureAggregator{}, blockB, nodes)
// // 	err = v.HandleMessage(&simplex.Message{Finalization: &finalization}, nodes[0])
// // 	require.NoError(t, err)

// // 	require.Equal(t, uint64(0), v.Storage.NumBlocks())
// // }

// // func TestHandleFinalizationFailsVerification(t *testing.T) {
// // 	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
// // 	v := newTestNonValidator(t, nodes, nil)

// // 	var verified atomic.Bool
// // 	block := testutil.NewTestBlock(simplex.ProtocolMetadata{
// // 		Round: 0,
// // 		Seq:   0,
// // 		Epoch: 0,
// // 	}, simplex.Blacklist{})
// // 	block.OnVerify = func() {
// // 		verified.Store(true)
// // 	}

// // 	// send block from leader
// // 	err := v.HandleMessage(blockMessage(t, block, nodes[0]), nodes[0])
// // 	require.NoError(t, err)

// // 	// send a finalization that fails verification
// // 	finalization := &simplex.Finalization{
// // 		QC: errQC{},
// // 	}
// // 	err = v.HandleMessage(&simplex.Message{Finalization: finalization}, nodes[0])
// // 	require.NoError(t, err)

// // 	require.Never(t, verified.Load, 2*time.Second, 20*time.Millisecond)
// // 	require.Equal(t, uint64(0), v.Storage.NumBlocks())
// // }

// // func TestBlockVerifyCalledOnce(t *testing.T) {
// // 	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
// // 	v := newTestNonValidator(t, nodes, nil)

// // 	verificationDelay := make(chan struct{})
// // 	var verifyCount atomic.Int32

// // 	block := testutil.NewTestBlock(simplex.ProtocolMetadata{Seq: 0, Round: 0, Epoch: 0}, simplex.Blacklist{})
// // 	block.VerificationDelay = verificationDelay
// // 	block.OnVerify = func() {
// // 		verifyCount.Add(1)
// // 		// Schedule a second verification while task 1 is still inside Verify.
// // 		// The OneTimeVerifier should return the cached result without calling Verify again.
// // 		_ = v.verifier.triggerVerify(block)
// // 	}

// // 	finalization, _ := testutil.NewFinalizationRecord(t, v.Logger, &testutil.TestSignatureAggregator{}, block, nodes)
// // 	err := v.HandleMessage(&simplex.Message{Finalization: &finalization}, nodes[0])
// // 	require.NoError(t, err)

// // 	err = v.HandleMessage(blockMessage(t, block, nodes[0]), nodes[0])
// // 	require.NoError(t, err)

// // 	// Unblock the in-progress verification.
// // 	close(verificationDelay)

// // 	require.Eventually(t, func() bool { return verifyCount.Load() == 1 }, 2*time.Second, 20*time.Millisecond)
// // 	require.Never(t, func() bool { return verifyCount.Load() > 1 }, 200*time.Millisecond, 20*time.Millisecond)
// // }

// // func TestHandleBlockMessage(t *testing.T) {
// // 	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}

// // 	tests := []struct {
// // 		name              string
// // 		lastVerifiedBlock *testutil.TestBlock
// // 		// finalizationBlock returns the block to finalize; nil means no finalization is sent.
// // 		// lastVerified may be nil when lastVerifiedBlock is not set for the test case.
// // 		finalizationBlock func(lastVerified, blockToSend *testutil.TestBlock) *testutil.TestBlock
// // 		blockSender       simplex.NodeID
// // 		blockSeq          uint64
// // 		expectVerified    bool
// // 		expectedNumBlocks uint64
// // 	}{
// // 		{
// // 			name:        "Next to Verify But No Finalization",
// // 			blockSender: nodes[0],
// // 		},
// // 		{
// // 			name:              "BlockMessage not sent from leader",
// // 			finalizationBlock: func(_, blockToSend *testutil.TestBlock) *testutil.TestBlock { return blockToSend },
// // 			blockSender:       nodes[1],
// // 		},
// // 		{
// // 			name: "Already Verified",
// // 			lastVerifiedBlock: testutil.NewTestBlock(simplex.ProtocolMetadata{
// // 				Round: 0,
// // 				Seq:   0,
// // 				Epoch: 0,
// // 			}, simplex.Blacklist{}),
// // 			finalizationBlock: func(lastVerified, _ *testutil.TestBlock) *testutil.TestBlock { return lastVerified },
// // 			blockSender:       nodes[1],
// // 		},
// // 		{
// // 			name:              "Finalization Received",
// // 			finalizationBlock: func(_, blockToSend *testutil.TestBlock) *testutil.TestBlock { return blockToSend },
// // 			blockSender:       nodes[0],
// // 			expectVerified:    true,
// // 			expectedNumBlocks: 1,
// // 		},
// // 		{
// // 			// seq 1 arrives with a finalization but seq 0 has not been verified yet,
// // 			// so the block is indexed but verification is deferred.
// // 			name:              "Finalization Received But Not Next To Verify",
// // 			finalizationBlock: func(_, blockToSend *testutil.TestBlock) *testutil.TestBlock { return blockToSend },
// // 			blockSender:       nodes[0],
// // 			blockSeq:          1,
// // 			expectVerified:    false,
// // 			expectedNumBlocks: 1,
// // 		},
// // 	}

// // 	for _, tt := range tests {
// // 		t.Run(tt.name, func(t *testing.T) {
// // 			var lastVerified simplex.Block
// // 			if tt.lastVerifiedBlock != nil {
// // 				lastVerified = tt.lastVerifiedBlock
// // 			}
// // 			v := newTestNonValidator(t, nodes, lastVerified)

// // 			var verified atomic.Bool
// // 			blockToSend := testutil.NewTestBlock(simplex.ProtocolMetadata{
// // 				Round: 0,
// // 				Seq:   tt.blockSeq,
// // 				Epoch: 0,
// // 			}, simplex.Blacklist{})
// // 			blockToSend.OnVerify = func() {
// // 				verified.Store(true)
// // 			}

// // 			if tt.finalizationBlock != nil {
// // 				finalizeBlock := tt.finalizationBlock(tt.lastVerifiedBlock, blockToSend)
// // 				finalization, _ := testutil.NewFinalizationRecord(t, v.Logger, &testutil.TestSignatureAggregator{}, finalizeBlock, nodes)
// // 				v.HandleMessage(&simplex.Message{Finalization: &finalization}, nodes[0])
// // 			}

// // 			err := v.HandleMessage(blockMessage(t, blockToSend, nodes[0]), tt.blockSender)
// // 			require.NoError(t, err)

// // 			if tt.expectVerified {
// // 				require.Eventually(t, verified.Load, 2*time.Second, 20*time.Millisecond)
// // 			} else {
// // 				require.Never(t, verified.Load, 2*time.Second, 20*time.Millisecond)
// // 			}
// // 			require.Equal(t, tt.expectedNumBlocks, v.Storage.NumBlocks())
// // 		})
// // 	}
// // }

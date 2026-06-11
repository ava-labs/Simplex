// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ava-labs/simplex/common"
	"github.com/ava-labs/simplex/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

// errQC is a QuorumCertificate whose Verify always fails. Used to drive
// the "qc does not verify" code path without mutating message contents.
type errQC struct {
	common.QuorumCertificate
}

func (errQC) Verify([]byte) error {
	return errors.New("qc verification failed")
}

type TestConfig struct {
	nodes      common.Nodes
	storage    common.Storage
	comm       common.Communication
	sigCreator common.SignatureAggregatorCreator
}

func blockMessage(t *testing.T, block common.Block, from common.NodeID) *common.Message {
	vote, err := testutil.NewTestVote(block, from)
	require.NoError(t, err)
	return &common.Message{
		BlockMessage: &common.BlockMessage{
			Block: block,
			Vote:  *vote,
		},
	}
}

func newBlock(seq, epoch uint64, prev common.Digest) *testutil.TestBlock {
	return testutil.NewTestBlock(common.ProtocolMetadata{
		Round: seq,
		Seq:   seq,
		Epoch: epoch,
		Prev:  prev,
	}, common.Blacklist{})
}

// blockMsg wraps b in a messageInfo sent by the round leader.
func blockMsg(t *testing.T, b common.Block, nodes common.Nodes) *messageInfo {
	leader := simplex.LeaderForRound(nodes.NodeIDs(), b.BlockHeader().Round)
	return &messageInfo{
		msg:  blockMessage(t, b, leader),
		from: leader,
	}
}

// finalizationMsg mints a fresh Finalization for b signed by every node in
// nodes, and wraps it in a messageInfo sent by nodes[0]. It takes the block
// (not a pre-built finalization) so callers can chain straight from
// chain.appendBlock without keeping a separate finalization variable.
func finalizationMsg(t *testing.T, b common.VerifiedBlock, nodes common.Nodes) *messageInfo {
	fin, _ := testutil.NewFinalizationRecord(t, &testutil.TestSignatureAggregator{}, b, nodes.NodeIDs())
	return &messageInfo{
		msg:  &common.Message{Finalization: &fin},
		from: nodes.NodeIDs()[0],
	}
}

// TestHandleMessages drives the non-validator with blocks and finalizations
// arriving in various orders and asserts it verifies and indexes them up to
// the expected height, including across epoch transitions.
func TestHandleMessages(t *testing.T) {
	// Each case starts from a chain seeded with seq 0 (genesis), seq 1
	// (sealing block opening epoch 1), and seq 2 (an epoch-1 block).
	tests := []struct {
		name           string
		setup          func(t *testing.T) (*testChain, []*messageInfo)
		expectedHeight uint64
	}{
		{
			name: "blocks and finalizations out of order",
			setup: func(t *testing.T) (*testChain, []*messageInfo) {
				tc := newSeededChain(t, testNodes, 2)
				b3, b4, b5 := tc.appendBlock(), tc.appendBlock(), tc.appendBlock()

				// finalization for seq 3 arrives last and should kick off
				// verification for seqs 3-5.
				return tc, []*messageInfo{
					blockMsg(t, b4, testNodes),
					blockMsg(t, b3, testNodes),
					finalizationMsg(t, b4, testNodes),
					finalizationMsg(t, b5, testNodes),
					blockMsg(t, b5, testNodes),
					finalizationMsg(t, b3, testNodes),
				}
			},
			expectedHeight: 6,
		},
		{
			name: "nil block doesn't panic",
			setup: func(t *testing.T) (*testChain, []*messageInfo) {
				tc := newSeededChain(t, testNodes, 2)
				b3 := tc.appendBlock()
				msg := blockMsg(t, b3, testNodes)
				msg.msg.BlockMessage.Block = nil

				return tc, []*messageInfo{msg}
			},
			expectedHeight: 3,
		},
		{
			name: "same block doesn't get verified or indexed twice",
			setup: func(t *testing.T) (*testChain, []*messageInfo) {
				tc := newSeededChain(t, testNodes, 2)
				b3, b4, b5 := tc.appendBlock(), tc.appendBlock(), tc.appendBlock()

				// sending a second b4 message will try and schedule the verification task for seq 4 again
				// If the task is scheduled twice, the test will fail since storage will panic if we index the same seq twice.
				return tc, []*messageInfo{
					blockMsg(t, b4, testNodes),
					finalizationMsg(t, b4, testNodes),
					blockMsg(t, b4, testNodes),
					blockMsg(t, b3, testNodes),
					finalizationMsg(t, b5, testNodes),
					blockMsg(t, b5, testNodes),
					finalizationMsg(t, b3, testNodes),
				}
			},
			expectedHeight: 6,
		},
		{
			name: "next block followed by finalization",
			setup: func(t *testing.T) (*testChain, []*messageInfo) {
				tc := newSeededChain(t, testNodes, 2)
				b3 := tc.appendBlock()

				return tc, []*messageInfo{
					blockMsg(t, b3, testNodes),
					finalizationMsg(t, b3, testNodes),
				}
			},
			expectedHeight: 4,
		},
		{
			name: "block message received from non leader",
			setup: func(t *testing.T) (*testChain, []*messageInfo) {
				tc := newSeededChain(t, testNodes, 2)
				b3 := tc.appendBlock()

				blockMsg := blockMsg(t, b3, testNodes)
				blockMsg.from = testNodes.NodeIDs()[0]
				return tc, []*messageInfo{
					blockMsg,
					finalizationMsg(t, b3, testNodes),
				}
			},
			expectedHeight: 3,
		},
		{
			name: "block digest mismatch with finalization",
			setup: func(t *testing.T) (*testChain, []*messageInfo) {
				tc := newSeededChain(t, testNodes, 2)
				b3 := tc.appendBlock()
				f3 := finalizationMsg(t, b3, testNodes)

				b3.Digest = common.Digest{}
				blockMsg := blockMsg(t, b3, testNodes)
				return tc, []*messageInfo{
					blockMsg,
					f3,
				}
			},
			expectedHeight: 3,
		},
		{
			name: "qc does not verify",
			setup: func(t *testing.T) (*testChain, []*messageInfo) {
				tc := newSeededChain(t, testNodes, 2)
				b3 := tc.appendBlock()
				f3 := finalizationMsg(t, b3, testNodes)
				f3.msg.Finalization.QC = errQC{QuorumCertificate: f3.msg.Finalization.QC}

				return tc, []*messageInfo{
					blockMsg(t, b3, testNodes),
					f3,
				}
			},
			expectedHeight: 3,
		},
		{
			name: "multiple epochs",
			setup: func(t *testing.T) (*testChain, []*messageInfo) {
				tc := newSeededChain(t, testNodes, 2)

				var epoch3Nodes = common.Nodes{
					{Node: common.NodeID{1}, Weight: 1},
					{Node: common.NodeID{2}, Weight: 1},
					{Node: common.NodeID{3}, Weight: 1},
					{Node: common.NodeID{4}, Weight: 1},
					{Node: common.NodeID{5}, Weight: 1},
				}

				// Send the sealing block + finalization that transitions to epoch 3.
				b3 := tc.appendSealing(epoch3Nodes)
				// Blocks 4 and 5 live in epoch 3 and are finalized by the new validator set.
				b4, b5 := tc.appendBlock(), tc.appendBlock()
				// Block 6 is another sealing epoch
				b6 := tc.appendSealing(testNodes)
				// Block 7 is part of the new epoch
				b7 := tc.appendBlock()

				return tc, []*messageInfo{
					blockMsg(t, b3, testNodes),
					finalizationMsg(t, b3, testNodes),
					finalizationMsg(t, b4, epoch3Nodes),
					finalizationMsg(t, b5, epoch3Nodes),
					blockMsg(t, b4, epoch3Nodes),
					blockMsg(t, b5, epoch3Nodes),
					blockMsg(t, b6, epoch3Nodes),
					finalizationMsg(t, b6, epoch3Nodes),
					blockMsg(t, b7, testNodes),
					finalizationMsg(t, b7, testNodes),
				}
			},
			expectedHeight: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc, msgs := tt.setup(t)
			require.Equal(t, uint64(3), tc.NumBlocks())
			nv, err := NewNonValidator(
				Config{
					Storage:                    tc,
					Comm:                       testutil.NewNoopComm(tc.nodes().NodeIDs()),
					Logger:                     testutil.MakeLogger(t, 1),
					SignatureAggregatorCreator: tc.signatureAggregatorCreator,
					MaxSequenceWindow:          simplex.DefaultMaxRoundWindow,
					ID:                         testNodes[0].Node,
				},
			)
			require.NoError(t, err)

			for _, m := range msgs {
				require.NoError(t, m.send(nv))
			}

			tc.WaitForBlockCommit(tt.expectedHeight - 1)
			require.Equal(t, tt.expectedHeight, nv.Storage.NumBlocks())
		})
	}
}

func TestNonValidator_StopsGracefully(t *testing.T) {
	tc := newSeededChain(t, testNodes, 2)
	nv, err := NewNonValidator(
		Config{
			Storage:                    tc,
			Comm:                       testutil.NewNoopComm(tc.nodes().NodeIDs()),
			Logger:                     testutil.MakeLogger(t, 1),
			SignatureAggregatorCreator: tc.signatureAggregatorCreator,
			MaxSequenceWindow:          simplex.DefaultMaxRoundWindow,
			ID:                         testNodes[0].Node,
		},
	)
	require.NoError(t, err)

	nv.Start()
	nv.Stop()

	b3 := tc.appendBlock()
	require.NoError(t, blockMsg(t, b3, testNodes).send(nv))
	require.NoError(t, finalizationMsg(t, b3, testNodes).send(nv))

	require.Never(t,
		func() bool {
			return nv.Storage.NumBlocks() > 3
		},
		2*time.Second,
		50*time.Millisecond,
	)
}

// TestHandleMessages_DuplicateBlock tests that when a duplicate block is received, the block is verify & indexed only once
func TestHandleMessages_DuplicateBlock(t *testing.T) {
	tc := newSeededChain(t, testNodes, 2)
	nv, err := NewNonValidator(
		Config{
			Storage:                    tc,
			Comm:                       testutil.NewNoopComm(tc.nodes().NodeIDs()),
			Logger:                     testutil.MakeLogger(t, 1),
			SignatureAggregatorCreator: tc.signatureAggregatorCreator,
			MaxSequenceWindow:          simplex.DefaultMaxRoundWindow,
			ID:                         testNodes[0].Node,
		},
	)
	require.NoError(t, err)

	nv.Start()
	defer nv.Stop()

	// Send the sealing block + finalization that transitions to epoch 3.
	b3 := tc.appendBlock()
	require.NoError(t, blockMsg(t, b3, testNodes).send(nv))
	require.NoError(t, finalizationMsg(t, b3, testNodes).send(nv))

	tc.WaitForBlockCommit(3)

	// Storage will panic if we try indexing the same block twice
	require.NoError(t, blockMsg(t, b3, testNodes).send(nv))
	require.NoError(t, finalizationMsg(t, b3, testNodes).send(nv))
}

// TestNonValidator_RequestHighestEpochOnStart verifies that a non-validator
// starting behind the network issues a replication request for the highest
// epoch on startup.
func TestNonValidator_RequestHighestEpochOnStart(t *testing.T) {
	tc := newSeededChain(t, testNodes, 2)
	tc.addEpochs(4, 8)
	responder := newTestResponder(t, testNodes.NodeIDs()[0], tc)

	nvStorage := tc.CloneUntil(2)
	nv, err := NewNonValidator(
		Config{
			Storage:                    nvStorage,
			Comm:                       responder,
			Logger:                     testutil.MakeLogger(t, 1),
			SignatureAggregatorCreator: tc.signatureAggregatorCreator,
			MaxSequenceWindow:          simplex.DefaultMaxRoundWindow,
			ID:                         responder.ID,
		},
	)

	require.NoError(t, err)

	nv.Start()
	defer nv.Stop()

	msg, ok := responder.popResponse()
	require.True(t, ok)
	require.NotNil(t, msg.msg.ReplicationResponse)
	require.NotNil(t, msg.msg.ReplicationResponse.LatestSeq)
}

// TestNonValidator_Bootstrap ensures a non-validator can replicate sequences given different states of the chain.
func TestNonValidator_Bootstrap(t *testing.T) {
	tests := []struct {
		name string
		// setup builds the full network chain the non-validator replicates from.
		setup             func(t *testing.T) *testChain
		maxSequenceWindow uint64
		// initialHeight is the number of blocks the non-validator's storage is
		// seeded with before replication begins.
		initialHeight uint64
		lastSeq       uint64
	}{
		{
			// Replicates multiple epochs within a single replication window.
			name: "replicates epochs",
			setup: func(t *testing.T) *testChain {
				tc := newSeededChain(t, testNodes, 2)
				tc.addEpochs(5, 10, 20, 30, 40)
				return tc
			},
			maxSequenceWindow: 50,
			initialHeight:     2,
			lastSeq:           40,
		},
		{
			// A small window forces replication well past the max round window.
			name: "past max round window",
			setup: func(t *testing.T) *testChain {
				tc := newSeededChain(t, testNodes, 2)
				tc.addEpochs(5, 10, 20, 30, 40, 50, 60, 80, 100)
				return tc
			},
			maxSequenceWindow: 5, // significantly lower
			initialHeight:     2,
			lastSeq:           100,
		},
		{
			// Storage starts with only the genesis block.
			name: "from genesis",
			setup: func(t *testing.T) *testChain {
				tc := newSeededChain(t, testNodes, 2)
				tc.addEpochs(5, 10, 20)
				return tc
			},
			maxSequenceWindow: 50,
			initialHeight:     1, // genesis is the only block
			lastSeq:           20,
		},
		{
			// Replicates a chain converted from snowman to simplex.
			name: "converted simplex chain",
			setup: func(t *testing.T) *testChain {
				tc := newSnowToSimplexChain(t, 10)
				firstBlock := tc.appendFirstSimplexAfterGenesis(testNodes)
				tc.Index(context.Background(), firstBlock, tc.newFinalization(firstBlock))
				return tc
			},
			maxSequenceWindow: 50,
			initialHeight:     11, // lastSnowmanSeq + 1
			lastSeq:           11,
		},
		{
			// Converted chain that then spans several simplex epochs.
			name: "converted simplex chain many epochs",
			setup: func(t *testing.T) *testChain {
				tc := newSnowToSimplexChain(t, 10)
				firstBlock := tc.appendFirstSimplexAfterGenesis(testNodes)
				tc.Index(context.Background(), firstBlock, tc.newFinalization(firstBlock))
				tc.addEpochs(20, 30)
				return tc
			},
			maxSequenceWindow: 50,
			initialHeight:     11, // lastSnowmanSeq + 1
			lastSeq:           30,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := tt.setup(t)
			responder := newTestResponder(t, testNodes.NodeIDs()[0], tc)
			nvStorage := tc.CloneUntil(tt.initialHeight)
			require.Equal(t, tt.initialHeight, nvStorage.NumBlocks())

			nv, err := NewNonValidator(
				Config{
					Storage:                    nvStorage,
					Comm:                       responder,
					Logger:                     testutil.MakeLogger(t, 1),
					SignatureAggregatorCreator: tc.signatureAggregatorCreator,
					MaxSequenceWindow:          tt.maxSequenceWindow,
					ID:                         responder.ID,
					StartTime:                  time.Now(),
				},
			)
			require.NoError(t, err)

			nv.Start()
			defer nv.Stop()

			advanceUntil(nv, responder, tt.lastSeq)
		})
	}
}

func TestNonValidator_ReplicationRequests(t *testing.T) {
	tc := newSeededChain(t, testNodes, 2)
	lastSeq := uint64(40)
	initialHeight := uint64(2)
	tc.addEpochs(5, 10, 20, 30, lastSeq)
	responder := newTestResponder(t, testNodes.NodeIDs()[0], tc)
	nvStorage := tc.CloneUntil(initialHeight)
	startTime := time.Now()
	nv, err := NewNonValidator(
		Config{
			Storage:                    nvStorage,
			Comm:                       responder,
			Logger:                     testutil.MakeLogger(t, 1),
			SignatureAggregatorCreator: tc.signatureAggregatorCreator,
			MaxSequenceWindow:          50,
			ID:                         responder.ID,
			StartTime:                  startTime,
		},
	)
	require.NoError(t, err)

	nv.Start()
	defer nv.Stop()

	count := 0
	for {
		// Send any requests as responses back to the node
		for msg, ok := responder.popResponse(); ok; {
			// drop every other message
			if count%2 == 0 {
				require.NoError(t, msg.send(nv))
			}
			count += 1
			msg, ok = responder.popResponse()
		}

		// check if storage has indexed all
		if lastSeq == nvStorage.NumBlocks()-1 {
			break
		}

		// update the time
		startTime = startTime.Add(simplex.DefaultReplicationRequestTimeout)
		nv.AdvanceTime(startTime)
	}

	// clear in flight responses
	startTime = startTime.Add(simplex.DefaultReplicationRequestTimeout)
	nv.AdvanceTime(startTime)
	responder.clearResponses()

	// ensure all timeout tasks were removed
	count = 0
	for {
		startTime = startTime.Add(simplex.DefaultReplicationRequestTimeout)
		nv.AdvanceTime(startTime)
		msg, ok := responder.popResponse()
		require.False(t, ok, fmt.Sprintf("all replication request tasks should be finished %v", msg))

		if count > 3 {
			break
		}
		count += 1
	}
}

// We optimistically store quorum rounds for sequences that validate an epoch(even though we have not verified the finalization)
// This test ensures, we verify the finalization when it is time to process it. If the finalization is incorrect, we need to re-request
// from the replicator.
func TestNonValidator_VerifiesFinalizationDuringReplication(t *testing.T) {
	tc := newSeededChain(t, testNodes, 2)
	epoch3Nodes := append(testNodes, common.Node{
		Node:   common.NodeID{byte(10)},
		Weight: 1,
	})
	epoch4Nodes := append(epoch3Nodes, common.Node{
		Node:   common.NodeID{byte(11)},
		Weight: 1,
	})

	startTime := time.Now()
	storage := testutil.NewInMemStorage()
	require.NoError(t, storage.Index(context.Background(), genesis, common.Finalization{}))

	nv, err := NewNonValidator(
		Config{
			Storage:                    storage,
			Comm:                       testutil.NewNoopComm(epoch4Nodes.NodeIDs()),
			Logger:                     testutil.MakeLogger(t, 1),
			SignatureAggregatorCreator: tc.signatureAggregatorCreator,
			MaxSequenceWindow:          5, // significantly lower the max round window
			ID:                         testNodes.NodeIDs()[0],
			StartTime:                  startTime,
		},
	)

	require.NoError(t, err)
	nv.Start()
	defer nv.Stop()

	s3, s4 := tc.appendSealing(epoch3Nodes), tc.appendSealing(epoch4Nodes)
	f3 := tc.newFinalization(s3)
	brokenFinalization := tc.newFinalization(s4)
	brokenFinalization.Finalization.Epoch = 9000
	brokenFinalization.QC = errQC{QuorumCertificate: brokenFinalization.QC}

	brokenQR := common.QuorumRound{
		Block:        s4,
		Finalization: &brokenFinalization,
	}
	brokenReplicationResponse := &common.ReplicationResponse{
		LatestSeq: &brokenQR,
	}

	// With 6 nodes, the threshold votes for validating the highest epoch is  2. Because F + 1 = 2.
	require.NoError(t, nv.HandleMessage(
		&common.Message{
			ReplicationResponse: brokenReplicationResponse,
		},
		testNodes.NodeIDs()[2],
	))
	require.NoError(t, nv.HandleMessage(
		&common.Message{
			ReplicationResponse: brokenReplicationResponse,
		},
		testNodes.NodeIDs()[3],
	))

	// send seq 1, 2, 3
	s1, f1, err := tc.Retrieve(1)
	require.NoError(t, err)
	s2, f2, err := tc.Retrieve(2)
	require.NoError(t, err)

	require.NoError(t, nv.HandleMessage(
		&common.Message{
			ReplicationResponse: &common.ReplicationResponse{
				Data: []common.QuorumRound{
					{
						Block:        s3,
						Finalization: &f3,
					},
					{
						Block:        s1.(common.Block),
						Finalization: &f1,
					},
					{
						Block:        s2.(common.Block),
						Finalization: &f2,
					},
				},
			},
		},
		testNodes.NodeIDs()[3],
	))

	require.Never(t,
		func() bool {
			return storage.NumBlocks() >= 5
		},
		2*time.Second,
		50*time.Millisecond,
	)
}

func advanceUntil(nv *NonValidator, responder *nonValidatorResponderComm, seq uint64) {
	startTime := nv.StartTime
	for {
		// Send any requests as responses back to the node
		for msg, ok := responder.popResponse(); ok; {
			// drop every other message
			require.NoError(responder.t, msg.send(nv))
			msg, ok = responder.popResponse()
		}

		// check if storage has indexed all
		if seq == nv.Storage.NumBlocks()-1 {
			break
		}

		// update the time
		startTime = startTime.Add(simplex.DefaultReplicationRequestTimeout)
		nv.AdvanceTime(startTime)
	}
}

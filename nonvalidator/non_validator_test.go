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

// errQC is a QuorumCertificate whose Verify always fails. Used to drive
// the "qc does not verify" code path without mutating message contents.
type errQC struct {
	simplex.QuorumCertificate
}

func (errQC) Verify([]byte) error {
	return errors.New("qc verification failed")
}

type TestConfig struct {
	nodes      simplex.Nodes
	storage    simplex.Storage
	comm       simplex.Communication
	sigCreator simplex.SignatureAggregatorCreator
}

// PoA Non-Validator
func newTestNonValidator(t *testing.T, testConfig TestConfig) *NonValidator {
	var storage simplex.Storage
	if testConfig.storage != nil {
		storage = testConfig.storage
	} else {
		s := testutil.NewInMemStorage()
		require.NoError(t, s.Index(context.Background(), genesis, simplex.Finalization{}))
		storage = s
	}

	nodes := testNodes
	if len(testConfig.nodes) > 0 {
		nodes = testConfig.nodes
	}

	var comm simplex.Communication = testutil.NewNoopComm(nodes.NodeIDs())
	if testConfig.comm != nil {
		comm = testConfig.comm
	}

	sigCreator := func(n []simplex.Node) simplex.SignatureAggregator {
		return &testutil.TestSignatureAggregator{N: len(n)}
	}
	if testConfig.sigCreator != nil {
		sigCreator = testConfig.sigCreator
	}

	config := Config{
		Storage:                    storage,
		Comm:                       comm,
		Logger:                     testutil.MakeLogger(t, 1),
		MaxRoundWindow:             simplex.DefaultMaxRoundWindow,
		SignatureAggregatorCreator: sigCreator,
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

// blockMsg wraps b in a messageInfo sent by the round leader.
func blockMsg(t *testing.T, b simplex.Block, nodes simplex.Nodes) *messageInfo {
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
func finalizationMsg(t *testing.T, b simplex.VerifiedBlock, nodes simplex.Nodes) *messageInfo {
	logger := testutil.MakeLogger(t, 1)
	fin, _ := testutil.NewFinalizationRecord(t, logger, &testutil.TestSignatureAggregator{}, b, nodes.NodeIDs())
	return &messageInfo{
		msg:  &simplex.Message{Finalization: &fin},
		from: nodes.NodeIDs()[0],
	}
}

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

				b3.Digest = simplex.Digest{}
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

				var epoch3Nodes = simplex.Nodes{
					{Node: simplex.NodeID{1}, Weight: 1},
					{Node: simplex.NodeID{2}, Weight: 1},
					{Node: simplex.NodeID{3}, Weight: 1},
					{Node: simplex.NodeID{4}, Weight: 1},
					{Node: simplex.NodeID{5}, Weight: 1},
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
			nv := newTestNonValidator(t, TestConfig{storage: tc, nodes: testNodes})

			for _, m := range msgs {
				m.send(nv)
			}

			tc.WaitForBlockCommit(tt.expectedHeight - 1)
			require.Equal(t, tt.expectedHeight, nv.Storage.NumBlocks())
		})
	}
}

// TestHandleMessages_DuplicateBlock tests that when a duplicate block is received, the block is verify & indexed only once
func TestHandleMessages_DuplicateBlock(t *testing.T) {
	tc := newSeededChain(t, testNodes, 2)
	nv := newTestNonValidator(t, TestConfig{storage: tc, nodes: testNodes})

	// Send the sealing block + finalization that transitions to epoch 3.
	b3 := tc.appendBlock()
	blockMsg(t, b3, testNodes).send(nv)
	finalizationMsg(t, b3, testNodes).send(nv)

	tc.WaitForBlockCommit(3)

	// Storage will panic if we try indexing the same block twice
	blockMsg(t, b3, testNodes).send(nv)
	finalizationMsg(t, b3, testNodes).send(nv)
}

// Epoch 100
// Epoch 50
// Epoch 20
// Epoch 1
// We want to replicate all seqs 0-100
// Maybe some messages randomly get dropped
func TestNonValidator_RequestHighestEpochOnStart(t *testing.T) {
	responder := newTestResponder(t, testNodes, testNodes.NodeIDs()[0])
	responder.initializeStorage(1, 3)

	nv := newTestNonValidator(t, TestConfig{nodes: testNodes, comm: responder})
	nv.Start()
	defer nv.Stop()

	msg, ok := responder.popResponse()
	require.True(t, ok)
	require.NotNil(t, msg.msg.ReplicationResponse)
	require.NotNil(t, msg.msg.ReplicationResponse.LatestSeq)
}

func TestNonValidator_ReplicatesEpochs(t *testing.T) {
	responder := newTestResponder(t, testNodes, testNodes.NodeIDs()[0])
	responder.initializeStorage(1, 10, 20, 30, 40, 43, 48)

	nv := newTestNonValidator(t, TestConfig{nodes: testNodes, comm: responder, sigCreator: responder.storage.signatureAggregatorCreator})
	nv.Start()
	defer nv.Stop()

	for msg, ok := responder.popResponse(); ok; {
		msg.send(nv)
		msg, ok = responder.popResponse()
	}
}

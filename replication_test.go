// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"bytes"
	"context"
	"fmt"
	"go.uber.org/zap/zapcore"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/record"
	"github.com/ava-labs/simplex/testutil"
	"github.com/ava-labs/simplex/wal"

	"github.com/stretchr/testify/require"
)

// TestReplication tests the replication process of a node that
// is behind the rest of the network by less than maxRoundWindow.
func TestBasicReplication(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}

	for i := range 3 * simplex.DefaultMaxRoundWindow {
		testName := fmt.Sprintf("Basic replication_of_%d_blocks", i)

		// lagging node cannot be the leader after node disconnects
		isLaggingNodeLeader := bytes.Equal(simplex.LeaderForRound(nodes, uint64(i)), nodes[3])
		if isLaggingNodeLeader {
			continue
		}

		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			testReplication(t, uint64(i), nodes)
		})
	}
}

func testReplication(t *testing.T, startSeq uint64, nodes []simplex.NodeID) {
	bb := newTestControlledBlockBuilder(t)
	net := newInMemNetwork(t, nodes)

	// initiate a network with 4 nodes. one node is behind by startSeq blocks
	storageData := createBlocks(t, nodes, &bb.testBlockBuilder, startSeq)
	testEpochConfig := &testNodeConfig{
		initialStorage:     storageData,
		replicationEnabled: true,
	}
	normalNode1 := newSimplexNode(t, nodes[0], net, bb, testEpochConfig)
	normalNode2 := newSimplexNode(t, nodes[1], net, bb, testEpochConfig)
	normalNode3 := newSimplexNode(t, nodes[2], net, bb, testEpochConfig)
	laggingNode := newSimplexNode(t, nodes[3], net, bb, &testNodeConfig{
		replicationEnabled: true,
	})

	require.Equal(t, startSeq, normalNode1.storage.NumBlocks())
	require.Equal(t, startSeq, normalNode2.storage.NumBlocks())
	require.Equal(t, startSeq, normalNode3.storage.NumBlocks())
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())

	net.startInstances()
	bb.triggerNewBlock()

	// all blocks except the lagging node start at round 8, seq 8.
	// lagging node starts at round 0, seq 0.
	// this asserts that the lagging node catches up to the latest round
	for i := 0; i <= int(startSeq); i++ {
		for _, n := range net.instances {
			n.storage.waitForBlockCommit(uint64(startSeq))
		}
	}
}

// TestReplicationAdversarialNode tests the replication process of a node that
// has been sent a different block by one node, however the rest of the network
// notarizes a different block for the same round
func TestReplicationAdversarialNode(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	quorum := simplex.Quorum(len(nodes))
	bb := newTestControlledBlockBuilder(t)
	net := newInMemNetwork(t, nodes)

	testEpochConfig := &testNodeConfig{
		replicationEnabled: true,
	}

	// doubleBlockProposalNode will propose two blocks for the same round
	doubleBlockProposalNode := newSimplexNode(t, nodes[0], net, bb, testEpochConfig)
	normalNode2 := newSimplexNode(t, nodes[1], net, bb, testEpochConfig)
	normalNode3 := newSimplexNode(t, nodes[2], net, bb, testEpochConfig)
	laggingNode := newSimplexNode(t, nodes[3], net, bb, &testNodeConfig{
		replicationEnabled: true,
	})

	require.Equal(t, uint64(0), doubleBlockProposalNode.storage.NumBlocks())
	require.Equal(t, uint64(0), normalNode2.storage.NumBlocks())
	require.Equal(t, uint64(0), normalNode3.storage.NumBlocks())
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())

	net.startInstances()
	doubleBlock := newTestBlock(doubleBlockProposalNode.e.Metadata())
	doubleBlockVote, err := newTestVote(doubleBlock, doubleBlockProposalNode.e.ID)
	require.NoError(t, err)
	msg := &simplex.Message{
		BlockMessage: &simplex.BlockMessage{
			Block: doubleBlock,
			Vote:  *doubleBlockVote,
		},
	}

	laggingNode.e.HandleMessage(msg, doubleBlockProposalNode.e.ID)
	net.Disconnect(laggingNode.e.ID)

	blocks := []simplex.VerifiedBlock{}
	for i := range 2 {
		bb.triggerNewBlock()
		block := <-bb.out
		blocks = append(blocks, block)
		for _, n := range net.instances[:3] {
			commited := n.storage.waitForBlockCommit(uint64(i))
			require.Equal(t, block, commited.(*testBlock))
		}
	}

	// lagging node should not have commited the block
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())
	require.Equal(t, uint64(0), laggingNode.e.Metadata().Round)
	net.Connect(laggingNode.e.ID)

	finalization, _ := newFinalizationRecord(t, laggingNode.e.Logger, laggingNode.e.SignatureAggregator, blocks[1], nodes[:quorum])
	finalizationMsg := &simplex.Message{
		Finalization: &finalization,
	}
	laggingNode.e.HandleMessage(finalizationMsg, doubleBlockProposalNode.e.ID)

	for i := range 2 {
		lagBlock := laggingNode.storage.waitForBlockCommit(uint64(i))
		require.Equal(t, blocks[i], lagBlock)
	}
}

// TestRebroadcastingWithReplication verifies that after network recovery,
// a lagging node and the rest of the network correctly propagate missing
// finalizations and index all blocks.
func TestRebroadcastingWithReplication(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	bb := newTestControlledBlockBuilder(t)
	net := newInMemNetwork(t, nodes)

	newNodeConfig := func(from simplex.NodeID) *testNodeConfig {
		comm := newTestComm(from, net, allowAllMessages)
		return &testNodeConfig{
			comm:               comm,
			replicationEnabled: true,
		}
	}

	newSimplexNode(t, nodes[0], net, bb, newNodeConfig(nodes[0]))
	newSimplexNode(t, nodes[1], net, bb, newNodeConfig(nodes[1]))
	newSimplexNode(t, nodes[2], net, bb, newNodeConfig(nodes[2]))
	// we do not expect the lagging node to build any blocks
	laggingBb := newTestControlledBlockBuilder(t)
	laggingNode := newSimplexNode(t, nodes[3], net, laggingBb, newNodeConfig(nodes[3]))

	for _, n := range net.instances {
		require.Equal(t, uint64(0), n.storage.NumBlocks())
	}

	epochTimes := make([]time.Time, 0, len(nodes))
	for _, n := range net.instances {
		epochTimes = append(epochTimes, n.e.StartTime)
	}

	net.startInstances()

	net.Disconnect(laggingNode.e.ID)
	numNotarizations := 9
	missedSeqs := uint64(0)

	// finalization for the first block
	bb.triggerNewBlock()
	for _, n := range net.instances {
		if n.e.ID.Equals(laggingNode.e.ID) {
			continue
		}
		n.storage.waitForBlockCommit(0)
	}

	net.setAllNodesMessageFilter(denyFinalizationMessages)
	// normal nodes continue to make progress
	for i := uint64(1); i < uint64(numNotarizations); i++ {
		emptyRound := bytes.Equal(simplex.LeaderForRound(nodes, i), laggingNode.e.ID)
		if emptyRound {
			advanceWithoutLeader(t, net, bb, epochTimes, i, laggingNode.e.ID)
			missedSeqs++
		} else {
			bb.triggerNewBlock()
			for _, n := range net.instances {
				if n.e.ID.Equals(laggingNode.e.ID) {
					continue
				}
				n.wal.assertNotarization(i)
			}
		}
	}

	for _, n := range net.instances {
		if n.e.ID.Equals(laggingNode.e.ID) {
			require.Equal(t, uint64(0), n.storage.NumBlocks())
			require.Equal(t, uint64(0), n.e.Metadata().Round)
			continue
		}

		// assert metadata
		require.Equal(t, uint64(numNotarizations), n.e.Metadata().Round)
		require.Equal(t, uint64(1), n.e.Storage.NumBlocks())
	}

	net.setAllNodesMessageFilter(allowAllMessages)
	net.Connect(laggingNode.e.ID)
	bb.triggerNewBlock()
	block := <-bb.out

	for i := 0; i <= int(block.metadata.Seq); i++ {
		for _, n := range net.instances {
			n.storage.waitForBlockCommit(uint64(i))
		}
	}
}

// TestReplicationEmptyNotarizations ensures a lagging node will properly replicate
// many empty notarizations in a row.
func TestReplicationEmptyNotarizations(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}, {5}, {6}}

	for endRound := uint64(2); endRound <= 2*simplex.DefaultMaxRoundWindow; endRound++ {
		isLaggingNodeLeader := bytes.Equal(simplex.LeaderForRound(nodes, endRound), nodes[5])
		if isLaggingNodeLeader {
			continue
		}

		testName := fmt.Sprintf("Empty_notarizations_end_round%d", endRound)
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			testReplicationEmptyNotarizations(t, nodes, endRound)
		})
	}
}

func testReplicationEmptyNotarizations(t *testing.T, nodes []simplex.NodeID, endRound uint64) {
	bb := newTestControlledBlockBuilder(t)
	laggingBb := newTestControlledBlockBuilder(t)
	net := newInMemNetwork(t, nodes)
	newNodeConfig := func(from simplex.NodeID) *testNodeConfig {
		comm := newTestComm(from, net, allowAllMessages)
		return &testNodeConfig{
			comm:               comm,
			replicationEnabled: true,
		}
	}

	startTimes := make([]time.Time, 0, len(nodes))
	newSimplexNode(t, nodes[0], net, bb, newNodeConfig(nodes[0]))
	newSimplexNode(t, nodes[1], net, bb, newNodeConfig(nodes[1]))
	newSimplexNode(t, nodes[2], net, bb, newNodeConfig(nodes[2]))
	newSimplexNode(t, nodes[3], net, bb, newNodeConfig(nodes[3]))
	newSimplexNode(t, nodes[4], net, bb, newNodeConfig(nodes[4]))
	laggingNode := newSimplexNode(t, nodes[5], net, laggingBb, newNodeConfig(nodes[5]))

	for _, n := range net.instances {
		require.Equal(t, uint64(0), n.storage.NumBlocks())
		startTimes = append(startTimes, n.e.StartTime)
	}

	net.startInstances()

	net.Disconnect(laggingNode.e.ID)

	bb.triggerNewBlock()
	for _, n := range net.instances {
		if n.e.ID.Equals(laggingNode.e.ID) {
			continue
		}
		n.storage.waitForBlockCommit(0)
	}

	net.setAllNodesMessageFilter(onlyAllowEmptyRoundMessages)

	// normal nodes continue to make progress
	for i := uint64(1); i < endRound; i++ {
		leader := simplex.LeaderForRound(nodes, i)
		if !leader.Equals(laggingNode.e.ID) {
			bb.triggerNewBlock()
		}

		advanceWithoutLeader(t, net, bb, startTimes, i, laggingNode.e.ID)
	}

	for _, n := range net.instances {
		if n.e.ID.Equals(laggingNode.e.ID) {
			require.Equal(t, uint64(0), n.storage.NumBlocks())
			require.Equal(t, uint64(0), n.e.Metadata().Round)
			continue
		}

		// assert metadata
		require.Equal(t, uint64(endRound), n.e.Metadata().Round)
		require.Equal(t, uint64(1), n.e.Metadata().Seq)
		require.Equal(t, uint64(1), n.e.Storage.NumBlocks())
	}

	net.setAllNodesMessageFilter(allowAllMessages)
	net.Connect(laggingNode.e.ID)
	bb.triggerNewBlock()
	for _, n := range net.instances {
		n.storage.waitForBlockCommit(1)
	}

	require.Equal(t, uint64(2), laggingNode.storage.NumBlocks())
	require.Equal(t, uint64(endRound+1), laggingNode.e.Metadata().Round)
	require.Equal(t, uint64(2), laggingNode.e.Metadata().Seq)
}

// TestReplicationStartsBeforeCurrentRound tests the replication process of a node that
// starts replicating in the middle of the current round.
func TestReplicationStartsBeforeCurrentRound(t *testing.T) {
	bb := newTestControlledBlockBuilder(t)
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	quorum := simplex.Quorum(len(nodes))
	net := newInMemNetwork(t, nodes)
	startSeq := uint64(simplex.DefaultMaxRoundWindow + 3)
	storageData := createBlocks(t, nodes, &bb.testBlockBuilder, startSeq)
	testEpochConfig := &testNodeConfig{
		initialStorage:     storageData,
		replicationEnabled: true,
	}
	normalNode1 := newSimplexNode(t, nodes[0], net, bb, testEpochConfig)
	normalNode2 := newSimplexNode(t, nodes[1], net, bb, testEpochConfig)
	normalNode3 := newSimplexNode(t, nodes[2], net, bb, testEpochConfig)
	laggingNode := newSimplexNode(t, nodes[3], net, bb, &testNodeConfig{
		replicationEnabled: true,
	})

	firstBlock := storageData[0].VerifiedBlock
	fBytes, err := firstBlock.Bytes()
	require.NoError(t, err)
	record := simplex.BlockRecord(firstBlock.BlockHeader(), fBytes)
	laggingNode.wal.Append(record)

	firstNotarizationRecord, err := newNotarizationRecord(laggingNode.e.Logger, laggingNode.e.SignatureAggregator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	laggingNode.wal.Append(firstNotarizationRecord)

	secondBlock := storageData[1].VerifiedBlock
	sBytes, err := secondBlock.Bytes()
	require.NoError(t, err)
	record = simplex.BlockRecord(secondBlock.BlockHeader(), sBytes)
	laggingNode.wal.Append(record)

	secondNotarizationRecord, err := newNotarizationRecord(laggingNode.e.Logger, laggingNode.e.SignatureAggregator, secondBlock, nodes[0:quorum])
	require.NoError(t, err)
	laggingNode.wal.Append(secondNotarizationRecord)

	require.Equal(t, startSeq, normalNode1.storage.NumBlocks())
	require.Equal(t, startSeq, normalNode2.storage.NumBlocks())
	require.Equal(t, startSeq, normalNode3.storage.NumBlocks())
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())

	net.startInstances()

	laggingNodeMd := laggingNode.e.Metadata()
	require.Equal(t, uint64(2), laggingNodeMd.Round)

	bb.triggerNewBlock()
	for i := 0; i <= int(startSeq); i++ {
		for _, n := range net.instances {
			n.storage.waitForBlockCommit(uint64(startSeq))
		}
	}
}

func TestReplicationFutureFinalization(t *testing.T) {
	// send a block, then simultaneously send a finalization for the block
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	quorum := simplex.Quorum(len(nodes))
	signatureAggregator := &testSignatureAggregator{}
	conf := simplex.EpochConfig{
		MaxProposalWait:     simplex.DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[1],
		Signer:              &testSigner{},
		WAL:                 wal.NewMemWAL(t),
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: signatureAggregator,
	}

	e, err := simplex.NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	require.Equal(t, md.Round, md.Seq)

	block := <-bb.out
	block.verificationDelay = make(chan struct{}) // add a delay to the block verification

	vote, err := newTestVote(block, nodes[0])
	require.NoError(t, err)

	err = e.HandleMessage(&simplex.Message{
		BlockMessage: &simplex.BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[0])
	require.NoError(t, err)

	finalization, _ := newFinalizationRecord(t, l, signatureAggregator, block, nodes[0:quorum])
	// send finalization
	err = e.HandleMessage(&simplex.Message{
		Finalization: &finalization,
	}, nodes[0])
	require.NoError(t, err)

	block.verificationDelay <- struct{}{} // unblock the block verification

	storedBlock := storage.waitForBlockCommit(0)
	require.Equal(t, uint64(1), storage.NumBlocks())
	require.Equal(t, block, storedBlock)
}

// TestReplicationAfterNodeDisconnects tests the replication process of a node that
// disconnects from the network and reconnects after the rest of the network has made progress.
//
// All nodes make progress for `startDisconnect` blocks. The lagging node disconnects
// and the rest of the nodes continue to make progress for another `endDisconnect - startDisconnect` blocks.
// The lagging node reconnects and the after the next `finalization` is sent, the lagging node catches up to the latest height.
func TestReplicationAfterNodeDisconnects(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}

	for startDisconnect := uint64(0); startDisconnect <= 5; startDisconnect++ {
		for endDisconnect := uint64(10); endDisconnect <= 20; endDisconnect++ {
			// lagging node cannot be the leader after node disconnects
			isLaggingNodeLeader := bytes.Equal(simplex.LeaderForRound(nodes, endDisconnect), nodes[3])
			if isLaggingNodeLeader {
				continue
			}

			testName := fmt.Sprintf("Disconnect_%d_to_%d", startDisconnect, endDisconnect)

			t.Run(testName, func(t *testing.T) {
				t.Parallel()
				testReplicationAfterNodeDisconnects(t, nodes, startDisconnect, endDisconnect)
			})
		}
	}
}

func testReplicationAfterNodeDisconnects(t *testing.T, nodes []simplex.NodeID, startDisconnect, endDisconnect uint64) {
	bb := newTestControlledBlockBuilder(t)
	laggingBb := newTestControlledBlockBuilder(t)
	net := newInMemNetwork(t, nodes)
	testConfig := &testNodeConfig{
		replicationEnabled: true,
	}
	normalNode1 := newSimplexNode(t, nodes[0], net, bb, testConfig)
	normalNode2 := newSimplexNode(t, nodes[1], net, bb, testConfig)
	normalNode3 := newSimplexNode(t, nodes[2], net, bb, testConfig)
	laggingNode := newSimplexNode(t, nodes[3], net, laggingBb, testConfig)

	require.Equal(t, uint64(0), normalNode1.storage.NumBlocks())
	require.Equal(t, uint64(0), normalNode2.storage.NumBlocks())
	require.Equal(t, uint64(0), normalNode3.storage.NumBlocks())
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())

	epochTimes := make([]time.Time, 0, 4)
	for _, n := range net.instances {
		epochTimes = append(epochTimes, n.e.StartTime)
	}

	net.startInstances()

	for i := uint64(0); i < startDisconnect; i++ {
		if bytes.Equal(simplex.LeaderForRound(nodes, i), nodes[3]) {
			laggingBb.triggerNewBlock()
		} else {
			bb.triggerNewBlock()
		}
		for _, n := range net.instances {
			n.storage.waitForBlockCommit(i)
		}
	}

	// all nodes have commited `startDisconnect` blocks
	for _, n := range net.instances {
		require.Equal(t, startDisconnect, n.storage.NumBlocks())
	}

	// lagging node disconnects
	net.Disconnect(nodes[3])

	isLaggingNodeLeader := bytes.Equal(simplex.LeaderForRound(nodes, startDisconnect), nodes[3])
	if isLaggingNodeLeader {
		laggingBb.triggerNewBlock()
	}

	missedSeqs := uint64(0)
	// normal nodes continue to make progress
	for i := startDisconnect; i < endDisconnect; i++ {
		emptyRound := bytes.Equal(simplex.LeaderForRound(nodes, i), nodes[3])
		if emptyRound {
			advanceWithoutLeader(t, net, bb, epochTimes, i, laggingNode.e.ID)
			missedSeqs++
		} else {
			bb.triggerNewBlock()
			for _, n := range net.instances[:3] {
				n.storage.waitForBlockCommit(i - missedSeqs)
			}
		}
	}
	// all nodes excpet for lagging node have progressed and commited [endDisconnect - missedSeqs] blocks
	for _, n := range net.instances[:3] {
		require.Equal(t, endDisconnect-missedSeqs, n.storage.NumBlocks())
	}
	require.Equal(t, startDisconnect, laggingNode.storage.NumBlocks())
	require.Equal(t, startDisconnect, laggingNode.e.Metadata().Round)
	// lagging node reconnects
	net.Connect(nodes[3])
	bb.triggerNewBlock()
	for _, n := range net.instances {
		n.storage.waitForBlockCommit(endDisconnect - missedSeqs)
	}

	for _, n := range net.instances {
		require.Equal(t, endDisconnect-missedSeqs, n.storage.NumBlocks()-1)
		require.Equal(t, endDisconnect+1, n.e.Metadata().Round)
	}

	// the lagging node should build a block when triggered if its the leader
	if bytes.Equal(simplex.LeaderForRound(nodes, endDisconnect+1), nodes[3]) {
		laggingBb.triggerNewBlock()
	} else {
		bb.triggerNewBlock()
	}

	for _, n := range net.instances {
		n.storage.waitForBlockCommit(endDisconnect - missedSeqs + 1)
	}
}

func onlyAllowBlockProposalsAndNotarizations(msg *simplex.Message, _, to simplex.NodeID) bool {
	// TODO: remove hardcoded node id
	if to.Equals(simplex.NodeID{4}) {
		return (msg.BlockMessage != nil || msg.VerifiedBlockMessage != nil || msg.Notarization != nil)
	}

	return true
}

// sendVotesToOneNode allows block messages to be sent to all nodes, and only
// passes vote messages to one node. This will allows that node to notarize the block,
// while the other blocks will timeout
func sendVotesToOneNode(filteredInNode simplex.NodeID) messageFilter {
	return func(msg *simplex.Message, _, to simplex.NodeID) bool {
		if msg.VerifiedBlockMessage != nil || msg.BlockMessage != nil {
			return true
		}

		if msg.VoteMessage != nil {
			// this is the lagging node
			if to.Equals(filteredInNode) {
				return true
			}
		}

		return false
	}
}

func TestReplicationStuckInProposingBlock(t *testing.T) {
	var aboutToBuildBlock sync.WaitGroup
	aboutToBuildBlock.Add(2)

	var cancelBlockBuilding sync.WaitGroup
	cancelBlockBuilding.Add(1)

	l := testutil.MakeLogger(t, 1)
	l.Intercept(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Scheduling block building") {
			aboutToBuildBlock.Done()
		}
		if strings.Contains(entry.Message, "We are the leader of this round, but a higher round has been finalized") {
			cancelBlockBuilding.Done()
		}
		return nil
	})
	tempBB := &testBlockBuilder{out: make(chan *testBlock, 4), blockShouldBeBuilt: make(chan struct{}, 1), in: make(chan *testBlock, 1)}
	tbb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1), in: make(chan *testBlock, 1)}
	bb := newTestControlledBlockBuilder(t)
	bb.testBlockBuilder = *tbb
	storage := newInMemStorage()
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	blocks := createBlocks(t, nodes, tempBB, 5)

	wal := newTestWAL(t)

	quorum := simplex.Quorum(len(nodes))
	signatureAggregator := &testSignatureAggregator{}
	sentMessages := make(chan *simplex.Message, 100)
	conf := simplex.EpochConfig{
		MaxProposalWait: simplex.DefaultMaxProposalWaitTime,
		Logger:          l,
		ID:              nodes[0],
		Signer:          &testSigner{},
		WAL:             wal,
		Verifier:        &testVerifier{},
		Storage:         storage,
		Comm: &recordingComm{
			Communication: noopComm(nodes),
			SentMessages:  sentMessages,
		},
		BlockBuilder:        bb,
		SignatureAggregator: signatureAggregator,
		ReplicationEnabled:  true,
	}

	e, err := simplex.NewEpoch(conf)
	e.ReplicationEnabled = true
	require.NoError(t, err)
	require.NoError(t, e.Start())

	bb.in <- blocks[0].VerifiedBlock.(*testBlock)
	bb.out <- blocks[0].VerifiedBlock.(*testBlock)

	bb.triggerNewBlock()
	notarizeAndFinalizeRound(t, e, &bb.testBlockBuilder)

	gb := storage.waitForBlockCommit(0)
	require.Equal(t, gb, blocks[0].VerifiedBlock.(*testBlock))

	highBlock, _ := blocks[3].VerifiedBlock.(*testBlock)

	highFinalization, _ := newFinalizationRecord(t, l, signatureAggregator, highBlock, nodes[0:quorum])

	// Trigger the replication process to start by sending a finalization for a block we do not have
	e.HandleMessage(&simplex.Message{
		Finalization: &highFinalization,
	}, nodes[1])

	// Wait for the replication request to be sent
	for {
		msg := <-sentMessages
		if msg.ReplicationRequest != nil {
			break
		}
	}

	// Drain the block builder channels
	for len(bb.testBlockBuilder.blockShouldBeBuilt) > 0 && len(bb.out) > 0 {
		select {
		case <-bb.blockShouldBeBuilt:
		default:
		}
		select {
		case <-bb.out:
		default:
		}
	}

	// Prepare the quorum round answer to be sent as a response to the replication request
	quorumRounds := make([]simplex.QuorumRound, 0, 4)
	for i := uint64(1); i <= 4; i++ {
		tb := blocks[i].VerifiedBlock.(*testBlock)
		finalization := blocks[i].Finalization
		quorumRounds = append(quorumRounds, simplex.QuorumRound{
			Block:        tb,
			Finalization: &finalization,
		})
	}

	// Respond to the replication request with a block that has a notarization
	replicationResponse := &simplex.ReplicationResponse{
		LatestRound: &quorumRounds[2],
		Data:        quorumRounds[:3],
	}

	e.HandleMessage(&simplex.Message{
		ReplicationResponse: replicationResponse,
	}, nodes[1])

	// Wait for the second block to be attempted to be built
	aboutToBuildBlock.Wait()

	// Trigger the replication process to start by sending a finalization for a block we do not have
	e.HandleMessage(&simplex.Message{
		Finalization: &blocks[4].Finalization,
	}, nodes[1])

	// Wait for the replication request to be sent
	for {
		msg := <-sentMessages
		if msg.ReplicationRequest != nil {
			break
		}
	}

	replicationResponse = &simplex.ReplicationResponse{
		LatestRound: &quorumRounds[3],
		Data:        quorumRounds[3:],
	}

	e.HandleMessage(&simplex.Message{
		ReplicationResponse: replicationResponse,
	}, nodes[1])

	storage.waitForBlockCommit(4)

	// Just for sanity, ensure that the block building was cancelled
	cancelBlockBuilding.Wait()
}

// TestReplicationNodeDiverges tests that a node replicates blocks even if they
// have a stale notarization for a round(i.e. a node notarized a block but the rest of the network
// propagated an empty notarization).
func TestReplicationNodeDiverges(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}, {5}, {6}}
	numBlocks := uint64(5)

	bb := newTestControlledBlockBuilder(t)
	laggingNodeBb := newTestControlledBlockBuilder(t)
	net := newInMemNetwork(t, nodes)

	nodeConfig := func(from simplex.NodeID) *testNodeConfig {
		comm := newTestComm(from, net, sendVotesToOneNode(nodes[3]))
		return &testNodeConfig{
			comm:               comm,
			replicationEnabled: true,
		}
	}

	newSimplexNode(t, nodes[0], net, bb, nodeConfig(nodes[0]))
	newSimplexNode(t, nodes[1], net, bb, nodeConfig(nodes[1]))
	newSimplexNode(t, nodes[2], net, bb, nodeConfig(nodes[2]))
	laggingNode := newSimplexNode(t, nodes[3], net, laggingNodeBb, nodeConfig(nodes[3]))

	// we need at least 6 nodes since the lagging node & leader will not timeout
	newSimplexNode(t, nodes[4], net, bb, nodeConfig(nodes[4]))
	newSimplexNode(t, nodes[5], net, bb, nodeConfig(nodes[5]))

	startTimes := make([]time.Time, 0, len(nodes))
	for _, n := range net.instances {
		require.Equal(t, uint64(0), n.storage.NumBlocks())
		startTimes = append(startTimes, n.e.StartTime)
	}

	net.startInstances()
	bb.triggerNewBlock()

	// because of the message filter, the lagging one will be the only one to notarize the block
	laggingNode.wal.assertNotarization(0)
	for _, n := range net.instances {
		if n.e.ID.Equals(laggingNode.e.ID) {
			continue
		}
		require.Equal(t, false, n.wal.containsNotarization(0))
	}

	// we disconnect lagging node first so that it doesn't send the notarized block to any other nodes
	net.Disconnect(laggingNode.e.ID)
	net.setAllNodesMessageFilter(
		// block sending votes from round 0 to ensure all nodes will timeout
		func(msg *simplex.Message, _, to simplex.NodeID) bool {
			return !(msg.VoteMessage != nil && msg.VoteMessage.Vote.Round == 0)
		},
	)

	// This function call ensures all nodes will timeout, and
	// receive an empty notarization for round 0(except for lagging).
	advanceWithoutLeader(t, net, bb, startTimes, 0, laggingNode.e.ID)

	for _, n := range net.instances {
		if n.e.ID.Equals(laggingNode.e.ID) {
			require.Equal(t, uint64(1), n.e.Metadata().Round)
			require.Equal(t, uint64(1), n.e.Metadata().Seq)
			continue
		}

		require.Equal(t, uint64(0), n.e.Metadata().Seq)
		require.Equal(t, uint64(1), n.e.Metadata().Round)
	}

	// advance [numBlocks] while the lagging node is disconnected
	missedSeqs := uint64(1) // missed the first seq
	for i := uint64(1); i < 1+numBlocks; i++ {
		emptyRound := bytes.Equal(simplex.LeaderForRound(nodes, i), laggingNode.e.ID)
		if emptyRound {
			advanceWithoutLeader(t, net, bb, startTimes, i, laggingNode.e.ID)
			missedSeqs++
		} else {
			bb.triggerNewBlock()
			for _, n := range net.instances {
				if n.e.ID.Equals(laggingNode.e.ID) {
					continue
				}
				n.storage.waitForBlockCommit(i - missedSeqs)
			}
		}
	}

	net.Connect(laggingNode.e.ID)
	bb.triggerNewBlock()
	for _, n := range net.instances {
		n.storage.waitForBlockCommit(numBlocks - missedSeqs + 1)
	}
	assertEqualLedgers(t, net)
}

func assertEqualLedgers(t *testing.T, net *inMemNetwork) {
	expectedLedger := map[uint64][]byte{}

	for seq, datum := range net.instances[0].storage.data {
		bytes, err := datum.VerifiedBlock.Bytes()
		require.NoError(t, err)
		expectedLedger[seq] = bytes
	}

	for _, n := range net.instances {
		actualLedger := map[uint64][]byte{}

		for seq, datum := range n.storage.data {
			bytes, err := datum.VerifiedBlock.Bytes()
			require.NoError(t, err)
			actualLedger[seq] = bytes
		}
		require.Equal(t, expectedLedger, actualLedger)
	}
}

func TestReplicationNotarizationWithoutFinalizations(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}

	for numBlocks := uint64(1); numBlocks <= 3*simplex.DefaultMaxRoundWindow; numBlocks++ {
		// lagging node cannot be the leader after node disconnects
		isLaggingNodeLeader := bytes.Equal(simplex.LeaderForRound(nodes, numBlocks), nodes[3])
		if isLaggingNodeLeader {
			continue
		}

		testName := fmt.Sprintf("NotarizationWithoutFinalization_%d_blocks", numBlocks)

		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			testReplicationNotarizationWithoutFinalizations(t, numBlocks, nodes)
		})
	}
}

// TestReplicationNotarizationWithoutFinalizations tests that a lagging node will replicate
// blocks that have notarizations but no finalizations.
func testReplicationNotarizationWithoutFinalizations(t *testing.T, numBlocks uint64, nodes []simplex.NodeID) {
	bb := newTestControlledBlockBuilder(t)
	net := newInMemNetwork(t, nodes)

	nodeConfig := func(from simplex.NodeID) *testNodeConfig {
		comm := newTestComm(from, net, onlyAllowBlockProposalsAndNotarizations)
		return &testNodeConfig{
			comm:               comm,
			replicationEnabled: true,
		}
	}

	newSimplexNode(t, nodes[0], net, bb, nodeConfig(nodes[0]))
	newSimplexNode(t, nodes[1], net, bb, nodeConfig(nodes[1]))
	newSimplexNode(t, nodes[2], net, bb, nodeConfig(nodes[2]))

	laggingNode := newSimplexNode(t, nodes[3], net, bb, nodeConfig(nodes[3]))

	for _, n := range net.instances {
		require.Equal(t, uint64(0), n.storage.NumBlocks())
	}

	net.startInstances()

	// normal nodes continue to make progress
	for i := uint64(0); i < uint64(numBlocks); i++ {
		bb.triggerNewBlock()
		for _, n := range net.instances[:3] {
			n.storage.waitForBlockCommit(uint64(i))
		}

	}

	laggingNode.wal.assertNotarization(numBlocks - 1)
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())
	require.Equal(t, uint64(numBlocks), laggingNode.e.Metadata().Round)

	net.setAllNodesMessageFilter(allowAllMessages)
	bb.triggerNewBlock()
	for _, n := range net.instances {
		n.storage.waitForBlockCommit(uint64(numBlocks))
	}
}

func waitToEnterRound(t *testing.T, e *simplex.Epoch, round uint64) {
	timeout := time.NewTimer(time.Minute)
	defer timeout.Stop()

	for {
		if e.Metadata().Round >= round {
			return
		}

		select {
		case <-time.After(time.Millisecond * 10):
			continue
		case <-timeout.C:
			require.Fail(t, "timed out waiting for event")
		}
	}
}

func advanceWithoutLeader(t *testing.T, net *inMemNetwork, bb *testControlledBlockBuilder, epochTimes []time.Time, round uint64, laggingNodeId simplex.NodeID) {
	// we need to ensure all blocks are waiting for the channel before proceeding
	// otherwise, we may send to a channel that is not ready to receive
	for _, n := range net.instances {
		if laggingNodeId.Equals(n.e.ID) {
			continue
		}

		waitToEnterRound(t, n.e, round)
	}

	for _, n := range net.instances {
		leader := n.e.ID.Equals(simplex.LeaderForRound(net.nodes, n.e.Metadata().Round))
		if leader || laggingNodeId.Equals(n.e.ID) {
			continue
		}
		bb.blockShouldBeBuilt <- struct{}{}
	}

	for i, n := range net.instances {
		// the leader will not write an empty vote to the wal
		// because it cannot both propose a block & send an empty vote in the same round
		leader := n.e.ID.Equals(simplex.LeaderForRound(net.nodes, n.e.Metadata().Round))
		if leader || laggingNodeId.Equals(n.e.ID) {
			continue
		}
		waitForBlockProposerTimeout(t, n.e, &epochTimes[i], round)
	}

	for _, n := range net.instances {
		if laggingNodeId.Equals(n.e.ID) {
			continue
		}
		recordType := n.wal.assertNotarization(round)
		require.Equal(t, record.EmptyNotarizationRecordType, recordType)
	}
}

func createBlocks(t *testing.T, nodes []simplex.NodeID, bb simplex.BlockBuilder, seqCount uint64) []simplex.VerifiedFinalizedBlock {
	logger := testutil.MakeLogger(t, int(0))
	ctx := context.Background()
	data := make([]simplex.VerifiedFinalizedBlock, 0, seqCount)
	var prev simplex.Digest
	for i := uint64(0); i < seqCount; i++ {
		protocolMetadata := simplex.ProtocolMetadata{
			Seq:   i,
			Round: i,
			Prev:  prev,
		}

		block, ok := bb.BuildBlock(ctx, protocolMetadata)
		require.True(t, ok)
		prev = block.BlockHeader().Digest
		finalization, _ := newFinalizationRecord(t, logger, &testSignatureAggregator{}, block, nodes)
		data = append(data, simplex.VerifiedFinalizedBlock{
			VerifiedBlock: block,
			Finalization:  finalization,
		})
	}
	return data
}

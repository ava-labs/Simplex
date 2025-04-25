// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"bytes"
	"context"
	"fmt"
	"simplex"
	"simplex/testutil"
	"simplex/wal"
	"testing"
	"time"

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

	require.Equal(t, startSeq, normalNode1.storage.Height())
	require.Equal(t, startSeq, normalNode2.storage.Height())
	require.Equal(t, startSeq, normalNode3.storage.Height())
	require.Equal(t, uint64(0), laggingNode.storage.Height())

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

	require.Equal(t, uint64(0), doubleBlockProposalNode.storage.Height())
	require.Equal(t, uint64(0), normalNode2.storage.Height())
	require.Equal(t, uint64(0), normalNode3.storage.Height())
	require.Equal(t, uint64(0), laggingNode.storage.Height())

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
	require.Equal(t, uint64(0), laggingNode.storage.Height())
	require.Equal(t, uint64(0), laggingNode.e.Metadata().Round)
	net.Connect(laggingNode.e.ID)

	fCert, _ := newFinalizationRecord(t, laggingNode.e.Logger, laggingNode.e.SignatureAggregator, blocks[1], nodes[:quorum])
	fCertMsg := &simplex.Message{
		FinalizationCertificate: &fCert,
	}
	laggingNode.e.HandleMessage(fCertMsg, doubleBlockProposalNode.e.ID)

	for i := range 2 {
		lagBlock := laggingNode.storage.waitForBlockCommit(uint64(i))
		require.Equal(t, blocks[i], lagBlock)
	}
}

// TestReplicationNotarizations tests that a lagging node also replicates
// notarizations after lagging behind.
func TestReplicationNotarizations(t *testing.T) {
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
		require.Equal(t, uint64(0), n.storage.Height())
	}

	epochTimes := make([]time.Time, 0, len(nodes))
	for _, n := range net.instances {
		epochTimes = append(epochTimes, n.e.StartTime)
	}

	net.startInstances()

	net.Disconnect(laggingNode.e.ID)
	numNotarizations := 9
	missedSeqs := uint64(0)
	blocks := []simplex.VerifiedBlock{}

	// finalization for the first block
	bb.triggerNewBlock()
	block := <-bb.out
	blocks = append(blocks, block)
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
			block := <-bb.out
			blocks = append(blocks, block)
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
			require.Equal(t, uint64(0), n.storage.Height())
			require.Equal(t, uint64(0), n.e.Metadata().Round)
			continue
		}

		// assert metadata
		require.Equal(t, uint64(numNotarizations), n.e.Metadata().Round)
		require.Equal(t, uint64(1), n.e.Storage.Height())
	}

	net.setAllNodesMessageFilter(allowAllMessages)
	net.Connect(laggingNode.e.ID)
	bb.triggerNewBlock()

	// lagging node should replicate the first finalized block and subsequent notarizations
	laggingNode.storage.waitForBlockCommit(0)

	for i := 1; i < numNotarizations+1; i++ {
		for _, n := range net.instances {
			// lagging node wont have a notarization record if it was the leader
			leader := simplex.LeaderForRound(nodes, uint64(i))
			if n.e.ID.Equals(leader) && n.e.ID.Equals(nodes[3]) {
				continue
			}

			n.wal.assertNotarizationOrFinalization(uint64(i), n.e.EpochConfig.QCDeserializer)
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
		require.Equal(t, uint64(0), n.storage.Height())
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
			require.Equal(t, uint64(0), n.storage.Height())
			require.Equal(t, uint64(0), n.e.Metadata().Round)
			continue
		}

		// assert metadata
		require.Equal(t, uint64(endRound), n.e.Metadata().Round)
		require.Equal(t, uint64(1), n.e.Metadata().Seq)
		require.Equal(t, uint64(1), n.e.Storage.Height())
	}

	net.setAllNodesMessageFilter(allowAllMessages)
	net.Connect(laggingNode.e.ID)
	bb.triggerNewBlock()
	for _, n := range net.instances {
		n.storage.waitForBlockCommit(1)
	}

	require.Equal(t, uint64(2), laggingNode.storage.Height())
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
	record := simplex.BlockRecord(firstBlock.BlockHeader(), firstBlock.Bytes())
	laggingNode.wal.Append(record)

	firstNotarizationRecord, err := newNotarizationRecord(laggingNode.e.Logger, laggingNode.e.SignatureAggregator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	laggingNode.wal.Append(firstNotarizationRecord)

	secondBlock := storageData[1].VerifiedBlock
	record = simplex.BlockRecord(secondBlock.BlockHeader(), secondBlock.Bytes())
	laggingNode.wal.Append(record)

	secondNotarizationRecord, err := newNotarizationRecord(laggingNode.e.Logger, laggingNode.e.SignatureAggregator, secondBlock, nodes[0:quorum])
	require.NoError(t, err)
	laggingNode.wal.Append(secondNotarizationRecord)

	require.Equal(t, startSeq, normalNode1.storage.Height())
	require.Equal(t, startSeq, normalNode2.storage.Height())
	require.Equal(t, startSeq, normalNode3.storage.Height())
	require.Equal(t, uint64(0), laggingNode.storage.Height())

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

func TestReplicationFutureFinalizationCertificate(t *testing.T) {
	// send a block, then simultaneously send a finalization certificate for the block
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

	fCert, _ := newFinalizationRecord(t, l, signatureAggregator, block, nodes[0:quorum])
	// send fcert
	err = e.HandleMessage(&simplex.Message{
		FinalizationCertificate: &fCert,
	}, nodes[0])
	require.NoError(t, err)

	block.verificationDelay <- struct{}{} // unblock the block verification

	storedBlock := storage.waitForBlockCommit(0)
	require.Equal(t, uint64(1), storage.Height())
	require.Equal(t, block, storedBlock)
}

// TestReplicationAfterNodeDisconnects tests the replication process of a node that
// disconnects from the network and reconnects after the rest of the network has made progress.
//
// All nodes make progress for `startDisconnect` blocks. The lagging node disconnects
// and the rest of the nodes continue to make progress for another `endDisconnect - startDisconnect` blocks.
// The lagging node reconnects and the after the next `fCert` is sent, the lagging node catches up to the latest height.
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

	require.Equal(t, uint64(0), normalNode1.storage.Height())
	require.Equal(t, uint64(0), normalNode2.storage.Height())
	require.Equal(t, uint64(0), normalNode3.storage.Height())
	require.Equal(t, uint64(0), laggingNode.storage.Height())

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
		require.Equal(t, startDisconnect, n.storage.Height())
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
		require.Equal(t, endDisconnect-missedSeqs, n.storage.Height())
	}
	require.Equal(t, startDisconnect, laggingNode.storage.Height())
	require.Equal(t, startDisconnect, laggingNode.e.Metadata().Round)
	// lagging node reconnects
	net.Connect(nodes[3])
	bb.triggerNewBlock()
	for _, n := range net.instances {
		n.storage.waitForBlockCommit(endDisconnect - missedSeqs)
	}

	for _, n := range net.instances {
		require.Equal(t, endDisconnect-missedSeqs, n.storage.Height()-1)
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

func onlyAllowBlockProposalsAndNotarizations(msg *simplex.Message, from, to simplex.NodeID) bool {
	// don't send to lagging node
	if to.Equals(simplex.NodeID{4}) {
		return (msg.BlockMessage != nil || msg.VerifiedBlockMessage != nil || msg.Notarization != nil)
	}

	return true
}

func TestReplicationNotarizationWithoutFinalizations(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}

	for numBlocks := uint64(1); numBlocks <= 3*simplex.DefaultMaxRoundWindow; numBlocks++ {
		// lagging node cannot be the leader after node disconnects
		isLaggingNodeLeader := bytes.Equal(simplex.LeaderForRound(nodes, numBlocks), nodes[3])
		if isLaggingNodeLeader {
			continue
		}

		testName := fmt.Sprintf("NotarizationWithoutFCert_%d_blocks", numBlocks)

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
		require.Equal(t, uint64(0), n.storage.Height())
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
	require.Equal(t, uint64(0), laggingNode.storage.Height())
	require.Equal(t, uint64(numBlocks), laggingNode.e.Metadata().Round)
}

// sendVotesToOneNode allows block messages to sent to all nodes, and only
// passes vote messages to one node. This will allows that node to notarize the block,
// while the other blocks will timeout
func sendVotesToOneNode(msg *simplex.Message, from, to simplex.NodeID) bool {
	if msg.VerifiedBlockMessage != nil || msg.BlockMessage != nil {
		return true
	}

	if msg.VoteMessage != nil {
		// this is the lagging node
		if to.Equals(simplex.NodeID{4}) {
			return true
		}
	}

	return false
}

// TestReplicationNodeDiverges
func TestReplicationNodeDiverges(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}, {5}, {6}}
	numBlocks := uint64(5)

	bb := newTestControlledBlockBuilder(t)
	laggingNodeBb := newTestControlledBlockBuilder(t)
	net := newInMemNetwork(t, nodes)

	nodeConfig := func(from simplex.NodeID) *testNodeConfig {
		comm := newTestComm(from, net, sendVotesToOneNode)
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
		require.Equal(t, uint64(0), n.storage.Height())
		startTimes = append(startTimes, n.e.StartTime)
	}

	net.startInstances()
	bb.triggerNewBlock()
	firstBlock := <-bb.out

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
	net.setAllNodesMessageFilter(allowAllMessages)

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

	// advance [numRounds] while the lagging node is disconnected
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

	// now advance the round from a block(the lagging node will realize it is behind)
	bb.triggerNewBlock()
	for _, n := range net.instances {
		// sanity check: assert that we did not index the notarized block
		storedBlock := n.storage.waitForBlockCommit(numBlocks - missedSeqs)
		require.NotEqual(t, storedBlock, firstBlock)
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
		empty := n.wal.assertNotarization(round)
		require.True(t, empty)
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
		fCert, _ := newFinalizationRecord(t, logger, &testSignatureAggregator{}, block, nodes)
		data = append(data, simplex.VerifiedFinalizedBlock{
			VerifiedBlock: block,
			FCert:         fCert,
		})
	}
	return data
}

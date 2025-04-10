// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"bytes"
	"context"
	"fmt"
	"simplex"
	"simplex/testutil"
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
	bb := testutil.NewTestControlledBlockBuilder(t)
	net := testutil.NewInMemNetwork(t, nodes)

	// initiate a network with 4 nodes. one node is behind by 8 blocks
	storageData := createBlocks(t, nodes, &bb.TestBlockBuilder, startSeq)
	testEpochConfig := &testutil.TestNodeConfig{
		InitialStorage:     storageData,
		ReplicationEnabled: true,
	}
	normalNode1 := testutil.NewSimplexNode(t, nodes[0], net, bb, testEpochConfig)
	normalNode2 := testutil.NewSimplexNode(t, nodes[1], net, bb, testEpochConfig)
	normalNode3 := testutil.NewSimplexNode(t, nodes[2], net, bb, testEpochConfig)
	laggingNode := testutil.NewSimplexNode(t, nodes[3], net, bb, &testutil.TestNodeConfig{
		ReplicationEnabled: true,
	})

	require.Equal(t, startSeq, normalNode1.Storage.Height())
	require.Equal(t, startSeq, normalNode2.Storage.Height())
	require.Equal(t, startSeq, normalNode3.Storage.Height())
	require.Equal(t, uint64(0), laggingNode.Storage.Height())

	net.StartInstances()
	bb.TriggerNewBlock()

	// all blocks except the lagging node start at round 8, seq 8.
	// lagging node starts at round 0, seq 0.
	// this asserts that the lagging node catches up to the latest round
	for i := 0; i <= int(startSeq); i++ {
		for _, n := range net.Instances() {
			n.Storage.WaitForBlockCommit(uint64(startSeq))
		}
	}
}

// TestReplicationAdversarialNode tests the replication process of a node that
// has been sent a different block by one node, however the rest of the network
// notarizes a different block for the same round
func TestReplicationAdversarialNode(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	quorum := simplex.Quorum(len(nodes))
	bb := testutil.NewTestControlledBlockBuilder(t)
	net := testutil.NewInMemNetwork(t, nodes)

	testEpochConfig := &testutil.TestNodeConfig{
		ReplicationEnabled: true,
	}

	// doubleBlockProposalNode will propose two blocks for the same round
	doubleBlockProposalNode := testutil.NewSimplexNode(t, nodes[0], net, bb, testEpochConfig)
	normalNode2 := testutil.NewSimplexNode(t, nodes[1], net, bb, testEpochConfig)
	normalNode3 := testutil.NewSimplexNode(t, nodes[2], net, bb, testEpochConfig)
	laggingNode := testutil.NewSimplexNode(t, nodes[3], net, bb, &testutil.TestNodeConfig{
		ReplicationEnabled: true,
	})

	require.Equal(t, uint64(0), doubleBlockProposalNode.Storage.Height())
	require.Equal(t, uint64(0), normalNode2.Storage.Height())
	require.Equal(t, uint64(0), normalNode3.Storage.Height())
	require.Equal(t, uint64(0), laggingNode.Storage.Height())

	net.StartInstances()
	doubleBlock := testutil.NewTestBlock(doubleBlockProposalNode.E.Metadata())
	doubleBlockVote, err := newTestVote(doubleBlock, doubleBlockProposalNode.E.ID, doubleBlockProposalNode.E.Signer)
	require.NoError(t, err)
	msg := &simplex.Message{
		BlockMessage: &simplex.BlockMessage{
			Block: doubleBlock,
			Vote:  *doubleBlockVote,
		},
	}

	laggingNode.E.HandleMessage(msg, doubleBlockProposalNode.E.ID)
	net.Disconnect(laggingNode.E.ID)

	blocks := []simplex.VerifiedBlock{}
	for i := range 2 {
		bb.TriggerNewBlock()
		block := <-bb.Out
		blocks = append(blocks, block)
		for _, n := range net.Instances()[:3] {
			commited := n.Storage.WaitForBlockCommit(uint64(i))
			require.Equal(t, block, commited.(*testutil.TestBlock))
		}
	}

	// lagging node should not have commited the block
	require.Equal(t, uint64(0), laggingNode.Storage.Height())
	require.Equal(t, uint64(0), laggingNode.E.Metadata().Round)
	net.Connect(laggingNode.E.ID)
	fCert, _ := newFinalizationRecord(t, laggingNode.E.Logger, laggingNode.E.SignatureAggregator, blocks[1], nodes[:quorum])

	fCertMsg := &simplex.Message{
		FinalizationCertificate: &fCert,
	}
	laggingNode.E.HandleMessage(fCertMsg, doubleBlockProposalNode.E.ID)

	for i := range 2 {
		lagBlock := laggingNode.Storage.WaitForBlockCommit(uint64(i))
		require.Equal(t, blocks[i], lagBlock)
	}
}

// TestReplicationNotarizations tests that a lagging node also replicates
// notarizations after lagging behind.
func TestReplicationNotarizations(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	bb := testutil.NewTestControlledBlockBuilder(t)
	net := testutil.NewInMemNetwork(t, nodes)

	newNodeConfig := func(from simplex.NodeID) *testutil.TestNodeConfig {
		comm := testutil.NewTestComm(from, net, testutil.AllowAllMessages)
		return &testutil.TestNodeConfig{
			Comm:               comm,
			ReplicationEnabled: true,
		}
	}

	testutil.NewSimplexNode(t, nodes[0], net, bb, newNodeConfig(nodes[0]))
	testutil.NewSimplexNode(t, nodes[1], net, bb, newNodeConfig(nodes[1]))
	testutil.NewSimplexNode(t, nodes[2], net, bb, newNodeConfig(nodes[2]))
	// we do not expect the lagging node to build any blocks
	laggingBb := testutil.NewTestControlledBlockBuilder(t)
	laggingNode := testutil.NewSimplexNode(t, nodes[3], net, laggingBb, newNodeConfig(nodes[3]))

	for _, n := range net.Instances() {
		require.Equal(t, uint64(0), n.Storage.Height())
	}

	epochTimes := make([]time.Time, 0, len(nodes))
	for _, n := range net.Instances() {
		epochTimes = append(epochTimes, n.E.StartTime)
	}

	net.StartInstances()

	net.Disconnect(laggingNode.E.ID)
	numNotarizations := 9
	missedSeqs := uint64(0)
	blocks := []simplex.VerifiedBlock{}

	// finalization for the first block
	bb.TriggerNewBlock()
	block := <-bb.Out
	blocks = append(blocks, block)
	for _, n := range net.Instances() {
		if n.E.ID.Equals(laggingNode.E.ID) {
			continue
		}
		n.Storage.WaitForBlockCommit(0)
	}

	net.SetAllNodesMessageFilter(denyFinalizationMessages)
	// normal nodes continue to make progress
	for i := uint64(1); i < uint64(numNotarizations); i++ {
		emptyRound := bytes.Equal(simplex.LeaderForRound(nodes, i), laggingNode.E.ID)
		if emptyRound {
			testutil.AdvanceWithoutLeader(t, net, bb, epochTimes, i, laggingNode.E.ID)
			missedSeqs++
		} else {
			bb.TriggerNewBlock()
			block := <-bb.Out
			blocks = append(blocks, block)
			for _, n := range net.Instances() {
				if n.E.ID.Equals(laggingNode.E.ID) {
					continue
				}
				n.Wal.AssertNotarization(i)
			}
		}
	}

	for _, n := range net.Instances() {
		if n.E.ID.Equals(laggingNode.E.ID) {
			require.Equal(t, uint64(0), n.Storage.Height())
			require.Equal(t, uint64(0), n.E.Metadata().Round)
			continue
		}

		require.Equal(t, uint64(numNotarizations), n.E.Metadata().Round)
		require.Equal(t, uint64(1), n.E.Storage.Height())
	}

	net.SetAllNodesMessageFilter(testutil.AllowAllMessages)
	net.Connect(laggingNode.E.ID)
	bb.TriggerNewBlock()

	// lagging node should replicate the first finalized block and subsequent notarizations
	laggingNode.Storage.WaitForBlockCommit(0)

	for i := 1; i < numNotarizations+1; i++ {
		for _, n := range net.Instances() {
			// lagging node wont have a notarization record if it was the leader
			leader := simplex.LeaderForRound(nodes, uint64(i))
			if n.E.ID.Equals(leader) && n.E.ID.Equals(nodes[3]) {
				continue
			}
			n.Wal.AssertNotarization(uint64(i))
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

// denyFinalizationMessages blocks any messages that would cause nodes in
// a network to index a block in storage.
func denyFinalizationMessages(msg *simplex.Message, destination simplex.NodeID) bool {
	if msg.Finalization != nil {
		return false
	}
	if msg.FinalizationCertificate != nil {
		return false
	}

	return true
}

func onlyAllowEmptyRoundMessages(msg *simplex.Message, _ simplex.NodeID) bool {
	if msg.EmptyNotarization != nil {
		return true
	}
	if msg.EmptyVoteMessage != nil {
		return true
	}
	return false
}

func testReplicationEmptyNotarizations(t *testing.T, nodes []simplex.NodeID, endRound uint64) {
	bb := testutil.NewTestControlledBlockBuilder(t)
	laggingBb := testutil.NewTestControlledBlockBuilder(t)
	net := testutil.NewInMemNetwork(t, nodes)
	newNodeConfig := func(from simplex.NodeID) *testutil.TestNodeConfig {
		comm := testutil.NewTestComm(from, net, testutil.AllowAllMessages)
		return &testutil.TestNodeConfig{
			Comm:               comm,
			ReplicationEnabled: true,
		}
	}

	startTimes := make([]time.Time, 0, len(nodes))
	testutil.NewSimplexNode(t, nodes[0], net, bb, newNodeConfig(nodes[0]))
	testutil.NewSimplexNode(t, nodes[1], net, bb, newNodeConfig(nodes[1]))
	testutil.NewSimplexNode(t, nodes[2], net, bb, newNodeConfig(nodes[2]))
	testutil.NewSimplexNode(t, nodes[3], net, bb, newNodeConfig(nodes[3]))
	testutil.NewSimplexNode(t, nodes[4], net, bb, newNodeConfig(nodes[4]))
	laggingNode := testutil.NewSimplexNode(t, nodes[5], net, laggingBb, newNodeConfig(nodes[5]))

	for _, n := range net.Instances() {
		require.Equal(t, uint64(0), n.Storage.Height())
		startTimes = append(startTimes, n.E.StartTime)
	}

	net.StartInstances()

	net.Disconnect(laggingNode.E.ID)

	bb.TriggerNewBlock()
	for _, n := range net.Instances() {
		if n.E.ID.Equals(laggingNode.E.ID) {
			continue
		}
		n.Storage.WaitForBlockCommit(0)
	}

	net.SetAllNodesMessageFilter(onlyAllowEmptyRoundMessages)

	// normal nodes continue to make progress
	for i := uint64(1); i < endRound; i++ {
		leader := simplex.LeaderForRound(nodes, i)
		if !leader.Equals(laggingNode.E.ID) {
			bb.TriggerNewBlock()
		}

		testutil.AdvanceWithoutLeader(t, net, bb, startTimes, i, laggingNode.E.ID)
	}

	for _, n := range net.Instances() {
		if n.E.ID.Equals(laggingNode.E.ID) {
			require.Equal(t, uint64(0), n.Storage.Height())
			require.Equal(t, uint64(0), n.E.Metadata().Round)
			continue
		}

		// assert metadata
		require.Equal(t, uint64(endRound), n.E.Metadata().Round)
		require.Equal(t, uint64(1), n.E.Metadata().Seq)
		require.Equal(t, uint64(1), n.E.Storage.Height())
	}

	net.SetAllNodesMessageFilter(testutil.AllowAllMessages)
	net.Connect(laggingNode.E.ID)
	bb.TriggerNewBlock()
	for _, n := range net.Instances() {
		n.Storage.WaitForBlockCommit(1)
	}

	require.Equal(t, uint64(2), laggingNode.Storage.Height())
	require.Equal(t, uint64(endRound+1), laggingNode.E.Metadata().Round)
	require.Equal(t, uint64(2), laggingNode.E.Metadata().Seq)
}

// TestReplicationStartsBeforeCurrentRound tests the replication process of a node that
// starts replicating in the middle of the current round.
func TestReplicationStartsBeforeCurrentRound(t *testing.T) {
	bb := testutil.NewTestControlledBlockBuilder(t)
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	quorum := simplex.Quorum(len(nodes))
	net := testutil.NewInMemNetwork(t, nodes)
	startSeq := uint64(simplex.DefaultMaxRoundWindow + 3)
	storageData := createBlocks(t, nodes, &bb.TestBlockBuilder, startSeq)
	testEpochConfig := &testutil.TestNodeConfig{
		InitialStorage:     storageData,
		ReplicationEnabled: true,
	}
	normalNode1 := testutil.NewSimplexNode(t, nodes[0], net, bb, testEpochConfig)
	normalNode2 := testutil.NewSimplexNode(t, nodes[1], net, bb, testEpochConfig)
	normalNode3 := testutil.NewSimplexNode(t, nodes[2], net, bb, testEpochConfig)
	laggingNode := testutil.NewSimplexNode(t, nodes[3], net, bb, &testutil.TestNodeConfig{
		ReplicationEnabled: true,
	})

	firstBlock := storageData[0].VerifiedBlock
	record := simplex.BlockRecord(firstBlock.BlockHeader(), firstBlock.Bytes())
	laggingNode.Wal.Append(record)

	firstNotarizationRecord, err := newNotarizationRecord(laggingNode.E.Logger, laggingNode.E.SignatureAggregator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	laggingNode.Wal.Append(firstNotarizationRecord)

	secondBlock := storageData[1].VerifiedBlock
	record = simplex.BlockRecord(secondBlock.BlockHeader(), secondBlock.Bytes())
	laggingNode.Wal.Append(record)

	secondNotarizationRecord, err := newNotarizationRecord(laggingNode.E.Logger, laggingNode.E.SignatureAggregator, secondBlock, nodes[0:quorum])
	require.NoError(t, err)
	laggingNode.Wal.Append(secondNotarizationRecord)

	require.Equal(t, startSeq, normalNode1.Storage.Height())
	require.Equal(t, startSeq, normalNode2.Storage.Height())
	require.Equal(t, startSeq, normalNode3.Storage.Height())
	require.Equal(t, uint64(0), laggingNode.Storage.Height())

	net.StartInstances()

	laggingNodeMd := laggingNode.E.Metadata()
	require.Equal(t, uint64(2), laggingNodeMd.Round)

	bb.TriggerNewBlock()
	for i := 0; i <= int(startSeq); i++ {
		for _, n := range net.Instances() {
			n.Storage.WaitForBlockCommit(uint64(startSeq))
		}
	}
}

func TestReplicationFutureFinalizationCertificate(t *testing.T) {
	// send a block, then simultaneously send a finalization certificate for the block
	l := testutil.MakeLogger(t, 1)
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}

	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	quorum := simplex.Quorum(len(nodes))
	signatureAggregator := &testutil.TestSignatureAggregator{}
	conf, _, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[1], testutil.NoopComm(nodes), bb)

	e, err := simplex.NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	require.Equal(t, md.Round, md.Seq)

	block := <-bb.Out
	block.VerificationDelay = make(chan struct{}) // add a delay to the block verification

	vote, err := newTestVote(block, nodes[0], conf.Signer)
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

	block.VerificationDelay <- struct{}{} // unblock the block verification

	storedBlock := storage.WaitForBlockCommit(0)
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
	bb := testutil.NewTestControlledBlockBuilder(t)
	laggingBb := testutil.NewTestControlledBlockBuilder(t)
	net := testutil.NewInMemNetwork(t, nodes)
	testConfig := &testutil.TestNodeConfig{
		ReplicationEnabled: true,
	}
	normalNode1 := testutil.NewSimplexNode(t, nodes[0], net, bb, testConfig)
	normalNode2 := testutil.NewSimplexNode(t, nodes[1], net, bb, testConfig)
	normalNode3 := testutil.NewSimplexNode(t, nodes[2], net, bb, testConfig)
	laggingNode := testutil.NewSimplexNode(t, nodes[3], net, laggingBb, testConfig)

	require.Equal(t, uint64(0), normalNode1.Storage.Height())
	require.Equal(t, uint64(0), normalNode2.Storage.Height())
	require.Equal(t, uint64(0), normalNode3.Storage.Height())
	require.Equal(t, uint64(0), laggingNode.Storage.Height())

	epochTimes := make([]time.Time, 0, 4)
	for _, n := range net.Instances() {
		epochTimes = append(epochTimes, n.E.StartTime)
	}

	net.StartInstances()

	for i := uint64(0); i < startDisconnect; i++ {
		if bytes.Equal(simplex.LeaderForRound(nodes, i), nodes[3]) {
			laggingBb.TriggerNewBlock()
		} else {
			bb.TriggerNewBlock()
		}
		for _, n := range net.Instances() {
			n.Storage.WaitForBlockCommit(i)
		}
	}

	// all nodes have commited `startDisconnect` blocks
	for _, n := range net.Instances() {
		require.Equal(t, startDisconnect, n.Storage.Height())
	}

	// lagging node disconnects
	net.Disconnect(nodes[3])

	isLaggingNodeLeader := bytes.Equal(simplex.LeaderForRound(nodes, startDisconnect), nodes[3])
	if isLaggingNodeLeader {
		laggingBb.TriggerNewBlock()
	}

	missedSeqs := uint64(0)
	// normal nodes continue to make progress
	for i := startDisconnect; i < endDisconnect; i++ {
		emptyRound := bytes.Equal(simplex.LeaderForRound(nodes, i), nodes[3])
		if emptyRound {
			testutil.AdvanceWithoutLeader(t, net, bb, epochTimes, i, laggingNode.E.ID)
			missedSeqs++
		} else {
			bb.TriggerNewBlock()
			for _, n := range net.Instances()[:3] {
				n.Storage.WaitForBlockCommit(i - missedSeqs)
			}
		}
	}
	// all nodes excpet for lagging node have progressed and commited [endDisconnect - missedSeqs] blocks
	for _, n := range net.Instances()[:3] {
		require.Equal(t, endDisconnect-missedSeqs, n.Storage.Height())
	}
	require.Equal(t, startDisconnect, laggingNode.Storage.Height())
	require.Equal(t, startDisconnect, laggingNode.E.Metadata().Round)
	// lagging node reconnects
	net.Connect(nodes[3])
	bb.TriggerNewBlock()
	for _, n := range net.Instances() {
		n.Storage.WaitForBlockCommit(endDisconnect - missedSeqs)
	}

	for _, n := range net.Instances() {
		require.Equal(t, endDisconnect-missedSeqs, n.Storage.Height()-1)
		require.Equal(t, endDisconnect+1, n.E.Metadata().Round)
	}

	// the lagging node should build a block when triggered if its the leader
	if bytes.Equal(simplex.LeaderForRound(nodes, endDisconnect+1), nodes[3]) {
		laggingBb.TriggerNewBlock()
	} else {
		bb.TriggerNewBlock()
	}

	for _, n := range net.Instances() {
		n.Storage.WaitForBlockCommit(endDisconnect - missedSeqs + 1)
	}
}

func onlyAllowBlockProposalsAndNotarizations(msg *simplex.Message, to simplex.NodeID) bool {
	// TODO: remove hardcoded node id
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
	bb := testutil.NewTestControlledBlockBuilder(t)
	net := testutil.NewInMemNetwork(t, nodes)

	nodeConfig := func(from simplex.NodeID) *testutil.TestNodeConfig {
		comm := testutil.NewTestComm(from, net, onlyAllowBlockProposalsAndNotarizations)
		return &testutil.TestNodeConfig{
			Comm:               comm,
			ReplicationEnabled: true,
		}
	}

	testutil.NewSimplexNode(t, nodes[0], net, bb, nodeConfig(nodes[0]))
	testutil.NewSimplexNode(t, nodes[1], net, bb, nodeConfig(nodes[1]))
	testutil.NewSimplexNode(t, nodes[2], net, bb, nodeConfig(nodes[2]))

	laggingNode := testutil.NewSimplexNode(t, nodes[3], net, bb, nodeConfig(nodes[3]))

	for _, n := range net.Instances() {
		require.Equal(t, uint64(0), n.Storage.Height())
	}

	net.StartInstances()

	// normal nodes continue to make progress
	for i := uint64(0); i < uint64(numBlocks); i++ {
		bb.TriggerNewBlock()
		for _, n := range net.Instances()[:3] {
			n.Storage.WaitForBlockCommit(uint64(i))
		}

	}

	laggingNode.Wal.AssertNotarization(numBlocks - 1)
	require.Equal(t, uint64(0), laggingNode.Storage.Height())
	require.Equal(t, uint64(numBlocks), laggingNode.E.Metadata().Round)

	net.SetAllNodesMessageFilter(testutil.AllowAllMessages)
	bb.TriggerNewBlock()
	for _, n := range net.Instances() {
		n.Storage.WaitForBlockCommit(uint64(numBlocks))
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
		fCert, _ := newFinalizationRecord(t, logger, &testutil.TestSignatureAggregator{}, block, nodes)
		data = append(data, simplex.VerifiedFinalizedBlock{
			VerifiedBlock: block,
			FCert:         fCert,
		})
	}
	return data
}

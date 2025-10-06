// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	. "github.com/ava-labs/simplex/testutil"
	"go.uber.org/zap/zapcore"

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
	net := NewInMemNetwork(t, nodes)

	// initiate a network with 4 nodes. one node is behind by startSeq blocks
	storageData := createBlocks(t, nodes, startSeq)
	testEpochConfig := &TestNodeConfig{
		InitialStorage:     storageData,
		ReplicationEnabled: true,
	}
	normalNode1 := NewSimplexNode(t, nodes[0], net, testEpochConfig)
	normalNode2 := NewSimplexNode(t, nodes[1], net, testEpochConfig)
	normalNode3 := NewSimplexNode(t, nodes[2], net, testEpochConfig)
	laggingNode := NewSimplexNode(t, nodes[3], net, &TestNodeConfig{
		ReplicationEnabled: true,
	})

	require.Equal(t, startSeq, normalNode1.Storage.NumBlocks())
	require.Equal(t, startSeq, normalNode2.Storage.NumBlocks())
	require.Equal(t, startSeq, normalNode3.Storage.NumBlocks())
	require.Equal(t, uint64(0), laggingNode.Storage.NumBlocks())

	net.StartInstances()
	net.TriggerLeaderBlockBuilder(startSeq)

	// all blocks except the lagging node start at round startSeq, seq startSeq.
	// lagging node starts at round 0, seq 0.
	// this asserts that the lagging node catches up to the latest round
	for i := 0; i <= int(startSeq); i++ {
		for _, n := range net.Instances {
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
	net := NewInMemNetwork(t, nodes)

	testEpochConfig := &TestNodeConfig{
		ReplicationEnabled: true,
	}

	// doubleBlockProposalNode will propose two blocks for the same round
	doubleBlockProposalNode := NewSimplexNode(t, nodes[0], net, testEpochConfig)
	normalNode2 := NewSimplexNode(t, nodes[1], net, testEpochConfig)
	normalNode3 := NewSimplexNode(t, nodes[2], net, testEpochConfig)
	laggingNode := NewSimplexNode(t, nodes[3], net, &TestNodeConfig{
		ReplicationEnabled: true,
	})

	require.Equal(t, uint64(0), doubleBlockProposalNode.Storage.NumBlocks())
	require.Equal(t, uint64(0), normalNode2.Storage.NumBlocks())
	require.Equal(t, uint64(0), normalNode3.Storage.NumBlocks())
	require.Equal(t, uint64(0), laggingNode.Storage.NumBlocks())

	net.StartInstances()
	doubleBlock := NewTestBlock(doubleBlockProposalNode.E.Metadata(), emptyBlacklist)
	doubleBlockVote, err := NewTestVote(doubleBlock, doubleBlockProposalNode.E.ID)
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
	for i := uint64(0); i < 2; i++ {
		block := net.TriggerLeaderBlockBuilder(i)

		blocks = append(blocks, block)
		for _, n := range net.Instances[:3] {
			committed := n.Storage.WaitForBlockCommit(i)
			require.Equal(t, block, committed.(*TestBlock))
		}
	}

	// lagging node should not have committed the block
	require.Equal(t, uint64(0), laggingNode.Storage.NumBlocks())
	require.Equal(t, uint64(0), laggingNode.E.Metadata().Round)
	net.Connect(laggingNode.E.ID)

	finalization, _ := NewFinalizationRecord(t, laggingNode.E.Logger, laggingNode.E.SignatureAggregator, blocks[1], nodes[:quorum])
	finalizationMsg := &simplex.Message{
		Finalization: &finalization,
	}
	laggingNode.E.HandleMessage(finalizationMsg, doubleBlockProposalNode.E.ID)

	for i := range 2 {
		lagBlock := laggingNode.Storage.WaitForBlockCommit(uint64(i))
		require.Equal(t, blocks[i], lagBlock)
	}
}

// TestRebroadcastingWithReplication verifies that after network recovery,
// a lagging node and the rest of the network correctly propagate missing
// finalizations and index all blocks.
func TestRebroadcastingWithReplication(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	net := NewInMemNetwork(t, nodes)

	newNodeConfig := func(from simplex.NodeID) *TestNodeConfig {
		comm := NewTestComm(from, net, AllowAllMessages)
		return &TestNodeConfig{
			Comm:               comm,
			ReplicationEnabled: true,
		}
	}

	NewSimplexNode(t, nodes[0], net, newNodeConfig(nodes[0]))
	NewSimplexNode(t, nodes[1], net, newNodeConfig(nodes[1]))
	NewSimplexNode(t, nodes[2], net, newNodeConfig(nodes[2]))
	// we do not expect the lagging node to build any blocks
	laggingNode := NewSimplexNode(t, nodes[3], net, newNodeConfig(nodes[3]))

	for _, n := range net.Instances {
		require.Equal(t, uint64(0), n.Storage.NumBlocks())
	}

	epochTimes := make([]time.Time, 0, len(nodes))
	for _, n := range net.Instances {
		epochTimes = append(epochTimes, n.E.StartTime)
	}

	net.StartInstances()

	net.Disconnect(laggingNode.E.ID)
	numNotarizations := uint64(9)
	missedSeqs := uint64(0)

	// finalization for the first block
	net.TriggerLeaderBlockBuilder(0)
	for _, n := range net.Instances {
		if n.E.ID.Equals(laggingNode.E.ID) {
			continue
		}
		n.Storage.WaitForBlockCommit(0)
	}

	net.SetAllNodesMessageFilter(denyFinalizationMessages)

	// normal nodes continue to make progress
	for i := uint64(1); i < numNotarizations; i++ {
		emptyRound := bytes.Equal(simplex.LeaderForRound(nodes, i), laggingNode.E.ID)
		if emptyRound {
			net.AdvanceWithoutLeader(epochTimes, i, laggingNode.E.ID)
			missedSeqs++
		} else {
			net.TriggerLeaderBlockBuilder(i)
			for _, n := range net.Instances {
				if n.E.ID.Equals(laggingNode.E.ID) {
					continue
				}
				n.WAL.AssertNotarization(i)
			}
		}
	}

	for _, n := range net.Instances {
		if n.E.ID.Equals(laggingNode.E.ID) {
			require.Equal(t, uint64(0), n.Storage.NumBlocks())
			require.Equal(t, uint64(0), n.E.Metadata().Round)
			continue
		}

		// assert metadata
		require.Equal(t, numNotarizations, n.E.Metadata().Round)
		require.Equal(t, uint64(1), n.E.Storage.NumBlocks())
	}

	// the lagging node has been asleep, it should be notified blocks are available
	laggingNode.TriggerBlockShouldBeBuilt()
	net.SetAllNodesMessageFilter(AllowAllMessages)
	net.Connect(laggingNode.E.ID)
	block := net.TriggerLeaderBlockBuilder(numNotarizations)

	timeout := time.NewTimer(30 * time.Second)
	for i := uint64(0); i <= block.Metadata.Seq; i++ {
		for _, n := range net.Instances {
			for {
				committed := n.Storage.NumBlocks()
				if committed > i {
					break
				}

				// if we haven't indexed, advance the time to trigger rebroadcast/replication timeouts
				select {
				case <-time.After(time.Millisecond * 10):
					for i, n := range net.Instances {
						epochTimes[i] = epochTimes[i].Add(2 * simplex.DefaultMaxProposalWaitTime)
						n.E.AdvanceTime(epochTimes[i])
					}
					continue
				case <-timeout.C:
					require.Fail(t, "timed out waiting for event")
				}
			}
		}
	}

	for _, n := range net.Instances {
		require.Equal(t, block.Metadata.Seq+1, n.Storage.NumBlocks())
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
	net := NewInMemNetwork(t, nodes)
	newNodeConfig := func(from simplex.NodeID) *TestNodeConfig {
		comm := NewTestComm(from, net, AllowAllMessages)
		return &TestNodeConfig{
			Comm:               comm,
			ReplicationEnabled: true,
		}
	}

	startTimes := make([]time.Time, 0, len(nodes))
	NewSimplexNode(t, nodes[0], net, newNodeConfig(nodes[0]))
	NewSimplexNode(t, nodes[1], net, newNodeConfig(nodes[1]))
	NewSimplexNode(t, nodes[2], net, newNodeConfig(nodes[2]))
	NewSimplexNode(t, nodes[3], net, newNodeConfig(nodes[3]))
	NewSimplexNode(t, nodes[4], net, newNodeConfig(nodes[4]))
	laggingNode := NewSimplexNode(t, nodes[5], net, newNodeConfig(nodes[5]))

	for _, n := range net.Instances {
		require.Equal(t, uint64(0), n.Storage.NumBlocks())
		startTimes = append(startTimes, n.E.StartTime)
	}

	net.StartInstances()

	net.Disconnect(laggingNode.E.ID)

	net.TriggerLeaderBlockBuilder(0)
	for _, n := range net.Instances {
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
			net.TriggerLeaderBlockBuilder(i)
		}

		net.AdvanceWithoutLeader(startTimes, i, laggingNode.E.ID)
	}

	for _, n := range net.Instances {
		if n.E.ID.Equals(laggingNode.E.ID) {
			require.Equal(t, uint64(0), n.Storage.NumBlocks())
			require.Equal(t, uint64(0), n.E.Metadata().Round)
			continue
		}

		// assert metadata
		require.Equal(t, uint64(endRound), n.E.Metadata().Round)
		require.Equal(t, uint64(1), n.E.Metadata().Seq)
		require.Equal(t, uint64(1), n.E.Storage.NumBlocks())
	}

	net.SetAllNodesMessageFilter(AllowAllMessages)
	net.Connect(laggingNode.E.ID)
	net.TriggerLeaderBlockBuilder(endRound)
	for _, n := range net.Instances {
		n.Storage.WaitForBlockCommit(1)
	}

	require.Equal(t, uint64(2), laggingNode.Storage.NumBlocks())
	require.Equal(t, uint64(endRound+1), laggingNode.E.Metadata().Round)
	require.Equal(t, uint64(2), laggingNode.E.Metadata().Seq)
}

// TestReplicationStartsBeforeCurrentRound tests the replication process of a node that
// starts replicating in the middle of the current round.
func TestReplicationStartsBeforeCurrentRound(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	quorum := simplex.Quorum(len(nodes))
	net := NewInMemNetwork(t, nodes)
	startSeq := uint64(simplex.DefaultMaxRoundWindow + 3)
	storageData := createBlocks(t, nodes, startSeq)
	testEpochConfig := &TestNodeConfig{
		InitialStorage:     storageData,
		ReplicationEnabled: true,
	}
	normalNode1 := NewSimplexNode(t, nodes[0], net, testEpochConfig)
	normalNode2 := NewSimplexNode(t, nodes[1], net, testEpochConfig)
	normalNode3 := NewSimplexNode(t, nodes[2], net, testEpochConfig)
	laggingNode := NewSimplexNode(t, nodes[3], net, &TestNodeConfig{
		ReplicationEnabled: true,
	})

	firstBlock := storageData[0].VerifiedBlock
	fBytes, err := firstBlock.Bytes()
	require.NoError(t, err)
	record := simplex.BlockRecord(firstBlock.BlockHeader(), fBytes)
	laggingNode.WAL.Append(record)

	firstNotarizationRecord, err := NewNotarizationRecord(laggingNode.E.Logger, laggingNode.E.SignatureAggregator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	laggingNode.WAL.Append(firstNotarizationRecord)

	secondBlock := storageData[1].VerifiedBlock
	sBytes, err := secondBlock.Bytes()
	require.NoError(t, err)
	record = simplex.BlockRecord(secondBlock.BlockHeader(), sBytes)
	laggingNode.WAL.Append(record)

	secondNotarizationRecord, err := NewNotarizationRecord(laggingNode.E.Logger, laggingNode.E.SignatureAggregator, secondBlock, nodes[0:quorum])
	require.NoError(t, err)
	laggingNode.WAL.Append(secondNotarizationRecord)

	require.Equal(t, startSeq, normalNode1.Storage.NumBlocks())
	require.Equal(t, startSeq, normalNode2.Storage.NumBlocks())
	require.Equal(t, startSeq, normalNode3.Storage.NumBlocks())
	require.Equal(t, uint64(0), laggingNode.Storage.NumBlocks())

	net.StartInstances()

	laggingNodeMd := laggingNode.E.Metadata()
	require.Equal(t, uint64(2), laggingNodeMd.Round)

	net.TriggerLeaderBlockBuilder(startSeq)
	for i := uint64(0); i <= startSeq; i++ {
		for _, n := range net.Instances {
			n.Storage.WaitForBlockCommit(startSeq)
		}
	}
}

func TestReplicationFutureFinalization(t *testing.T) {
	// send a block, then simultaneously send a finalization for the block
	bb := &TestBlockBuilder{Out: make(chan *TestBlock, 1)}
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	quorum := simplex.Quorum(len(nodes))

	conf, _, storage := DefaultTestNodeEpochConfig(t, nodes[1], NoopComm(nodes), bb)

	e, err := simplex.NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
	require.True(t, ok)
	require.Equal(t, md.Round, md.Seq)

	block := <-bb.Out
	block.VerificationDelay = make(chan struct{}) // add a delay to the block verification

	vote, err := NewTestVote(block, nodes[0])
	require.NoError(t, err)

	err = e.HandleMessage(&simplex.Message{
		BlockMessage: &simplex.BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[0])
	require.NoError(t, err)

	finalization, _ := NewFinalizationRecord(t, e.Logger, e.SignatureAggregator, block, nodes[0:quorum])
	// send finalization
	err = e.HandleMessage(&simplex.Message{
		Finalization: &finalization,
	}, nodes[0])
	require.NoError(t, err)

	block.VerificationDelay <- struct{}{} // unblock the block verification

	storedBlock := storage.WaitForBlockCommit(0)
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
	net := NewInMemNetwork(t, nodes)
	testConfig := &TestNodeConfig{
		ReplicationEnabled: true,
	}
	normalNode1 := NewSimplexNode(t, nodes[0], net, testConfig)
	normalNode2 := NewSimplexNode(t, nodes[1], net, testConfig)
	normalNode3 := NewSimplexNode(t, nodes[2], net, testConfig)
	laggingNode := NewSimplexNode(t, nodes[3], net, testConfig)

	require.Equal(t, uint64(0), normalNode1.Storage.NumBlocks())
	require.Equal(t, uint64(0), normalNode2.Storage.NumBlocks())
	require.Equal(t, uint64(0), normalNode3.Storage.NumBlocks())
	require.Equal(t, uint64(0), laggingNode.Storage.NumBlocks())

	epochTimes := make([]time.Time, 0, 4)
	for _, n := range net.Instances {
		epochTimes = append(epochTimes, n.E.StartTime)
	}

	net.StartInstances()

	for i := uint64(0); i < startDisconnect; i++ {
		net.TriggerLeaderBlockBuilder(i)
		for _, n := range net.Instances {
			n.Storage.WaitForBlockCommit(i)
		}
	}

	// all nodes have committed `startDisconnect` blocks
	for _, n := range net.Instances {
		require.Equal(t, startDisconnect, n.Storage.NumBlocks())
	}

	// lagging node disconnects
	net.Disconnect(laggingNode.E.ID)
	isLaggingNodeLeader := bytes.Equal(simplex.LeaderForRound(nodes, startDisconnect), laggingNode.E.ID)
	if isLaggingNodeLeader {
		net.TriggerLeaderBlockBuilder(startDisconnect)
	}

	missedSeqs := uint64(0)
	// normal nodes continue to make progress
	for i := startDisconnect; i < endDisconnect; i++ {
		emptyRound := bytes.Equal(simplex.LeaderForRound(nodes, i), nodes[3])
		if emptyRound {
			net.AdvanceWithoutLeader(epochTimes, i, laggingNode.E.ID)
			missedSeqs++
		} else {
			net.TriggerLeaderBlockBuilder(i)
			for _, n := range net.Instances[:3] {
				n.Storage.WaitForBlockCommit(i - missedSeqs)
			}
		}
	}

	// all nodes except for lagging node have progressed and committed [endDisconnect - missedSeqs] blocks
	for _, n := range net.Instances[:3] {
		require.Equal(t, endDisconnect-missedSeqs, n.Storage.NumBlocks())
	}
	require.Equal(t, startDisconnect, laggingNode.Storage.NumBlocks())
	require.Equal(t, startDisconnect, laggingNode.E.Metadata().Round)
	// lagging node reconnects
	net.Connect(laggingNode.E.ID)
	net.TriggerLeaderBlockBuilder(endDisconnect)

	var blacklist simplex.Blacklist
	for _, n := range net.Instances {
		block := n.Storage.WaitForBlockCommit(endDisconnect - missedSeqs)
		blacklist = block.Blacklist()
	}

	for _, n := range net.Instances {
		require.Equal(t, endDisconnect-missedSeqs, n.Storage.NumBlocks()-1)
	}

	if blacklist.IsNodeSuspected(3) {
		t.Log("lagging node is blacklisted, cannot continue replication")
		return
	}

	// the lagging node should build a block when triggered if its the leader
	net.TriggerLeaderBlockBuilder(endDisconnect + 1)
	for _, n := range net.Instances {
		n.Storage.WaitForBlockCommit(endDisconnect - missedSeqs + 1)
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
func sendVotesToOneNode(filteredInNode simplex.NodeID) MessageFilter {
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

	tbb := &TestBlockBuilder{Out: make(chan *TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1), In: make(chan *TestBlock, 1)}
	bb := NewTestControlledBlockBuilder(t)
	bb.TestBlockBuilder = *tbb
	storage := NewInMemStorage()
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	blocks := createBlocks(t, nodes, 5)

	quorum := simplex.Quorum(len(nodes))
	sentMessages := make(chan *simplex.Message, 100)

	conf, _, storage := DefaultTestNodeEpochConfig(t, nodes[0], &recordingComm{
		Communication: NoopComm(nodes),
		SentMessages:  sentMessages,
	}, bb)

	conf.ReplicationEnabled = true
	l := conf.Logger.(*TestLogger)
	l.Intercept(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Scheduling block building") {
			aboutToBuildBlock.Done()
		}
		if strings.Contains(entry.Message, "We are the leader of this round, but a higher round has been finalized. Aborting block building.") {
			cancelBlockBuilding.Done()
		}
		return nil
	})

	e, err := simplex.NewEpoch(conf)
	e.ReplicationEnabled = true
	require.NoError(t, err)
	require.NoError(t, e.Start())

	bb.In <- blocks[0].VerifiedBlock.(*TestBlock)
	bb.Out <- blocks[0].VerifiedBlock.(*TestBlock)

	bb.TriggerNewBlock()
	notarizeAndFinalizeRound(t, e, &bb.TestBlockBuilder)

	gb := storage.WaitForBlockCommit(0)
	require.Equal(t, gb, blocks[0].VerifiedBlock.(*TestBlock))

	highBlock, _ := blocks[3].VerifiedBlock.(*TestBlock)

	highFinalization, _ := NewFinalizationRecord(t, e.Logger, e.SignatureAggregator, highBlock, nodes[0:quorum])

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
	for len(bb.TestBlockBuilder.BlockShouldBeBuilt) > 0 && len(bb.Out) > 0 {
		select {
		case <-bb.TestBlockBuilder.BlockShouldBeBuilt:
		default:
		}
		select {
		case <-bb.Out:
		default:
		}
	}

	// Prepare the quorum round answer to be sent as a response to the replication request
	quorumRounds := make([]simplex.QuorumRound, 0, 4)
	for i := uint64(1); i <= 4; i++ {
		tb := blocks[i].VerifiedBlock.(*TestBlock)
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

	storage.WaitForBlockCommit(4)

	// Just for sanity, ensure that the block building was cancelled
	cancelBlockBuilding.Wait()
}

// TestReplicationNodeDiverges tests that a node replicates blocks even if they
// have a stale notarization for a round(i.e. a node notarized a block but the rest of the network
// propagated an empty notarization).
func TestReplicationNodeDiverges(t *testing.T) {
	t.Skip("flaky test, should re-enable with the fix for https://github.com/ava-labs/Simplex/issues/262")
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}, {5}, {6}}
	numBlocks := uint64(5)

	net := NewInMemNetwork(t, nodes)

	nodeConfig := func(from simplex.NodeID) *TestNodeConfig {
		comm := NewTestComm(from, net, sendVotesToOneNode(nodes[3]))
		return &TestNodeConfig{
			Comm:               comm,
			ReplicationEnabled: true,
		}
	}

	NewSimplexNode(t, nodes[0], net, nodeConfig(nodes[0]))
	NewSimplexNode(t, nodes[1], net, nodeConfig(nodes[1]))
	NewSimplexNode(t, nodes[2], net, nodeConfig(nodes[2]))
	laggingNode := NewSimplexNode(t, nodes[3], net, nodeConfig(nodes[3]))

	// we need at least 6 nodes since the lagging node & leader will not timeout
	NewSimplexNode(t, nodes[4], net, nodeConfig(nodes[4]))
	NewSimplexNode(t, nodes[5], net, nodeConfig(nodes[5]))

	startTimes := make([]time.Time, 0, len(nodes))
	for _, n := range net.Instances {
		require.Equal(t, uint64(0), n.Storage.NumBlocks())
		startTimes = append(startTimes, n.E.StartTime)
	}

	net.StartInstances()
	net.TriggerLeaderBlockBuilder(0)

	// because of the message filter, the lagging one will be the only one to notarize the block
	laggingNode.WAL.AssertNotarization(0)
	for _, n := range net.Instances {
		if n.E.ID.Equals(laggingNode.E.ID) {
			continue
		}
		require.Equal(t, false, n.WAL.ContainsNotarization(0))
	}

	// we disconnect lagging node first so that it doesn't send the notarized block to any other nodes
	net.Disconnect(laggingNode.E.ID)
	net.SetAllNodesMessageFilter(
		// block sending votes from round 0 to ensure all nodes will timeout
		func(msg *simplex.Message, _, to simplex.NodeID) bool {
			return !(msg.VoteMessage != nil && msg.VoteMessage.Vote.Round == 0)
		},
	)

	// This function call ensures all nodes will timeout, and
	// receive an empty notarization for round 0(except for lagging).
	net.AdvanceWithoutLeader(startTimes, 0, laggingNode.E.ID)

	for _, n := range net.Instances {
		if n.E.ID.Equals(laggingNode.E.ID) {
			require.Equal(t, uint64(1), n.E.Metadata().Round)
			require.Equal(t, uint64(1), n.E.Metadata().Seq)
			continue
		}

		require.Equal(t, uint64(0), n.E.Metadata().Seq)
		require.Equal(t, uint64(1), n.E.Metadata().Round)
	}

	// advance [numBlocks] while the lagging node is disconnected
	missedSeqs := uint64(1) // missed the first seq
	for i := uint64(1); i < 1+numBlocks; i++ {
		emptyRound := bytes.Equal(simplex.LeaderForRound(nodes, i), laggingNode.E.ID)
		if emptyRound {
			net.AdvanceWithoutLeader(startTimes, i, laggingNode.E.ID)
			missedSeqs++
		} else {
			net.TriggerLeaderBlockBuilder(i)
			for _, n := range net.Instances {
				if n.E.ID.Equals(laggingNode.E.ID) {
					continue
				}
				n.Storage.WaitForBlockCommit(i - missedSeqs)
			}
		}
	}

	net.Connect(laggingNode.E.ID)
	net.TriggerLeaderBlockBuilder(numBlocks + 1)
	for _, n := range net.Instances {
		n.Storage.WaitForBlockCommit(numBlocks - missedSeqs + 1)
	}
	assertEqualLedgers(t, net)
}

func assertEqualLedgers(t *testing.T, net *InMemNetwork) {
	expectedLedger := map[uint64][]byte{}

	for seq := range net.Instances[0].Storage.NumBlocks() {
		block, _, err := net.Instances[0].Storage.Retrieve(seq)
		require.NoError(t, err)
		bytes, err := block.Bytes()
		require.NoError(t, err)
		expectedLedger[seq] = bytes

	}

	for _, n := range net.Instances {
		actualLedger := map[uint64][]byte{}

		for seq := range n.Storage.NumBlocks() {
			block, _, err := n.Storage.Retrieve(seq)
			require.NoError(t, err)
			bytes, err := block.Bytes()
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
	net := NewInMemNetwork(t, nodes)

	nodeConfig := func(from simplex.NodeID) *TestNodeConfig {
		comm := NewTestComm(from, net, onlyAllowBlockProposalsAndNotarizations)
		return &TestNodeConfig{
			Comm:               comm,
			ReplicationEnabled: true,
		}
	}

	NewSimplexNode(t, nodes[0], net, nodeConfig(nodes[0]))
	NewSimplexNode(t, nodes[1], net, nodeConfig(nodes[1]))
	NewSimplexNode(t, nodes[2], net, nodeConfig(nodes[2]))

	laggingNode := NewSimplexNode(t, nodes[3], net, nodeConfig(nodes[3]))

	for _, n := range net.Instances {
		require.Equal(t, uint64(0), n.Storage.NumBlocks())
	}

	net.StartInstances()

	// normal nodes continue to make progress
	for i := uint64(0); i < uint64(numBlocks); i++ {
		net.TriggerLeaderBlockBuilder(i)
		for _, n := range net.Instances[:3] {
			n.Storage.WaitForBlockCommit(uint64(i))
		}

	}

	laggingNode.WAL.AssertNotarization(numBlocks - 1)
	require.Equal(t, uint64(0), laggingNode.Storage.NumBlocks())
	require.Equal(t, uint64(numBlocks), laggingNode.E.Metadata().Round)

	net.SetAllNodesMessageFilter(AllowAllMessages)
	net.TriggerLeaderBlockBuilder(numBlocks)
	for _, n := range net.Instances {
		n.Storage.WaitForBlockCommit(uint64(numBlocks))
	}
}

func createBlocks(t *testing.T, nodes []simplex.NodeID, seqCount uint64) []simplex.VerifiedFinalizedBlock {
	bb := NewTestBlockBuilder()
	logger := MakeLogger(t, int(0))
	ctx := context.Background()
	data := make([]simplex.VerifiedFinalizedBlock, 0, seqCount)
	var prev simplex.Digest
	for i := uint64(0); i < seqCount; i++ {
		protocolMetadata := simplex.ProtocolMetadata{
			Seq:   i,
			Round: i,
			Prev:  prev,
		}

		block, ok := bb.BuildBlock(ctx, protocolMetadata, emptyBlacklist)
		require.True(t, ok)
		prev = block.BlockHeader().Digest
		finalization, _ := NewFinalizationRecord(t, logger, &TestSignatureAggregator{}, block, nodes)
		data = append(data, simplex.VerifiedFinalizedBlock{
			VerifiedBlock: block,
			Finalization:  finalization,
		})
	}
	return data
}

func TestReplicationVerifyNotarization(t *testing.T) {
	bb := &TestBlockBuilder{Out: make(chan *TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}

	// This function takes a QC and makes it that it is signed by only 2 out of 4 nodes,
	// while still having a quorum of signatures.
	corruptQC := func(qc simplex.QuorumCertificate) simplex.QuorumCertificate {
		badQC := qc.(TestQC)
		// Duplicate the last signature
		badQC = append(badQC, badQC[len(badQC)-1])
		// Remove the first signature
		badQC = badQC[1:]

		// Finalization should have 3 signers
		require.Len(t, badQC.Signers(), 3)

		// But all these signers are either the second and third node.
		require.Contains(t, badQC.Signers(), nodes[1])
		require.Contains(t, badQC.Signers(), nodes[2])

		// Not the first or the fourth node.
		require.NotContains(t, badQC.Signers(), nodes[0])
		require.NotContains(t, badQC.Signers(), nodes[3])

		return badQC
	}

	quorum := simplex.Quorum(len(nodes))
	sentMessages := make(chan *simplex.Message, 100)

	conf, wal, _ := DefaultTestNodeEpochConfig(t, nodes[1], &recordingComm{
		Communication: NewNoopComm(nodes),
		SentMessages:  sentMessages,
	}, bb)
	conf.ReplicationEnabled = true

	e, err := simplex.NewEpoch(conf)
	require.NoError(t, err)
	require.NoError(t, e.Start())

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
	require.True(t, ok)
	require.Equal(t, md.Round, md.Seq)

	block := <-bb.Out

	finalization, _ := NewFinalizationRecord(t, e.Logger, e.SignatureAggregator, block, nodes[0:quorum])

	// Trigger the replication process to start by sending a finalization for a block we do not have
	e.HandleMessage(&simplex.Message{
		Finalization: &finalization,
	}, nodes[0])

	// Wait for the replication request to be sent
	for {
		msg := <-sentMessages
		if msg.ReplicationRequest != nil {
			break
		}
	}

	notarization, err := NewNotarization(e.Logger, e.SignatureAggregator, block, nodes[0:quorum])
	require.NoError(t, err)

	// Corrupt the QC
	notarization.QC = corruptQC(notarization.QC)

	// Respond to the replication request with a block that has a notarization
	replicationResponse := &simplex.ReplicationResponse{
		Data: []simplex.QuorumRound{
			{
				Block:        block,
				Notarization: &notarization,
			},
		},
	}
	e.HandleMessage(&simplex.Message{
		ReplicationResponse: replicationResponse,
	}, nodes[0])

	require.Never(t, func() bool {
		return wal.ContainsNotarization(0)
	}, time.Millisecond*500, time.Millisecond*10, "Did not expect block with a corrupt QC to be written to the WAL")
}

func TestReplicationVerifyEmptyNotarization(t *testing.T) {
	bb := &TestBlockBuilder{Out: make(chan *TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}

	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}

	// This function takes a QC and makes it that it is signed by only 2 out of 4 nodes,
	// while still having a quorum of signatures.
	corruptQC := func(qc simplex.QuorumCertificate) simplex.QuorumCertificate {
		badQC := qc.(TestQC)
		// Duplicate the last signature
		badQC = append(badQC, badQC[len(badQC)-1])
		// Remove the first signature
		badQC = badQC[1:]

		// Finalization should have 3 signers
		require.Len(t, badQC.Signers(), 3)

		// But all these signers are either the second and third node.
		require.Contains(t, badQC.Signers(), nodes[1])
		require.Contains(t, badQC.Signers(), nodes[2])

		// Not the first or the fourth node.
		require.NotContains(t, badQC.Signers(), nodes[0])
		require.NotContains(t, badQC.Signers(), nodes[3])

		return badQC
	}

	quorum := simplex.Quorum(len(nodes))
	sentMessages := make(chan *simplex.Message, 100)
	conf, wal, _ := DefaultTestNodeEpochConfig(t, nodes[1], &recordingComm{
		Communication: NewNoopComm(nodes),
		SentMessages:  sentMessages,
	}, bb)
	conf.ReplicationEnabled = true

	e, err := simplex.NewEpoch(conf)
	require.NoError(t, err)
	require.NoError(t, e.Start())

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
	require.True(t, ok)
	require.Equal(t, md.Round, md.Seq)

	block := <-bb.Out

	finalization, _ := NewFinalizationRecord(t, e.Logger, e.SignatureAggregator, block, nodes[0:quorum])

	// Trigger the replication process to start by sending a finalization for a block we do not have
	e.HandleMessage(&simplex.Message{
		Finalization: &finalization,
	}, nodes[0])

	// Wait for the replication request to be sent
	for {
		msg := <-sentMessages
		if msg.ReplicationRequest != nil {
			break
		}
	}

	emptyNotarization := NewEmptyNotarization(nodes[0:quorum], 0)

	// Corrupt the QC
	emptyNotarization.QC = corruptQC(emptyNotarization.QC)

	// Respond to the replication request with a block that has a notarization
	replicationResponse := &simplex.ReplicationResponse{
		Data: []simplex.QuorumRound{
			{
				EmptyNotarization: emptyNotarization,
			},
		},
	}
	e.HandleMessage(&simplex.Message{
		ReplicationResponse: replicationResponse,
	}, nodes[0])

	require.Never(t, func() bool {
		return wal.ContainsEmptyNotarization(0)
	}, time.Millisecond*500, time.Millisecond*10, "Did not expect an empty notarization with a corrupt QC to be written to the WAL")
}

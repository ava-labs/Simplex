// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

func TestSimplexRebroadcastFinalizationVotes(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	net := testutil.NewInMemNetwork(t, nodes)

	var allowFinalizeVotes atomic.Bool

	var numFinalizeVotesSent atomic.Uint32

	config := func(from simplex.NodeID) *testutil.TestNodeConfig {
		return &testutil.TestNodeConfig{
			Comm: testutil.NewTestComm(from, net, func(msg *simplex.Message, from simplex.NodeID, to simplex.NodeID) bool {
				if msg.Finalization != nil && !allowFinalizeVotes.Load() {
					return false
				}
				if allowFinalizeVotes.Load() && msg.FinalizeVote != nil {
					numFinalizeVotesSent.Add(1)
				}
				return allowFinalizeVotes.Load() || msg.FinalizeVote == nil
			}),
		}
	}

	testutil.NewSimplexNode(t, nodes[0], net, config(nodes[0]))
	testutil.NewSimplexNode(t, nodes[1], net, config(nodes[1]))
	testutil.NewSimplexNode(t, nodes[2], net, config(nodes[2]))
	testutil.NewSimplexNode(t, nodes[3], net, config(nodes[3]))

	net.StartInstances()

	lastSeq := uint64(9)

	for seq := uint64(0); seq <= lastSeq; seq++ {
		for _, n := range net.Instances {
			testutil.WaitToEnterRound(t, n.E, seq)
		}
		net.TriggerLeaderBlockBuilder(seq)
		for _, n := range net.Instances {
			n.WAL.AssertNotarization(seq)
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(net.Instances))

	for _, n := range net.Instances {
		go func(n *testutil.TestNode) {
			defer wg.Done()
			n.Storage.EnsureNoBlockCommit(t, 0)
		}(n)
	}

	wg.Wait()

	allowFinalizeVotes.Store(true)

	require.Eventually(t, func() bool {
		var allHaveFinalized bool = true
		for _, n := range net.Instances {
			if n.Storage.NumBlocks() < lastSeq+1 {
				allHaveFinalized = false
				break
			}
		}

		if allHaveFinalized {
			return true
		}

		for _, n := range net.Instances {
			n.AdvanceTime(simplex.DefaultFinalizeVoteRebroadcastTimeout / 3)
		}

		return false

	}, time.Second*10, time.Millisecond*100)

	// Close the recorded messages channel. A message sent to this channel will cause a panic.
	finalizeVoteSentCount := numFinalizeVotesSent.Load()

	// Advance the time to make sure we do not continue to send finalize votes.
	for _, n := range net.Instances {
		n.AdvanceTime(simplex.DefaultFinalizeVoteRebroadcastTimeout * 2)
		n.AdvanceTime(simplex.DefaultFinalizeVoteRebroadcastTimeout * 2)
		n.AdvanceTime(simplex.DefaultFinalizeVoteRebroadcastTimeout * 2)
	}

	require.Equal(t, finalizeVoteSentCount, numFinalizeVotesSent.Load(), "no more finalize votes should have been sent")

	// Next, we run the nodes and notarize 100 blocks, and ensure that less than 400 finalize votes were sent.
	previousVoteSentCount := finalizeVoteSentCount // We copy the value just to give it a better name.
	for seq := lastSeq + 1; seq < lastSeq+101; seq++ {
		for _, n := range net.Instances {
			testutil.WaitToEnterRound(t, n.E, seq)
		}
		net.TriggerLeaderBlockBuilder(seq)
		for _, n := range net.Instances {
			n.Storage.WaitForBlockCommit(seq)
			n.AdvanceTime(simplex.DefaultFinalizeVoteRebroadcastTimeout)
		}
	}

	require.LessOrEqual(t, previousVoteSentCount+400, numFinalizeVotesSent.Load())

}

func TestSimplexMultiNodeSimple(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	net := testutil.NewInMemNetwork(t, nodes)
	testutil.NewSimplexNode(t, nodes[0], net, nil)
	testutil.NewSimplexNode(t, nodes[1], net, nil)
	testutil.NewSimplexNode(t, nodes[2], net, nil)
	testutil.NewSimplexNode(t, nodes[3], net, nil)

	net.StartInstances()

	for seq := uint64(0); seq < 10; seq++ {
		net.TriggerLeaderBlockBuilder(seq)
		for _, n := range net.Instances {
			n.Storage.WaitForBlockCommit(seq)
		}
	}
}

func dontVoteFilter(msg *simplex.Message, _, _ simplex.NodeID) bool {
	if msg.VoteMessage != nil || msg.EmptyVoteMessage != nil || msg.Notarization != nil || msg.FinalizeVote != nil {
		return false
	}
	return true
}

func TestSimplexMultiNodeBlacklist(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	net := testutil.NewInMemNetwork(t, nodes)
	testEpochConfig := &testutil.TestNodeConfig{
		ReplicationEnabled: true,
	}
	testutil.NewSimplexNode(t, nodes[0], net, testEpochConfig)
	testutil.NewSimplexNode(t, nodes[1], net, testEpochConfig)
	testutil.NewSimplexNode(t, nodes[2], net, testEpochConfig)
	testutil.NewSimplexNode(t, nodes[3], net, testEpochConfig)

	net.StartInstances()

	// Advance to the fourth node's turn by building three blocks
	for seq := 0; seq < 3; seq++ {
		net.TriggerLeaderBlockBuilder(uint64(seq))
		for _, n := range net.Instances {
			n.Storage.WaitForBlockCommit(uint64(seq))
		}
	}

	// The fourth node is disconnected, so the rest should time out on it.
	net.Disconnect(nodes[3])

	for i := range net.Instances[:3] {
		select {
		case net.Instances[i].BB.BlockShouldBeBuilt <- struct{}{}:
		default:

		}
	}

	for _, n := range net.Instances[:3] {
		testutil.WaitForBlockProposerTimeout(t, n.E, &n.E.StartTime, 3)
	}

	// Build two more blocks, which should blacklist the fourth node.
	// Ensure the fourth node is blacklisted by checking the blacklist on all nodes.

	// Build a block, ensure the blacklist contains the fourth node as a suspect.
	net.TriggerLeaderBlockBuilder(uint64(4))
	for _, n := range net.Instances[:3] {
		block := n.Storage.WaitForBlockCommit(uint64(3))
		blacklist := block.Blacklist()
		require.Equal(t, simplex.Blacklist{
			NodeCount:      4,
			SuspectedNodes: simplex.SuspectedNodes{{NodeIndex: 3, SuspectingCount: 1, OrbitSuspected: 1}},
			Updates:        []simplex.BlacklistUpdate{{NodeIndex: 3, Type: simplex.BlacklistOpType_NodeSuspected}},
		}, blacklist)
	}

	net.SetNodeMessageFilter(nodes[3], dontVoteFilter)
	// Reconnect the fourth node.
	net.Connect(nodes[3])

	// Build another block, ensure the blacklist contains the fourth node as blacklisted.
	net.Instances[1].BB.TriggerNewBlock()
	for _, n := range net.Instances[:3] {
		block := n.Storage.WaitForBlockCommit(uint64(4))
		blacklist := block.Blacklist()
		require.Equal(t, simplex.Blacklist{
			NodeCount:      4,
			SuspectedNodes: simplex.SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 1}},
			Updates:        []simplex.BlacklistUpdate{{NodeIndex: 3, Type: simplex.BlacklistOpType_NodeSuspected}},
		}, blacklist)
	}

	// Wait for the node to replicate the missing blocks.
	net.Instances[3].BB.TriggerNewBlock()
	block := net.Instances[3].Storage.WaitForBlockCommit(4)
	require.Equal(t, simplex.Blacklist{
		NodeCount:      4,
		SuspectedNodes: simplex.SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 1}},
		Updates:        []simplex.BlacklistUpdate{{NodeIndex: 3, Type: simplex.BlacklistOpType_NodeSuspected}},
	}, block.Blacklist())

	// Make another block.
	net.Instances[2].BB.TriggerNewBlock()
	for _, n := range net.Instances {
		n.Storage.WaitForBlockCommit(uint64(5))
	}

	// The fourth node is still blacklisted, so it should not be able to propose a block.
	net.Instances[3].BB.TriggerNewBlock() // This shouldn't be here, this is just to side-step a bug.
	for _, n := range net.Instances {
		testutil.WaitForBlockProposerTimeout(t, n.E, &n.E.StartTime, 7)
	}

	// Disconnect the third node to force messages from the fourth node to be taken into account.
	net.Disconnect(nodes[2])
	net.SetNodeMessageFilter(nodes[3], testutil.AllowAllMessages)
	// Make two blocks.
	allButThirdNode := []*testutil.TestNode{net.Instances[0], net.Instances[1], net.Instances[3]}
	for i := 0; i < 2; i++ {
		net.Instances[i].BB.TriggerNewBlock()
		for _, n := range allButThirdNode {
			n.BB.BlockShouldBeBuilt <- struct{}{}
			n.Storage.WaitForBlockCommit(uint64(6 + i))
		}
	}

	// Skip the third node because it is disconnected.
	for i := range allButThirdNode {
		select {
		case net.Instances[i].BB.BlockShouldBeBuilt <- struct{}{}:
		default:

		}
	}

	for _, n := range allButThirdNode {
		testutil.WaitForBlockProposerTimeout(t, n.E, &n.E.StartTime, 10)
	}

	// Since the fourth node is still blacklisted, we should skip this round.
	for _, n := range allButThirdNode {
		testutil.WaitForBlockProposerTimeout(t, n.E, &n.E.StartTime, 11)
	}

	var lastBlacklist simplex.Blacklist

	// Create two blocks
	for i := 0; i < 2; i++ {
		net.Instances[i].BB.TriggerNewBlock()
		for _, n := range allButThirdNode {
			n.BB.BlockShouldBeBuilt <- struct{}{}
			block := n.Storage.WaitForBlockCommit(uint64(8 + i))
			lastBlacklist = block.Blacklist()
		}
	}

	// The last node is not suspected anymore, it has been redeemed.
	require.False(t, lastBlacklist.IsNodeSuspected(3))

	// The third node will now time out.
	for i := range allButThirdNode {
		select {
		case net.Instances[i].BB.BlockShouldBeBuilt <- struct{}{}:
		default:

		}
	}

	for _, n := range allButThirdNode {
		testutil.WaitForBlockProposerTimeout(t, n.E, &n.E.StartTime, 14)
	}

	// The fourth node should now be able to propose a block.
	net.Instances[3].BB.TriggerNewBlock()
	for _, n := range allButThirdNode {
		n.BB.BlockShouldBeBuilt <- struct{}{}
		block := n.Storage.WaitForBlockCommit(uint64(10))
		lastBlacklist = block.Blacklist()
	}
}

func onlySendBlockProposalsAndVotes(splitNodes []simplex.NodeID) testutil.MessageFilter {
	return func(m *simplex.Message, from simplex.NodeID, to simplex.NodeID) bool {
		if m.BlockMessage != nil {
			return true
		}
		for _, splitNode := range splitNodes {
			if to.Equals(splitNode) {
				return false
			}
		}
		return true
	}
}

// TestSplitVotes ensures that nodes who have timeout out, while the rest of the network has
// progressed due to notarizations, are able to collect the notarizations and continue
func TestSplitVotes(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	net := testutil.NewInMemNetwork(t, nodes)

	config := func(from simplex.NodeID) *testutil.TestNodeConfig {
		return &testutil.TestNodeConfig{
			Comm: testutil.NewTestComm(from, net, onlySendBlockProposalsAndVotes(nodes[2:])),
		}
	}

	testutil.NewSimplexNode(t, nodes[0], net, config(nodes[0]))
	testutil.NewSimplexNode(t, nodes[1], net, config(nodes[1]))
	splitNode2 := testutil.NewSimplexNode(t, nodes[2], net, config(nodes[2]))
	splitNode3 := testutil.NewSimplexNode(t, nodes[3], net, config(nodes[3]))

	net.StartInstances()

	net.TriggerLeaderBlockBuilder(0)
	for _, n := range net.Instances {
		n.WAL.AssertBlockProposal(0)
		n.TriggerBlockShouldBeBuilt()

		if n.E.ID.Equals(splitNode2.E.ID) || n.E.ID.Equals(splitNode3.E.ID) {
			require.Equal(t, uint64(0), n.E.Metadata().Round)
			testutil.WaitForBlockProposerTimeout(t, n.E, &n.E.StartTime, 0)
			require.False(t, n.WAL.ContainsNotarization(0))
		} else {
			n.WAL.AssertNotarization(0)
			require.Equal(t, uint64(1), n.E.Metadata().Round)
		}
	}

	net.SetAllNodesMessageFilter(testutil.AllowAllMessages)

	time2 := splitNode2.E.StartTime
	time3 := splitNode3.E.StartTime

	for {
		time2 = time2.Add(splitNode2.E.EpochConfig.MaxRebroadcastWait / 3)
		splitNode2.E.AdvanceTime(time2)

		time3 = time3.Add(splitNode3.E.EpochConfig.MaxRebroadcastWait / 3)
		splitNode3.E.AdvanceTime(time3)
		if splitNode2.WAL.ContainsNotarization(0) && splitNode3.WAL.ContainsNotarization(0) {
			break
		}
	}

	// splitNode3 will receive the notarization from splitNode2
	splitNode2.WAL.AssertNotarization(0)
	splitNode3.WAL.AssertNotarization(0)

	for _, n := range net.Instances {
		require.Equal(t, uint64(0), n.Storage.NumBlocks())
		require.Equal(t, uint64(1), n.E.Metadata().Round)
		require.Equal(t, uint64(1), n.E.Metadata().Seq)
	}

	// once the new round gets finalized, it will re-broadcast
	// all past notarizations allowing the nodes to index both seq 0 & 1
	net.TriggerLeaderBlockBuilder(1)

	for _, n := range net.Instances {
		n.Storage.WaitForBlockCommit(0)
		n.Storage.WaitForBlockCommit(1)
		require.Equal(t, uint64(2), n.Storage.NumBlocks())
		require.Equal(t, uint64(2), n.E.Metadata().Round)
		require.Equal(t, uint64(2), n.E.Metadata().Seq)
	}
}

// denyFinalizationMessages blocks any messages that would cause nodes in
// a network to index a block in storage.
func denyFinalizationMessages(msg *simplex.Message, _, _ simplex.NodeID) bool {
	if msg.FinalizeVote != nil {
		return false
	}
	if msg.Finalization != nil {
		return false
	}

	return true
}

func onlyAllowEmptyRoundMessages(msg *simplex.Message, _, _ simplex.NodeID) bool {
	if msg.EmptyNotarization != nil {
		return true
	}
	if msg.EmptyVoteMessage != nil {
		return true
	}
	return false
}

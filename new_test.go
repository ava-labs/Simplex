package simplex_test

// import (
// 	"bytes"
// 	"testing"
// 	"time"

// 	"github.com/ava-labs/simplex"
// 	"github.com/ava-labs/simplex/testutil"
// 	. "github.com/ava-labs/simplex/testutil"

// 	"github.com/stretchr/testify/require"
// )

// // TestReplicationVotesForNotarizations tests that a lagging node will replicate
// // finalizations and notarizations. It ensures the node sends finalized votes for rounds
// // without finalizations.
// func TestReplicationVotesForNotarizations(t *testing.T) {
// 	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}

// 	// TODO: numFinalized and numNotarized could be parameterized to test different scenarios
// 	numFinalizedBlocks := uint64(5)
// 	// number of notarized blocks after the finalized blocks
// 	numNotarizedBlocks := uint64(11)
// 	net := NewInMemNetwork(t, nodes)

// 	storageData := createBlocks(t, nodes, numFinalizedBlocks)
// 	nodeConfig := func(from simplex.NodeID) *TestNodeConfig {
// 		comm := NewTestComm(from, net, almostFinalizeBlocks)
// 		return &TestNodeConfig{
// 			InitialStorage:     storageData,
// 			Comm:               comm,
// 			ReplicationEnabled: true,
// 		}
// 	}

// 	n1 := NewSimplexNode(t, nodes[0], net, nodeConfig(nodes[0]))
// 	n2 := NewSimplexNode(t, nodes[1], net, nodeConfig(nodes[1]))
// 	adversary := NewSimplexNode(t, nodes[2], net, nodeConfig(nodes[2]))
// 	laggingNode := NewSimplexNode(t, nodes[3], net, &TestNodeConfig{
// 		ReplicationEnabled: true,
// 	})

// 	for _, n := range net.Instances {
// 		if n.E.ID.Equals(laggingNode.E.ID) {
// 			require.Equal(t, uint64(0), n.Storage.NumBlocks())
// 			continue
// 		}
// 		require.Equal(t, numFinalizedBlocks, n.Storage.NumBlocks())
// 	}

// 	// lagging node should be disconnected while nodes create notarizations without finalizations
// 	net.Disconnect(laggingNode.E.ID)

// 	net.StartInstances()

// 	missedSeqs := uint64(0)
// 	// normal nodes continue to make progress
// 	for round := numFinalizedBlocks; round < numFinalizedBlocks+numNotarizedBlocks; round++ {
// 		emptyRound := bytes.Equal(simplex.LeaderForRound(nodes, round), laggingNode.E.ID)
// 		if emptyRound {
// 			missedSeqs++
// 			net.AdvanceWithoutLeader(round, laggingNode.E.ID)
// 		} else {
// 			net.TriggerLeaderBlockBuilder(round)
// 			for _, n := range net.Instances {
// 				if n.E.ID.Equals(laggingNode.E.ID) {
// 					continue
// 				}
// 				n.WAL.AssertNotarization(round)
// 			}
// 		}
// 	}

// 	// all nodes should be on round [numFinalizedBlocks + numNotarizedBlocks - 1]
// 	for _, n := range net.Instances {
// 		if n.E.ID.Equals(laggingNode.E.ID) {
// 			require.Equal(t, uint64(0), n.Storage.NumBlocks())
// 			require.Equal(t, uint64(0), n.E.Metadata().Round)
// 			continue
// 		}
// 		require.Equal(t, numFinalizedBlocks, n.Storage.NumBlocks())
// 		require.Equal(t, numFinalizedBlocks+numNotarizedBlocks, n.E.Metadata().Round)
// 	}

// 	// at this point in time, the adversarial node will disconnect
// 	// since each node has sent 2 finalized votes, which is one short of a quorum
// 	// the lagging node will need to replicate the finalizations, and then send votes for notarizations
// 	net.Disconnect(adversary.E.ID)
// 	net.Connect(laggingNode.E.ID)
// 	net.SetAllNodesMessageFilter(AllowAllMessages)

// 	// the adversary should not be the leader(to simplify test)
// 	isAdversaryLeader := bytes.Equal(simplex.LeaderForRound(nodes, numFinalizedBlocks+numNotarizedBlocks), adversary.E.ID)
// 	require.False(t, isAdversaryLeader)

// 	// lagging node should not be leader
// 	isLaggingNodeLeader := bytes.Equal(simplex.LeaderForRound(nodes, numFinalizedBlocks+numNotarizedBlocks), laggingNode.E.ID)
// 	require.False(t, isLaggingNodeLeader)

// 	// trigger block building, but we only have 2 connected nodes so the nodes will time out
// 	net.TriggerLeaderBlockBuilder(numFinalizedBlocks + numNotarizedBlocks)

// 	// ensure time out on required nodes
// 	n1.TimeoutOnRound(numFinalizedBlocks + numNotarizedBlocks)
// 	n2.TimeoutOnRound(numFinalizedBlocks + numNotarizedBlocks)
// 	require.Equal(t, uint64(0), laggingNode.E.Metadata().Round)
// 	laggingNode.TimeoutOnRound(0)

// 	expectedNumBlocks := numFinalizedBlocks + numNotarizedBlocks - missedSeqs
// 	// because the adversarial node is offline , we may need to send replication requests many times
// 	for {
// 		time.Sleep(time.Millisecond * 100)
// 		if laggingNode.Storage.NumBlocks() == expectedNumBlocks {
// 			break
// 		}

// 		laggingNode.AdvanceTime(simplex.DefaultReplicationRequestTimeout)
// 	}

// 	for _, n := range net.Instances {
// 		if n.E.ID.Equals(adversary.E.ID) {
// 			continue
// 		}
// 		n.Storage.WaitForBlockCommit(expectedNumBlocks - 1) // subtract -1 because seq starts at 0
// 	}

// 	laggingNode.TimeoutOnRound(numFinalizedBlocks + numNotarizedBlocks)
// 	for _, n := range net.Instances {
// 		if n.E.ID.Equals(adversary.E.ID) {
// 			continue
// 		}
// 		WaitToEnterRound(t, n.E, numFinalizedBlocks+numNotarizedBlocks+1)
// 		require.True(t, n.WAL.ContainsEmptyNotarization(numFinalizedBlocks+numNotarizedBlocks))
// 	}
// }

// func TestReplicationRequestUnknownSeqsAndRounds(t *testing.T) {
// 	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
// 	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
// 	comm := NewListenerComm(nodes)
// 	conf, _, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], comm, bb)
// 	conf.ReplicationEnabled = true

// 	e, err := simplex.NewEpoch(conf)
// 	require.NoError(t, err)
// 	require.NoError(t, e.Start())

// 	req := &simplex.Message{
// 		ReplicationRequest: &simplex.ReplicationRequest{
// 			Rounds:      []uint64{100, 101, 102},
// 			Seqs:        []uint64{200, 201, 202},
// 			LatestRound: 1,
// 		},
// 	}

// 	err = e.HandleMessage(req, nodes[1])
// 	require.NoError(t, err)

// 	require.Never(t, func() bool { return len(comm.in) > 0 }, 5*time.Second, 100*time.Millisecond)
// }

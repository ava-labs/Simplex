package simplex_test

import (
	"simplex"
	"testing"

	"github.com/stretchr/testify/require"
)

func rejectReplicationRequests(msg *simplex.Message, from simplex.NodeID) bool {
	if msg.ReplicationRequest != nil {
		return false
	}

	return true
}

// A node attempts to request blocks to replicate, but fails to receive them
func TestReplicationRequestTimeout(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	startSeq := uint64(8)

	// node begins replication
	bb := newTestControlledBlockBuilder(t)
	net := newInMemNetwork(t, nodes)

	storageData := createBlocks(t, nodes, &bb.testBlockBuilder, startSeq)

	newNodeConfig := func(from simplex.NodeID) *testNodeConfig {
		comm := newTestComm(from, net, rejectReplicationRequests)
		return &testNodeConfig{
			initialStorage:     storageData,
			comm:               comm,
			replicationEnabled: true,
		}
	}

	newSimplexNode(t, nodes[0], net, bb, newNodeConfig(nodes[0]))
	newSimplexNode(t, nodes[1], net, bb, newNodeConfig(nodes[1]))
	newSimplexNode(t, nodes[2], net, bb, newNodeConfig(nodes[2]))
	laggingNode := newSimplexNode(t, nodes[3], net, bb, &testNodeConfig{
		replicationEnabled: true,
	})

	net.startInstances()
	bb.triggerNewBlock()

	// typically the lagging node would catch up here, but since we block
	// replication requests, the lagging node will be forced to resend requests after a timeout
	for i := 0; i <= int(startSeq); i++ {
		for _, n := range net.instances {
			if n.e.ID.Equals(laggingNode.e.ID) {
				continue
			}
			n.storage.waitForBlockCommit(uint64(startSeq))
		}
	}

	// assert the lagging node has not received any replicaiton requests
	require.Equal(t, uint64(0), laggingNode.storage.Height())

	// after the timeout, the nodes should respond and the lagging node will replicate
	net.setAllNodesMessageFilter(allowAllMessages)
	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout / 2))

	require.Equal(t, uint64(0), laggingNode.storage.Height())
	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout * 2))

	laggingNode.storage.waitForBlockCommit(uint64(startSeq))
}

package testutil

import (
	"bytes"
	"simplex"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type inMemNetwork struct {
	t            *testing.T
	nodes        []simplex.NodeID
	instances    []*testNode
	lock         sync.RWMutex
	disconnected map[string]struct{}
}

// NewInMemNetwork creates an in-memory network. Node IDs must be provided before
// adding instances, as nodes require prior knowledge of all participants.
func NewInMemNetwork(t *testing.T, nodes []simplex.NodeID) *inMemNetwork {
	net := &inMemNetwork{
		t:            t,
		nodes:        nodes,
		instances:    make([]*testNode, 0),
		disconnected: make(map[string]struct{}),
	}
	return net
}

func (n *inMemNetwork) addNode(node *testNode) {
	allowed := false
	for _, id := range n.nodes {
		if bytes.Equal(id, node.E.ID) {
			allowed = true
			break
		}
	}
	require.True(node.t, allowed, "node must be declared before adding")
	n.instances = append(n.instances, node)
}

func (n *inMemNetwork) SetAllNodesMessageFilter(filter messageFilter) {
	for _, instance := range n.instances {
		instance.E.Comm.(*testComm).setFilter(filter)
	}
}

func (n *inMemNetwork) IsDisconnected(node simplex.NodeID) bool {
	n.lock.RLock()
	defer n.lock.RUnlock()

	_, ok := n.disconnected[string(node)]
	return ok
}

func (n *inMemNetwork) Connect(node simplex.NodeID) {
	n.lock.Lock()
	defer n.lock.Unlock()

	delete(n.disconnected, string(node))
}

func (n *inMemNetwork) Disconnect(node simplex.NodeID) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.disconnected[string(node)] = struct{}{}
}

func (n *inMemNetwork) Instances() []*testNode {
	return n.instances
}

// startInstances starts all instances in the network.
// The first one is typically the leader, so we make sure to start it last.
func (n *inMemNetwork) StartInstances() {
	require.Equal(n.t, len(n.nodes), len(n.instances))

	for i := len(n.nodes) - 1; i >= 0; i-- {
		n.instances[i].Start()
	}
}

// AdvanceWithoutLeader advances the network to the next round without the leader proposing a block.
// This is useful for testing scenarios where the leader is lagging behind.
func AdvanceWithoutLeader(t *testing.T, net *inMemNetwork, bb *testControlledBlockBuilder, epochTimes []time.Time, round uint64, laggingNodeId simplex.NodeID) {
	// we need to ensure all blocks are waiting for the channel before proceeding
	// otherwise, we may send to a channel that is not ready to receive
	for _, n := range net.instances {
		if laggingNodeId.Equals(n.E.ID) {
			continue
		}

		n.WaitToEnterRound(round)
	}

	for _, n := range net.instances {
		leader := n.E.ID.Equals(simplex.LeaderForRound(net.nodes, n.E.Metadata().Round))
		if leader || laggingNodeId.Equals(n.E.ID) {
			continue
		}
		bb.BlockShouldBeBuilt <- struct{}{}
	}

	for i, n := range net.instances {
		// the leader will not write an empty vote to the wal
		// because it cannot both propose a block & send an empty vote in the same round
		leader := n.E.ID.Equals(simplex.LeaderForRound(net.nodes, n.E.Metadata().Round))
		if leader || laggingNodeId.Equals(n.E.ID) {
			continue
		}
		n.waitForBlockProposerTimeout(&epochTimes[i], round)
	}

	for _, n := range net.instances {
		if laggingNodeId.Equals(n.E.ID) {
			continue
		}
		n.Wal.AssertNotarization(round)
	}
}

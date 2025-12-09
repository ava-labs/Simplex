// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package testutil

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/record"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type InMemNetwork struct {
	t            *testing.T
	nodes        []simplex.NodeID
	Instances    []*TestNode
	lock         sync.RWMutex
	disconnected map[string]struct{}
}

// NewInMemNetwork creates an in-memory network. Node IDs must be provided before
// adding instances, as nodes require prior knowledge of all participants.
func NewInMemNetwork(t *testing.T, nodes []simplex.NodeID) *InMemNetwork {
	simplex.SortNodes(nodes)
	net := &InMemNetwork{
		t:            t,
		nodes:        nodes,
		Instances:    make([]*TestNode, 0),
		disconnected: make(map[string]struct{}),
	}
	return net
}

// StartInstances starts all instances in the network.
// The first one is typically the leader, so we make sure to start it last.
func (n *InMemNetwork) StartInstances() {
	require.Equal(n.t, len(n.nodes), len(n.Instances))

	for i := len(n.nodes) - 1; i >= 0; i-- {
		n.Instances[i].Start()
	}
}

func (n *InMemNetwork) TriggerLeaderBlockBuilder(round uint64) {
	leader := simplex.LeaderForRound(n.nodes, round)
	for _, instance := range n.Instances {
		if !instance.E.ID.Equals(leader) {
			continue
		}
		if n.IsDisconnected(leader) {
			instance.E.Logger.Info("triggering block build on disconnected leader", zap.Stringer("leader", leader))
		}

		// wait for the node to enter the round we expect it to propose a block for
		// otherwise we may trigger a build block too early
		WaitToEnterRound(n.t, instance.E, round)

		instance.BB.TriggerNewBlock()
		return
	}

	// we should always find the leader
	require.Fail(n.t, "leader not found")
}

func (n *InMemNetwork) addNode(node *TestNode) {
	allowed := false
	for _, id := range n.nodes {
		if bytes.Equal(id, node.E.ID) {
			allowed = true
			break
		}
	}
	require.True(node.t, allowed, "node must be declared before adding")
	n.Instances = append(n.Instances, node)
}

type TestNetworkCommunication interface {
	simplex.Communication
	SetFilter(filter MessageFilter)
}

func (n *InMemNetwork) SetNodeMessageFilter(node simplex.NodeID, filter MessageFilter) {
	for _, instance := range n.Instances {
		if !instance.E.ID.Equals(node) {
			continue
		}
		comm, ok := instance.E.Comm.(TestNetworkCommunication)
		if !ok {
			continue
		}
		comm.SetFilter(filter)
	}
}

func (n *InMemNetwork) SetAllNodesMessageFilter(filter MessageFilter) {
	for _, instance := range n.Instances {
		comm, ok := instance.E.Comm.(TestNetworkCommunication)
		if !ok {
			continue
		}
		comm.SetFilter(filter)
	}
}

func (n *InMemNetwork) IsDisconnected(node simplex.NodeID) bool {
	n.lock.RLock()
	defer n.lock.RUnlock()

	_, ok := n.disconnected[string(node)]
	return ok
}

func (n *InMemNetwork) Connect(node simplex.NodeID) {
	n.lock.Lock()
	defer n.lock.Unlock()

	delete(n.disconnected, string(node))
}

func (n *InMemNetwork) Disconnect(node simplex.NodeID) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.disconnected[string(node)] = struct{}{}
}

func (n *InMemNetwork) AdvanceWithoutLeader(round uint64, laggingNodeId simplex.NodeID) {
	// we need to ensure all blocks are waiting for the channel before proceeding
	// otherwise, we may send to a channel that is not ready to receive
	for _, n := range n.Instances {
		if laggingNodeId.Equals(n.E.ID) {
			continue
		}

		WaitToEnterRound(n.t, n.E, round)
	}

	for _, n := range n.Instances {
		n.BB.TriggerBlockShouldBeBuilt()
	}

	for _, n := range n.Instances {
		leader := n.E.ID.Equals(simplex.LeaderForRound(n.E.Comm.Nodes(), n.E.Metadata().Round))
		if leader || laggingNodeId.Equals(n.E.ID) {
			continue
		}
		epochTime := time.UnixMilli(n.currentTime.Load())
		WaitForBlockProposerTimeout(n.t, n.E, &epochTime, round)
	}

	for _, n := range n.Instances {
		if laggingNodeId.Equals(n.E.ID) {
			continue
		}
		recordType := n.WAL.AssertNotarization(round)
		require.Equal(n.t, record.EmptyNotarizationRecordType, recordType)
	}
}

func (n *InMemNetwork) AdvanceTime(duration time.Duration) {
	for _, instance := range n.Instances {
		instance.AdvanceTime(duration)
	}
}

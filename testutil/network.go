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

type BasicInMemoryNetwork struct {
	t            *testing.T
	nodes        []simplex.NodeID
	lock         sync.RWMutex
	disconnected map[string]struct{}
	instances    []*BasicNode
}

func NewBasicInMemoryNetwork(t *testing.T, nodes []simplex.NodeID) *BasicInMemoryNetwork {
	simplex.SortNodes(nodes)
	return &BasicInMemoryNetwork{
		t:            t,
		nodes:        nodes,
		disconnected: make(map[string]struct{}),
		instances:    make([]*BasicNode, 0),
	}
}

type TestNetworkCommunication interface {
	simplex.Communication
	SetFilter(filter MessageFilter)
}

func (b *BasicInMemoryNetwork) SetNodeMessageFilter(node simplex.NodeID, filter MessageFilter) {
	for _, instance := range b.instances {
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

func (b *BasicInMemoryNetwork) SetAllNodesMessageFilter(filter MessageFilter) {
	for _, instance := range b.instances {
		comm, ok := instance.E.Comm.(TestNetworkCommunication)
		if !ok {
			continue
		}
		comm.SetFilter(filter)
	}
}

func (b *BasicInMemoryNetwork) IsDisconnected(node simplex.NodeID) bool {
	b.lock.RLock()
	defer b.lock.RUnlock()

	_, ok := b.disconnected[string(node)]
	return ok
}

func (b *BasicInMemoryNetwork) Connect(node simplex.NodeID) {
	b.lock.Lock()
	defer b.lock.Unlock()

	delete(b.disconnected, string(node))
}

func (b *BasicInMemoryNetwork) Disconnect(node simplex.NodeID) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.disconnected[string(node)] = struct{}{}
}

func (b *BasicInMemoryNetwork) AdvanceTime(increment time.Duration) {
	for _, instance := range b.instances {
		instance.AdvanceTime(increment)
	}
}

// StartInstances starts all instances in the network.
// The first one is typically the leader, so we make sure to start it last.
func (b *BasicInMemoryNetwork) StartInstances() {
	require.Equal(b.t, len(b.nodes), len(b.instances))

	for i := len(b.nodes) - 1; i >= 0; i-- {
		b.instances[i].Start()
	}
}

func (b *BasicInMemoryNetwork) StopInstances() {
	for _, instance := range b.instances {
		instance.Stop()
	}
}

func (b *BasicInMemoryNetwork) AddNode(node *BasicNode) {
	allowed := false
	for _, id := range b.nodes {
		if bytes.Equal(id, node.E.ID) {
			allowed = true
			break
		}
	}
	require.True(node.t, allowed, "node must be declared before adding")
	b.instances = append(b.instances, node)
}

func (b *BasicInMemoryNetwork) RemoveNode(node *BasicNode) {
	b.lock.Lock()
	defer b.lock.Unlock()

	for i, instance := range b.instances {
		if instance == node {
			// Remove the instance from the slice
			b.instances = append(b.instances[:i], b.instances[i+1:]...)
			return
		}
	}
}

type ControlledInMemoryNetwork struct {
	*BasicInMemoryNetwork
	Instances []*ControlledNode
}

// NewControlledNetwork creates an in-memory network. Node IDs must be provided before
// adding instances, as nodes require prior knowledge of all participants.
func NewControlledNetwork(t *testing.T, nodes []simplex.NodeID) *ControlledInMemoryNetwork {
	simplex.SortNodes(nodes)
	net := &ControlledInMemoryNetwork{
		BasicInMemoryNetwork: NewBasicInMemoryNetwork(t, nodes),
		Instances:            make([]*ControlledNode, 0),
	}
	return net
}

func (n *ControlledInMemoryNetwork) TriggerLeaderBlockBuilder(round uint64) {
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

		instance.TriggerNewBlock()
		return
	}

	// we should always find the leader
	require.Fail(n.t, "leader not found")
}

func (n *ControlledInMemoryNetwork) addNode(node *ControlledNode) {
	n.BasicInMemoryNetwork.AddNode(node.BasicNode)
	n.Instances = append(n.Instances, node)
}

func (n *ControlledInMemoryNetwork) AdvanceWithoutLeader(round uint64, laggingNodeId simplex.NodeID) {
	// we need to ensure all blocks are waiting for the channel before proceeding
	// otherwise, we may send to a channel that is not ready to receive
	for _, n := range n.Instances {
		if laggingNodeId.Equals(n.E.ID) {
			continue
		}

		WaitToEnterRound(n.t, n.E, round)
	}

	for _, n := range n.Instances {
		n.BlockShouldBeBuilt()
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

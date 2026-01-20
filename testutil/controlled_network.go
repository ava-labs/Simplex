// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.
package testutil

import (
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/record"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

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

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.
package testutil

import (
	"context"
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

type ControlledNode struct {
	*BasicNode
	bb      *testControlledBlockBuilder
	WAL     *TestWAL
	Storage *InMemStorage
}

// newSimplexNode creates a new testNode and adds it to [net].
func NewControlledSimplexNode(t *testing.T, nodeID simplex.NodeID, net *ControlledInMemoryNetwork, config *TestNodeConfig) *ControlledNode {
	comm := NewTestComm(nodeID, net.BasicInMemoryNetwork, AllowAllMessages)
	bb := NewTestControlledBlockBuilder(t)
	if config != nil && config.BlockBuilder != nil {
		bb = config.BlockBuilder
	}

	epochConfig, wal, storage := DefaultTestNodeEpochConfig(t, nodeID, comm, bb)

	if config != nil {
		UpdateEpochConfig(&epochConfig, config)
		if config.WAL != nil {
			wal = config.WAL
		}
		if config.Storage != nil {
			storage = config.Storage
		}
	}

	e, err := simplex.NewEpoch(epochConfig)
	require.NoError(t, err)
	ti := &ControlledNode{
		BasicNode: NewBasicNode(t, e, epochConfig.Logger.(*TestLogger)),
		WAL:       wal,
		bb:        bb,
		Storage:   storage,
	}

	net.addNode(ti)
	return ti
}

// TimeoutOnRound advances time until the node times out of the given round.
func (t *ControlledNode) TimeoutOnRound(round uint64) {
	for {
		currentRound := t.E.Metadata().Round
		if currentRound > round {
			return
		}

		if !t.ShouldBlockBeBuilt() {
			t.BlockShouldBeBuilt()
		}

		t.AdvanceTime(t.E.MaxProposalWait)

		// check the wal for an empty vote for that round
		if hasVote := t.WAL.ContainsEmptyVote(round); hasVote {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (t *ControlledNode) ShouldBlockBeBuilt() bool {
	return len(t.bb.BlockShouldBeBuilt) > 0
}

func (t *ControlledNode) TriggerNewBlock() {
	t.bb.TriggerNewBlock()
}
func (t *ControlledNode) BlockShouldBeBuilt() {
	select {
	case t.bb.BlockShouldBeBuilt <- struct{}{}:
	default:
	}
}

func (t *ControlledNode) TickUntilRoundAdvanced(round uint64, tick time.Duration) {
	timeout := time.NewTimer(time.Minute)
	defer timeout.Stop()

	for {
		if t.E.Metadata().Round >= round {
			return
		}

		select {
		case <-time.After(time.Millisecond * 10):
			t.AdvanceTime(tick)
			continue
		case <-timeout.C:
			require.Fail(t.t, "timed out waiting to enter round", "current round %d, waiting for round %d", t.E.Metadata().Round, round)
		}
	}
}


// testControlledBlockBuilder is a BlockBuilder that only builds a block when
// a control signal is received.
type testControlledBlockBuilder struct {
	t       *testing.T
	control chan struct{}
	TestBlockBuilder
}

// NewTestControlledBlockBuilder returns a BlockBuilder that only builds a block
// when triggerNewBlock is called.
func NewTestControlledBlockBuilder(t *testing.T) *testControlledBlockBuilder {
	return &testControlledBlockBuilder{
		t:                t,
		control:          make(chan struct{}, 1),
		TestBlockBuilder: *NewTestBlockBuilder(),
	}
}

func (t *testControlledBlockBuilder) TriggerNewBlock() {
	select {
	case t.control <- struct{}{}:
	default:
	}
}

func (t *testControlledBlockBuilder) BuildBlock(ctx context.Context, metadata simplex.ProtocolMetadata, blacklist simplex.Blacklist) (simplex.VerifiedBlock, bool) {
	select {
	case <-t.control:
	case <-ctx.Done():
		return nil, false
	}
	return t.TestBlockBuilder.BuildBlock(ctx, metadata, blacklist)
}

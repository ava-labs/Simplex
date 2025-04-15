// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"simplex"
	"testing"

	"github.com/stretchr/testify/require"
)

func rejectReplicationRequests(msg *simplex.Message, from simplex.NodeID) bool {
	return msg.ReplicationRequest == nil && msg.ReplicationResponse == nil && msg.VerifiedReplicationResponse == nil
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

type testTimeoutMessageFilter struct {
	t *testing.T

	replicationResponses chan struct{}
}

func (m *testTimeoutMessageFilter) failOnReplicationRequest(msg *simplex.Message, from simplex.NodeID) bool {
	require.Nil(m.t, msg.ReplicationRequest)
	return true
}

func (m *testTimeoutMessageFilter) receivedReplicationRequest(msg *simplex.Message, from simplex.NodeID) bool {
	if msg.VerifiedReplicationResponse != nil || msg.ReplicationResponse != nil {
		m.replicationResponses <- struct{}{}
		return false
	}

	return true
}

func TestReplicationRequestTimeoutCancels(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	startSeq := uint64(8)

	bb := newTestControlledBlockBuilder(t)
	net := newInMemNetwork(t, nodes)

	// initiate a network with 4 nodes. one node is behind by startSeq blocks
	storageData := createBlocks(t, nodes, &bb.testBlockBuilder, startSeq)
	testEpochConfig := &testNodeConfig{
		initialStorage:     storageData,
		replicationEnabled: true,
	}
	newSimplexNode(t, nodes[0], net, bb, testEpochConfig)
	newSimplexNode(t, nodes[1], net, bb, testEpochConfig)
	newSimplexNode(t, nodes[2], net, bb, testEpochConfig)
	laggingNode := newSimplexNode(t, nodes[3], net, bb, &testNodeConfig{
		replicationEnabled: true,
	})

	net.startInstances()
	bb.triggerNewBlock()

	// all blocks except the lagging node start at round 8, seq 8.
	// lagging node starts at round 0, seq 0.
	// this asserts that the lagging node catches up to the latest round
	for i := 0; i <= int(startSeq); i++ {
		for _, n := range net.instances {
			n.storage.waitForBlockCommit(uint64(startSeq))
		}
	}

	// ensure lagging node doesn't resend requests
	mf := &testTimeoutMessageFilter{
		t: t,
	}
	laggingNode.e.Comm.(*testComm).setFilter(mf.failOnReplicationRequest)
	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout * 2))
}

// A node attempts to request blocks to replicate, but fails to
// receive them multiple times
func TestReplicationRequestTimeoutMultiple(t *testing.T) {
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

	mf := &testTimeoutMessageFilter{
		t:                    t,
		replicationResponses: make(chan struct{}, 1),
	}

	newSimplexNode(t, nodes[0], net, bb, newNodeConfig(nodes[0]))
	normalNode2 := newSimplexNode(t, nodes[1], net, bb, newNodeConfig(nodes[1]))
	normalNode2.e.Comm.(*testComm).setFilter(mf.receivedReplicationRequest)
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

	// this is done from normalNode2 since the lagging node will request
	// seqs [0-startSeq/3] after the timeout
	<-mf.replicationResponses

	// assert the lagging node has not received any replication responses
	require.Equal(t, uint64(0), laggingNode.storage.Height())
	normalNode2.e.Comm.(*testComm).setFilter(allowAllMessages)

	// after the timeout, only normalNode2 should respond
	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout / 2))
	require.Equal(t, uint64(0), laggingNode.storage.Height())
	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout))
	laggingNode.storage.waitForBlockCommit(startSeq / 3)

	net.setAllNodesMessageFilter(allowAllMessages)
	// timeout again, now all nodes will respond
	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout * 2))
	laggingNode.storage.waitForBlockCommit(startSeq)
}

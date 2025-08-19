// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"bytes"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex"

	"github.com/stretchr/testify/require"
)

func rejectReplicationRequests(msg *simplex.Message, _, _ simplex.NodeID) bool {
	return msg.ReplicationRequest == nil && msg.ReplicationResponse == nil && msg.VerifiedReplicationResponse == nil
}

// A node attempts to request blocks to replicate, but fails to receive them
func TestReplicationRequestTimeout(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	numInitialSeqs := uint64(8)

	// node begins replication
	bb := newTestControlledBlockBuilder(t)
	net := newInMemNetwork(t, nodes)

	storageData := createBlocks(t, nodes, &bb.testBlockBuilder, numInitialSeqs)

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
	for i := 0; i <= int(numInitialSeqs); i++ {
		for _, n := range net.instances {
			if n.e.ID.Equals(laggingNode.e.ID) {
				continue
			}
			n.storage.waitForBlockCommit(uint64(numInitialSeqs))
		}
	}

	// assert the lagging node has not received any replication requests
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())

	// after the timeout, the nodes should respond and the lagging node will replicate
	net.setAllNodesMessageFilter(allowAllMessages)
	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout / 2))
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())

	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout * 2))
	laggingNode.storage.waitForBlockCommit(uint64(numInitialSeqs))
}

type testTimeoutMessageFilter struct {
	t *testing.T

	replicationResponses chan struct{}
}

func (m *testTimeoutMessageFilter) failOnReplicationRequest(msg *simplex.Message, _, _ simplex.NodeID) bool {
	require.Nil(m.t, msg.ReplicationRequest)
	return true
}

// receiveReplicationRequest is used to filter out sending replication responses, and notify a channel
// when a replication request is received.
func (m *testTimeoutMessageFilter) receivedReplicationRequest(msg *simplex.Message, _, _ simplex.NodeID) bool {
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

	// ensure enough time passes after advanceTime is called
	bb.triggerNewBlock()
	for _, n := range net.instances {
		n.storage.waitForBlockCommit(uint64(startSeq + 1))
	}
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
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())
	normalNode2.e.Comm.(*testComm).setFilter(allowAllMessages)

	// after the timeout, only normalNode2 should respond
	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout / 2))
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())

	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout))
	laggingNode.storage.waitForBlockCommit(startSeq / 3)

	net.setAllNodesMessageFilter(allowAllMessages)
	// timeout again, now all nodes will respond
	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout * 2))
	laggingNode.storage.waitForBlockCommit(startSeq)
}

// modifies the replication response to only send every even quorum round
func incompleteReplicationResponseFilter(msg *simplex.Message, _, _ simplex.NodeID) bool {
	if msg.VerifiedReplicationResponse != nil || msg.ReplicationResponse != nil {
		newLen := len(msg.VerifiedReplicationResponse.Data) / 2
		newData := make([]simplex.VerifiedQuorumRound, 0, newLen)

		for _, qr := range msg.VerifiedReplicationResponse.Data {
			if qr.GetRound()%2 == 0 {
				newData = append(newData, qr)
			}
		}
		msg.VerifiedReplicationResponse.Data = newData
	}
	return true
}

// A node attempts to request blocks to replicate, but receives incomplete
// responses from nodes.
func TestReplicationRequestIncompleteResponses(t *testing.T) {
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

	recordedMessages := make(chan *simplex.Message, 1000)
	comm := newTestComm(nodes[3], net, allowAllMessages)

	laggingNode := newSimplexNode(t, nodes[3], net, bb, &testNodeConfig{
		replicationEnabled: true,
		comm:               &recordingComm{Communication: comm, SentMessages: recordedMessages},
	})

	for _, node := range net.instances[:3] {
		node.Silence()
	}

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
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())
	net.setAllNodesMessageFilter(incompleteReplicationResponseFilter)

	// after the timeout, only normalNode2 should respond(but with incomplete data)
	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout / 2))
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())

	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout))
	laggingNode.storage.waitForBlockCommit(0)

	require.Eventually(t, func() bool {
		msg, ok := <-recordedMessages
		if !ok {
			return false
		}

		if msg.ReplicationRequest == nil {
			return false
		}

		return reflect.DeepEqual(msg.ReplicationRequest.Seqs, []uint64{3, 4, 5})

	}, 30*time.Second, 10*time.Millisecond)

	net.setAllNodesMessageFilter(allowAllMessages)
	// timeout again, now all nodes will respond
	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout * 2))
	laggingNode.storage.waitForBlockCommit(startSeq)
}

type collectNotarizationComm struct {
	lock          *sync.Mutex
	notarizations map[uint64]*simplex.Notarization
	testNetworkCommunication

	replicationResponses chan struct{}
}

func newCollectNotarizationComm(nodeID simplex.NodeID, net *inMemNetwork, notarizations map[uint64]*simplex.Notarization, lock *sync.Mutex) *collectNotarizationComm {
	return &collectNotarizationComm{
		notarizations:            notarizations,
		testNetworkCommunication: newTestComm(nodeID, net, allowAllMessages),
		replicationResponses:     make(chan struct{}, 3),
		lock:                     lock,
	}
}

func (c *collectNotarizationComm) Send(msg *simplex.Message, to simplex.NodeID) {
	if msg.Notarization != nil {
		c.lock.Lock()
		c.notarizations[msg.Notarization.Vote.Round] = msg.Notarization
		c.lock.Unlock()
	}
	c.testNetworkCommunication.Send(msg, to)
}

func (c *collectNotarizationComm) Broadcast(msg *simplex.Message) {
	if msg.Notarization != nil {
		c.lock.Lock()
		c.notarizations[msg.Notarization.Vote.Round] = msg.Notarization
		c.lock.Unlock()
	}
	c.testNetworkCommunication.Broadcast(msg)
}

func (c *collectNotarizationComm) removeFinalizationsFromReplicationResponses(msg *simplex.Message, from, to simplex.NodeID) bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	if msg.VerifiedReplicationResponse != nil || msg.ReplicationResponse != nil {
		newData := make([]simplex.VerifiedQuorumRound, 0, len(msg.VerifiedReplicationResponse.Data))

		for i := 0; i < len(msg.VerifiedReplicationResponse.Data); i++ {
			qr := msg.VerifiedReplicationResponse.Data[i]
			if qr.Finalization != nil && c.notarizations[qr.GetRound()] != nil {
				qr.Finalization = nil
				qr.Notarization = c.notarizations[qr.GetRound()]
			}
			newData = append(newData, qr)
		}
		msg.VerifiedReplicationResponse.Data = newData
		c.replicationResponses <- struct{}{}
	}
	return true
}

// TestReplicationRequestWithoutFinalization tests that a replication request is not marked as completed
// if we are expecting a finalization but it is not present in the response.
func TestReplicationRequestWithoutFinalization(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	endDisconnect := uint64(10)
	bb := newTestControlledBlockBuilder(t)
	laggingBb := newTestControlledBlockBuilder(t)
	net := newInMemNetwork(t, nodes)

	notarizations := make(map[uint64]*simplex.Notarization)
	mapLock := &sync.Mutex{}
	testConfig := func(nodeID simplex.NodeID) *testNodeConfig {
		return &testNodeConfig{
			replicationEnabled: true,
			comm:               newCollectNotarizationComm(nodeID, net, notarizations, mapLock),
		}
	}

	notarizationComm := newCollectNotarizationComm(nodes[0], net, notarizations, mapLock)
	newSimplexNode(t, nodes[0], net, bb, &testNodeConfig{
		replicationEnabled: true,
		comm:               notarizationComm,
	})
	newSimplexNode(t, nodes[1], net, bb, testConfig(nodes[1]))
	newSimplexNode(t, nodes[2], net, bb, testConfig(nodes[2]))
	laggingNode := newSimplexNode(t, nodes[3], net, laggingBb, testConfig(nodes[3]))

	epochTimes := make([]time.Time, 0, 4)
	for _, n := range net.instances {
		epochTimes = append(epochTimes, n.e.StartTime)
	}
	// lagging node disconnects
	net.Disconnect(nodes[3])
	net.startInstances()

	missedSeqs := uint64(0)
	// normal nodes continue to make progress
	for i := uint64(0); i < endDisconnect; i++ {
		emptyRound := bytes.Equal(simplex.LeaderForRound(nodes, i), nodes[3])
		if emptyRound {
			advanceWithoutLeader(t, net, bb, epochTimes, i, laggingNode.e.ID)
			missedSeqs++
		} else {
			bb.triggerNewBlock()
			for _, n := range net.instances[:3] {
				n.storage.waitForBlockCommit(i - missedSeqs)
			}
		}
	}

	// all nodes excpet for lagging node have progressed and commited [endDisconnect - missedSeqs] blocks
	for _, n := range net.instances[:3] {
		require.Equal(t, endDisconnect-missedSeqs, n.storage.NumBlocks())
	}
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())
	require.Equal(t, uint64(0), laggingNode.e.Metadata().Round)
	// lagging node reconnects
	net.setAllNodesMessageFilter(notarizationComm.removeFinalizationsFromReplicationResponses)
	net.Connect(nodes[3])
	bb.triggerNewBlock()
	for _, n := range net.instances {
		if n.e.ID.Equals(laggingNode.e.ID) {
			continue
		}

		n.storage.waitForBlockCommit(endDisconnect - missedSeqs)
		<-notarizationComm.replicationResponses
	}

	// wait until the lagging nodes sends replication requests
	// due to the removeFinalizationsFromReplicationResponses message filter
	// the lagging node should not have processed any replication requests
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())

	// we should still have these replication requests in the timeout handler
	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout * 2))

	for range net.instances[:3] {
		// the lagging node should have sent replication requests
		// and the normal nodes should have responded
		<-notarizationComm.replicationResponses
	}

	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())
	// We should still have these replication requests in the timeout handler
	// but now we allow the lagging node to process them
	net.setAllNodesMessageFilter(allowAllMessages)
	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout * 4))
	laggingNode.storage.waitForBlockCommit(endDisconnect - missedSeqs)
}

// TestReplicationMalformedQuorumRound tests that a node resends a replication request when it receives a malformed quorum round message.
func TestReplicationMalformedQuorumRound(t *testing.T) {
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

	recordedMessages := make(chan *simplex.Message, 1000)
	comm := newTestComm(nodes[3], net, allowAllMessages)

	laggingNode := newSimplexNode(t, nodes[3], net, bb, &testNodeConfig{
		replicationEnabled: true,
		comm:               &recordingComm{Communication: comm, SentMessages: recordedMessages},
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

	<-mf.replicationResponses

	// assert the lagging node has not received any replication responses
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())
	net.setAllNodesMessageFilter(
		func(msg *simplex.Message, _, _ simplex.NodeID) bool {
			if msg.VerifiedReplicationResponse != nil || msg.ReplicationResponse != nil {
				newData := make([]simplex.VerifiedQuorumRound, 0, len(msg.VerifiedReplicationResponse.Data))

				for _, qr := range msg.VerifiedReplicationResponse.Data {
					qr.Notarization = nil // remove notarization
					qr.Finalization = nil // remove finalization
					newData = append(newData, qr)
				}
				msg.VerifiedReplicationResponse.Data = newData
			}
			return true
		},
	)

	// after the timeout, only normalNode2 should respond, but with malformed data
	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout / 2))
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())

	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout))
	require.Equal(t, uint64(0), laggingNode.storage.NumBlocks())

	require.Eventually(t, func() bool {
		msg, ok := <-recordedMessages
		if !ok {
			return false
		}

		if msg.ReplicationRequest == nil {
			return false
		}

		return reflect.DeepEqual(msg.ReplicationRequest.Seqs, []uint64{3, 4, 5})

	}, 30*time.Second, 10*time.Millisecond)

	net.setAllNodesMessageFilter(allowAllMessages)
	// timeout again, now all nodes will respond
	laggingNode.e.AdvanceTime(laggingNode.e.StartTime.Add(simplex.DefaultReplicationRequestTimeout * 2))
	laggingNode.storage.waitForBlockCommit(startSeq)
}

// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"simplex"
	. "simplex"
	"simplex/record"
	"simplex/testutil"
	"simplex/wal"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimplexMultiNodeSimple(t *testing.T) {
	bb := newTestControlledBlockBuilder()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	net := newInMemNetwork(nodes)
	n1 := newSimplexNode(t, nodes[0], net, bb)
	n2 := newSimplexNode(t, nodes[1], net, bb)
	n3 := newSimplexNode(t, nodes[2], net, bb)
	n4 := newSimplexNode(t, nodes[3], net, bb)
	net.addInstances(t, n1, n2, n3, n4)

	bb.triggerNewBlock()

	net.startInstances()

	for _, n := range net.instances {
		n.wal.assertNotarization(uint64(0))
	}

	// for seq := 0; seq < 10; seq++ {
	// 	for _, n := range net.instances {
	// 		n.wal.assertNotarization(uint64(seq))
	// 	}
	// 	bb.triggerNewBlock()
	// }

	// for seq := 0; seq < 10; seq++ {
	// 	for _, n := range net.instances {
	// 		n.storage.waitForBlockCommit(uint64(seq))
	// 	}
	// }
}

func (t *testNode) start() {
	go t.handleMessages()
	require.NoError(t.t, t.e.Start())
}

func newSimplexNodeWithStorage(t *testing.T, nodeID NodeID, net *inMemNetwork, bb BlockBuilder, storage []SequenceData) *testNode {
	conf := defaultTestNodeEpochConfig(t, nodeID, net, bb)
	for _, data := range storage {
		conf.Storage.Index(data.Block, data.FCert)
	}
	e, err := NewEpoch(conf)
	require.NoError(t, err)
	ti := &testNode{
		wal:    conf.WAL.(*testWAL),
		e:      e,
		t:      t,
		storage: conf.Storage.(*InMemStorage),
		ingress: make(chan struct {
			msg  *Message
			from NodeID
		}, 100)}
			
	return ti
}

func newSimplexNode(t *testing.T, nodeID NodeID, net *inMemNetwork, bb BlockBuilder) *testNode {
	conf := defaultTestNodeEpochConfig(t, nodeID, net, bb)
	e, err := NewEpoch(conf)
	require.NoError(t, err)
	ti := &testNode{
		wal:    conf.WAL.(*testWAL),
		e:      e,
		t:      t,
		storage: conf.Storage.(*InMemStorage),
		ingress: make(chan struct {
			msg  *Message
			from NodeID
		}, 100)}

	return ti
}

func defaultTestNodeEpochConfig(t *testing.T, nodeID NodeID, net *inMemNetwork, bb BlockBuilder) EpochConfig {
	l := testutil.MakeLogger(t, int(nodeID[0]))
	wal := newTestWAL(t)
	storage := newInMemStorage()
	conf := EpochConfig{
		MaxProposalWait: DefaultMaxProposalWaitTime,
		Comm: &testComm{
			from: nodeID,
			net:  net,
		},
		Logger:              l,
		ID:                  nodeID,
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}
	return conf
}

type testNode struct {
	wal     *testWAL
	storage  *InMemStorage
	e       *Epoch
	ingress chan struct {
		msg  *Message
		from NodeID
	}
	t *testing.T
}

func (t *testNode) proposeBlock() Block{
	md := t.e.Metadata()
	block, ok := t.e.BlockBuilder.BuildBlock(context.Background(), md)
	require.True(t.t, ok)

	// send node a message from the leader
	vote, err := newTestVote(block, t.e.ID)
	require.NoError(t.t, err)
	msg := &Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}
	
	t.e.Comm.Broadcast(msg)
	return block
}

func (t *testNode) sendFinalization(block Block) {
	msg := &Message{
		Finalization: newTestFinalization(t.t, block, t.e.ID),
	}
	t.e.Comm.Broadcast(msg)
}

func (t *testNode) voteOnBlock(block Block) {
	vote, err := newTestVote(block, t.e.ID)
	require.NoError(t.t, err)
	msg := &Message{
		VoteMessage: vote,
	}
	t.e.Comm.Broadcast(msg)
}

func (t *testNode) HandleMessage(msg *Message, from NodeID) error {
	err := t.e.HandleMessage(msg, from)
	require.NoError(t.t, err)
	return err
}

func (t *testNode) handleMessages() {
	for msg := range t.ingress {
		err := t.HandleMessage(msg.msg, msg.from)
		require.NoError(t.t, err)
		if err != nil {
			return
		}
	}
}

type testWAL struct {
	WriteAheadLog
	t      *testing.T
	lock   sync.Mutex
	signal sync.Cond
}

func newTestWAL(t *testing.T) *testWAL {
	var tw testWAL
	tw.WriteAheadLog = wal.NewMemWAL(t)
	tw.signal = sync.Cond{L: &tw.lock}
	tw.t = t
	return &tw
}

func (tw *testWAL) Append(b []byte) error {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	err := tw.WriteAheadLog.Append(b)
	tw.signal.Signal()
	return err
}

func (tw *testWAL) assertWALSize(n int) {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	for {
		rawRecords, err := tw.WriteAheadLog.ReadAll()
		require.NoError(tw.t, err)

		if len(rawRecords) == n {
			return
		}

		tw.signal.Wait()
	}
}

func (tw *testWAL) assertNotarization(round uint64) {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	for {
		rawRecords, err := tw.WriteAheadLog.ReadAll()
		require.NoError(tw.t, err)

		for _, rawRecord := range rawRecords {
			if binary.BigEndian.Uint16(rawRecord[:2]) == record.NotarizationRecordType {
				_, vote, err := ParseNotarizationRecord(rawRecord)
				require.NoError(tw.t, err)

				if vote.Round == round {
					return
				}
			}
		}

		tw.signal.Wait()
	}

}

type testComm struct {
	from NodeID
	net  *inMemNetwork
}

func (c *testComm) ListNodes() []NodeID {
	return c.net.nodes
}

func (c *testComm) SendMessage(msg *Message, destination NodeID) {
	for _, instance := range c.net.instances {
		if bytes.Equal(instance.e.ID, destination) {
			instance.ingress <- struct {
				msg  *Message
				from NodeID
			}{msg: msg, from: c.from}
			return
		}
	}
}

func (c *testComm) Broadcast(msg *Message) {
	for _, instance := range c.net.instances {
		// Skip sending the message to yourself
		if bytes.Equal(c.from, instance.e.ID) {
			continue
		}
		instance.ingress <- struct {
			msg  *Message
			from NodeID
		}{msg: msg, from: c.from}
	}
}

type inMemNetwork struct {
	instances []*testNode
	nodes     []NodeID
}

// must pass all nodeIDs before adding instances to the network
func newInMemNetwork(nodeIDs []NodeID) *inMemNetwork {
	net := &inMemNetwork{
		instances: make([]*testNode, 0),
		nodes:     nodeIDs,
	}
	return net
}

func (n *inMemNetwork) addInstances(t *testing.T, node ...*testNode) {
	allowed := false
	for _, n := range node {
		if n.e.ID.Equals(n.e.ID) {
			allowed = true
			break
		}
	}
	require.True(t, allowed, "node not allowed to join network")
	n.instances = append(n.instances, node...)
}

func (n *inMemNetwork) getLeader(round uint64) *testNode {
	leaderId := simplex.LeaderForRound(n.nodes, round)

	for _, n := range n.instances {
		if bytes.Equal(n.e.ID, leaderId) {
			return n
		}
	}
	return nil
}

func (n *inMemNetwork) startInstances() {
	for i := len(n.nodes) - 1; i >= 0; i-- {
		n.instances[i].start()
	}
}

type testControlledBlockBuilder struct {
	control chan struct{}
	testBlockBuilder
}

func newTestControlledBlockBuilder() *testControlledBlockBuilder {
	return &testControlledBlockBuilder{
		control:          make(chan struct{}, 1),
		testBlockBuilder: testBlockBuilder{out: make(chan *testBlock, 1)},
	}
}

func (t *testControlledBlockBuilder) triggerNewBlock() {
	select {
	case t.control <- struct{}{}:
	default:

	}
}

func (t *testControlledBlockBuilder) BuildBlock(ctx context.Context, metadata ProtocolMetadata) (Block, bool) {
	<-t.control
	return t.testBlockBuilder.BuildBlock(ctx, metadata)
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"bytes"
	"context"
	"encoding/binary"
	. "simplex"
	"simplex/record"
	"simplex/testutil"
	"simplex/wal"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimplexMultiNodeSimple(t *testing.T) {
	bb := newTestControlledBlockBuilder(t)

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	net := newInMemNetwork(t, nodes)
	newSimplexNode(t, nodes[0], net, bb)
	newSimplexNode(t, nodes[1], net, bb)
	newSimplexNode(t, nodes[2], net, bb)
	newSimplexNode(t, nodes[3], net, bb)

	bb.triggerNewBlock()

	net.startInstances()

	for seq := 0; seq < 10; seq++ {
		for _, n := range net.instances {
			n.storage.waitForBlockCommit(uint64(seq))
		}
		bb.triggerNewBlock()
	}
}

func (t *testNode) start() {
	go t.handleMessages()
	require.NoError(t.t, t.e.Start())
}

func newSimplexNodeWithStorage(t *testing.T, nodeID NodeID, net *inMemNetwork, bb BlockBuilder, storage []FinalizedBlock) *testNode {
	wal := newTestWAL(t)
	conf := defaultTestNodeEpochConfig(t, nodeID, net, wal, bb)
	for _, data := range storage {
		conf.Storage.Index(data.Block, data.FCert)
	}
	e, err := NewEpoch(conf)
	require.NoError(t, err)
	ti := &testNode{
		wal:     wal,
		e:       e,
		t:       t,
		storage: conf.Storage.(*InMemStorage),
		ingress: make(chan struct {
			msg  *Message
			from NodeID
		}, 100)}

	net.addNode(ti)
	return ti
}

func newSimplexNode(t *testing.T, nodeID NodeID, net *inMemNetwork, bb BlockBuilder) *testNode {
	wal := newTestWAL(t)
	conf := defaultTestNodeEpochConfig(t, nodeID, net, wal, bb)
	e, err := NewEpoch(conf)
	require.NoError(t, err)
	ti := &testNode{
		wal:     wal,
		e:       e,
		t:       t,
		storage: conf.Storage.(*InMemStorage),
		ingress: make(chan struct {
			msg  *Message
			from NodeID
		}, 100)}

	net.addNode(ti)
	return ti
}

func defaultTestNodeEpochConfig(t *testing.T, nodeID NodeID, net *inMemNetwork, wal WriteAheadLog, bb BlockBuilder) EpochConfig {
	l := testutil.MakeLogger(t, int(nodeID[0]))
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
		BlockDeserializer:   &blockDeserializer{},
		QCDeserializer:      &testQCDeserializer{t: t},
	}
	return conf
}

type testNode struct {
	wal     *testWAL
	storage *InMemStorage
	e       *Epoch
	ingress chan struct {
		msg  *Message
		from NodeID
	}
	t *testing.T
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
	t         *testing.T
	nodes     []NodeID
	instances []*testNode
}

// newInMemNetwork creates an in-memory network. Node IDs must be provided before
// adding instances, as nodes require prior knowledge of all participants.
func newInMemNetwork(t *testing.T, nodes []NodeID) *inMemNetwork {
	net := &inMemNetwork{
		t:         t,
		nodes:     nodes,
		instances: make([]*testNode, 0),
	}
	return net
}

func (n *inMemNetwork) addNode(node *testNode) {
	allowed := false
	for _, id := range n.nodes {
		if bytes.Equal(id, node.e.ID) {
			allowed = true
			break
		}
	}
	require.True(node.t, allowed, "node must be declared before adding")
	n.instances = append(n.instances, node)
}

// startInstances starts all instances in the network.
// The first one is typically the leader, so we make sure to start it last.
func (n *inMemNetwork) startInstances() {
	require.Equal(n.t, len(n.nodes), len(n.instances))

	for i := len(n.nodes) - 1; i >= 0; i-- {
		n.instances[i].start()
	}
}

// testControlledBlockBuilder is a test block builder that blocks
// block building until a trigger is received
type testControlledBlockBuilder struct {
	t       *testing.T
	control chan struct{}
	testBlockBuilder
}

func newTestControlledBlockBuilder(t *testing.T) *testControlledBlockBuilder {
	return &testControlledBlockBuilder{
		t:                t,
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
	require.Equal(t.t, metadata.Seq, metadata.Round)
	<-t.control
	return t.testBlockBuilder.BuildBlock(ctx, metadata)
}

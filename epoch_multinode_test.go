// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"bytes"
	"context"
	"github.com/stretchr/testify/require"
	. "simplex"
	"simplex/wal"
	"testing"
)

func TestSimplexMultiNodeSimple(t *testing.T) {
	bb := newTestControlledBlockBuilder()

	var net inMemNetwork
	n1 := newSimplexNode(t, 1, &net, bb)
	n2 := newSimplexNode(t, 2, &net, bb)
	n3 := newSimplexNode(t, 3, &net, bb)
	n4 := newSimplexNode(t, 4, &net, bb)

	bb.triggerNewBlock()

	instances := []*testInstance{n1, n2, n3, n4}

	for _, n := range instances {
		n.start()
	}

	for seq := 0; seq < 100; seq++ {
		for _, n := range instances {
			n.ledger.waitForBlockCommit(uint64(seq))
			bb.triggerNewBlock()
		}
	}
}

func newSimplexNode(t *testing.T, id uint8, net *inMemNetwork, bb BlockBuilder) *testInstance {
	l := makeLogger(t)
	storage := newInMemStorage()

	nodeID := NodeID{id}

	e := &Epoch{
		Comm: &testComm{
			from: nodeID,
			net:  net,
		},
		BlockDigester: blockDigester{},
		Logger:        l,
		ID:            nodeID,
		Signer:        &testSigner{},
		WAL:           &wal.InMemWAL{},
		Verifier:      &testVerifier{},
		BlockVerifier: &testVerifier{},
		Storage:       storage,
		BlockBuilder:  bb,
	}

	ti := &testInstance{
		e:      e,
		t:      t,
		ledger: storage,
		ingress: make(chan struct {
			msg  *Message
			from NodeID
		}, 100)}

	net.nodes = append(net.nodes, nodeID)
	net.instances = append(net.instances, ti)

	return ti
}

type testInstance struct {
	ledger  *InMemStorage
	e       *Epoch
	ingress chan struct {
		msg  *Message
		from NodeID
	}
	t *testing.T
}

func (t *testInstance) start() {
	require.NoError(t.t, t.e.Start())
	go t.run()
}

func (t *testInstance) run() {
	for {
		select {
		case msg := <-t.ingress:
			err := t.e.HandleMessage(msg.msg, msg.from)
			require.NoError(t.t, err)
			if err != nil {
				return
			}
		}
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
	nodes     []NodeID
	instances []*testInstance
}

type testControlledBlockBuilder struct {
	control chan struct{}
	testBlockBuilder
}

func newTestControlledBlockBuilder() *testControlledBlockBuilder {
	return &testControlledBlockBuilder{
		control:          make(chan struct{}, 1),
		testBlockBuilder: make(testBlockBuilder, 1),
	}
}

func (t *testControlledBlockBuilder) triggerNewBlock() {
	t.control <- struct{}{}
}

func (t *testControlledBlockBuilder) BuildBlock(ctx context.Context, metadata ProtocolMetadata) (Block, bool) {
	<-t.control
	return t.testBlockBuilder.BuildBlock(ctx, metadata)
}

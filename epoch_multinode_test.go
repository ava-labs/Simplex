// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"bytes"
	"context"
	"encoding/binary"
	. "simplex"
	"simplex/record"
	"simplex/wal"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimplexMultiNodeSimple(t *testing.T) {
	bb := newTestControlledBlockBuilder()

	var net inMemNetwork
	net.nodes = []NodeID{{1}, {2}, {3}, {4}}

	n1 := newSimplexNode(t, 1, &net, bb)
	n2 := newSimplexNode(t, 2, &net, bb)
	n3 := newSimplexNode(t, 3, &net, bb)
	n4 := newSimplexNode(t, 4, &net, bb)

	bb.triggerNewBlock()

	instances := []*testInstance{n1, n2, n3, n4}

	for _, n := range instances {
		n.start()
	}

	for seq := 0; seq < 10; seq++ {
		for _, n := range instances {
			n.assertNotarization(uint64(seq))
		}
		bb.triggerNewBlock()
	}
}

func (t *testInstance) start() {
	go t.handleMessages()
	require.NoError(t.t, t.e.Start())
}

func newSimplexNode(t *testing.T, id uint8, net *inMemNetwork, bb BlockBuilder) *testInstance {
	l := makeLogger(t, int(id))
	storage := newInMemStorage()

	nodeID := NodeID{id}

	wal := &wal.InMemWAL{}

	conf := EpochConfig{
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

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	ti := &testInstance{
		wal:    wal,
		e:      e,
		t:      t,
		ledger: storage,
		ingress: make(chan struct {
			msg  *Message
			from NodeID
		}, 100)}

	net.instances = append(net.instances, ti)

	return ti
}

type testInstance struct {
	wal     *wal.InMemWAL
	ledger  *InMemStorage
	e       *Epoch
	ingress chan struct {
		msg  *Message
		from NodeID
	}
	t *testing.T
}

func (t *testInstance) assertNotarization(round uint64) {
	rawRecords, err := t.wal.ReadAll()
	require.NoError(t.t, err)

	for _, rawRecord := range rawRecords {
		if binary.BigEndian.Uint16(rawRecord[:2]) == record.NotarizationRecordType {
			_, vote, err := NotarizationFromRecord(rawRecord)
			require.NoError(t.t, err)

			if vote.Round == round {
				return
			}
		}
	}
}

func (t *testInstance) handleMessages() {
	for msg := range t.ingress {
		err := t.e.HandleMessage(msg.msg, msg.from)
		require.NoError(t.t, err)
		if err != nil {
			return
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

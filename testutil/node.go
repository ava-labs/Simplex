// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package testutil

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/stretchr/testify/require"
)

type TestNode struct {
	currentTime atomic.Int64
	WAL         *TestWAL
	Storage     *InMemStorage
	E           *simplex.Epoch
	ingress     chan struct {
		msg  *simplex.Message
		from simplex.NodeID
	}
	l  *TestLogger
	t  *testing.T
	BB *testControlledBlockBuilder
}

// newSimplexNode creates a new testNode and adds it to [net].
func NewSimplexNode(t *testing.T, nodeID simplex.NodeID, net *InMemNetwork, config *TestNodeConfig) *TestNode {
	comm := NewTestComm(nodeID, net, AllowAllMessages)
	bb := NewTestControlledBlockBuilder(t)
	epochConfig, wal, storage := DefaultTestNodeEpochConfig(t, nodeID, comm, bb)

	if config != nil {
		updateEpochConfig(&epochConfig, config)
	}

	e, err := simplex.NewEpoch(epochConfig)
	require.NoError(t, err)
	ti := &TestNode{
		l:       epochConfig.Logger.(*TestLogger),
		WAL:     wal,
		BB:      bb,
		E:       e,
		t:       t,
		Storage: storage,
		ingress: make(chan struct {
			msg  *simplex.Message
			from simplex.NodeID
		}, 100)}

	ti.currentTime.Store(epochConfig.StartTime.UnixMilli())

	net.addNode(ti)
	return ti
}

func updateEpochConfig(epochConfig *simplex.EpochConfig, testConfig *TestNodeConfig) {
	// set the initial storage
	for _, data := range testConfig.InitialStorage {
		epochConfig.Storage.Index(context.Background(), data.VerifiedBlock, data.Finalization)
	}

	// TODO: remove optional replication flag
	epochConfig.ReplicationEnabled = testConfig.ReplicationEnabled

	// custom communication
	if testConfig.Comm != nil {
		epochConfig.Comm = testConfig.Comm
	}

	if testConfig.SigAggregator != nil {
		epochConfig.SignatureAggregator = testConfig.SigAggregator
	}
}

func (t *TestNode) Start() {
	go t.handleMessages()
	require.NoError(t.t, t.E.Start())
}

type TestNodeConfig struct {
	// optional
	InitialStorage     []simplex.VerifiedFinalizedBlock
	Comm               simplex.Communication
	SigAggregator      simplex.SignatureAggregator
	ReplicationEnabled bool
}

// TriggerBlockShouldBeBuilt signals this nodes block builder it is expecting a block to be built.
func (t *TestNode) TriggerBlockShouldBeBuilt() {
	select {
	case t.BB.BlockShouldBeBuilt <- struct{}{}:
	default:
	}
}

func (t *TestNode) AdvanceTime(duration time.Duration) {
	now := time.UnixMilli(t.currentTime.Load()).Add(duration)
	t.currentTime.Store(now.UnixMilli())
	t.E.AdvanceTime(now)
}

func (t *TestNode) Silence() {
	t.l.Silence()
}

func (t *TestNode) HandleMessage(msg *simplex.Message, from simplex.NodeID) error {
	err := t.E.HandleMessage(msg, from)
	require.NoError(t.t, err)
	return err
}

func (t *TestNode) handleMessages() {
	for msg := range t.ingress {
		err := t.HandleMessage(msg.msg, msg.from)
		require.NoError(t.t, err)
		if err != nil {
			return
		}
	}
}

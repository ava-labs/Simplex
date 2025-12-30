// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package testutil

import (
	"context"
	"fmt"
	"sync"
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
	lock        sync.RWMutex
	running     sync.WaitGroup
	shouldStop  atomic.Bool
	E           *simplex.Epoch
	ingress     chan struct {
		msg  *simplex.Message
		from simplex.NodeID
	}
	l                *TestLogger
	t                *testing.T
	BB               ControlledBlockBuilder
	messageTypesSent map[string]uint64
}

func newTestNode(t *testing.T, nodeID simplex.NodeID, net *InMemNetwork, config *TestNodeConfig) *TestNode {
	comm := NewTestComm(nodeID, net, AllowAllMessages)
	var bb ControlledBlockBuilder = NewTestControlledBlockBuilder(t)
	if config != nil && config.BlockBuilder != nil {
		bb = config.BlockBuilder
	}

	epochConfig, wal, storage := DefaultTestNodeEpochConfig(t, nodeID, comm, bb)

	if config != nil {
		updateEpochConfig(&epochConfig, config)
		if config.WAL != nil {
			wal = config.WAL
		}
		if config.Storage != nil {
			storage = config.Storage
		}
	}

	e, err := simplex.NewEpoch(epochConfig)
	require.NoError(t, err)
	ti := &TestNode{
		l:                epochConfig.Logger.(*TestLogger),
		WAL:              wal,
		BB:               bb,
		E:                e,
		t:                t,
		Storage:          storage,
		messageTypesSent: make(map[string]uint64),
		ingress: make(chan struct {
			msg  *simplex.Message
			from simplex.NodeID
		}, 100000)}

	ti.currentTime.Store(epochConfig.StartTime.UnixMilli())
	return ti
}

// newSimplexNode creates a new testNode and adds it to [net].
func NewSimplexNode(t *testing.T, nodeID simplex.NodeID, net *InMemNetwork, config *TestNodeConfig) *TestNode {
	ti := newTestNode(t, nodeID, net, config)

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

	if testConfig.BlockBuilder != nil {
		epochConfig.BlockBuilder = testConfig.BlockBuilder
	}

	if testConfig.MaxRoundWindow != 0 {
		epochConfig.MaxRoundWindow = testConfig.MaxRoundWindow
	}

	if testConfig.Logger != nil {
		epochConfig.Logger = testConfig.Logger
	}

	if testConfig.WAL != nil {
		epochConfig.WAL = testConfig.WAL
	}

	if testConfig.Storage != nil {
		epochConfig.Storage = testConfig.Storage
	}

	if testConfig.StartTime != 0 {
		epochConfig.StartTime = time.UnixMilli(testConfig.StartTime)
	}
}

func (t *TestNode) Start() {
	t.running.Add(1)
	go t.handleMessages()
	require.NoError(t.t, t.E.Start())
}

type ControlledBlockBuilder interface {
	simplex.BlockBuilder
	TriggerNewBlock()
	TriggerBlockShouldBeBuilt()
	ShouldBlockBeBuilt() bool
}

type TestNodeConfig struct {
	// optional
	InitialStorage     []simplex.VerifiedFinalizedBlock
	Comm               simplex.Communication
	SigAggregator      simplex.SignatureAggregator
	ReplicationEnabled bool
	BlockBuilder       ControlledBlockBuilder

	// Long Running Tests
	MaxRoundWindow uint64
	Logger         *TestLogger
	WAL            *TestWAL
	Storage        *InMemStorage
	StartTime      int64
}

func (t *TestNode) AdvanceTime(duration time.Duration) {
	now := time.UnixMilli(t.currentTime.Load()).Add(duration)
	t.currentTime.Store(now.UnixMilli())
	t.E.AdvanceTime(now)
}

func (t *TestNode) Silence() {
	t.l.Silence()
}

func (t *TestNode) SilenceExceptKeywords(keywords ...string) {
	t.l.SilenceExceptKeywords(keywords...)
}

func (t *TestNode) HandleMessage(msg *simplex.Message, from simplex.NodeID) error {
	err := t.E.HandleMessage(msg, from)
	require.NoError(t.t, err)
	return err
}

func (t *TestNode) handleMessages() {
	defer t.running.Done()
	for msg := range t.ingress {
		if t.shouldStop.Load() {
			return
		}
		err := t.HandleMessage(msg.msg, msg.from)
		require.NoError(t.t, err)
		if err != nil {
			return
		}
	}
}

// TimeoutOnRound advances time until the node times out of the given round.
func (t *TestNode) TimeoutOnRound(round uint64) {
	for {
		currentRound := t.E.Metadata().Round
		if currentRound > round {
			return
		}

		if !t.BB.ShouldBlockBeBuilt() {
			t.BB.TriggerBlockShouldBeBuilt()
		}

		t.AdvanceTime(t.E.MaxProposalWait)

		// check the wal for an empty vote for that round
		if hasVote := t.WAL.ContainsEmptyVote(round); hasVote {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (t *TestNode) TickUntilRoundAdvanced(round uint64, tick time.Duration) {
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

func (t *TestNode) enqueue(msg *simplex.Message, from, to simplex.NodeID) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	if t.shouldStop.Load() {
		return
	}

	t.RecordMessageTypeSent(msg)

	select {
	case t.ingress <- struct {
		msg  *simplex.Message
		from simplex.NodeID
	}{msg: msg, from: from}:
	default:
		// drop the message if the ingress channel is full
		formattedString := fmt.Sprintf("Ingress channel is too full, failing test. From %v -> to %v", from, to)
		panic(formattedString)
	}

}

func (t *TestNode) Stop() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.E.Stop()
	t.shouldStop.Store(true)
	close(t.ingress)
	t.running.Wait()
}

func (t *TestNode) RecordMessageTypeSent(msg *simplex.Message) {
	t.lock.Lock()
	defer t.lock.Unlock()

	switch {
	case msg.BlockMessage != nil:
		t.messageTypesSent["BlockMessage"]++
	case msg.VerifiedBlockMessage != nil:
		t.messageTypesSent["VerifiedBlockMessage"]++
	case msg.ReplicationRequest != nil:
		t.messageTypesSent["ReplicationRequest"]++
	case msg.ReplicationResponse != nil:
		t.messageTypesSent["ReplicationResponse"]++
	case msg.Notarization != nil:
		t.messageTypesSent["VerifiedReplicationRequest"]++
	case msg.VerifiedReplicationResponse != nil:
		t.messageTypesSent["VerifiedReplicationResponse"]++
	case msg.Finalization != nil:
		t.messageTypesSent["NotarizationMessage"]++
	case msg.FinalizeVote != nil:
		t.messageTypesSent["FinalizationMessage"]++
	case msg.VoteMessage != nil:
		t.messageTypesSent["VoteMessage"]++
	case msg.EmptyVoteMessage != nil:
		t.messageTypesSent["EmptyVoteMessage"]++
	case msg.EmptyNotarization != nil:
		t.messageTypesSent["EmptyNotarization"]++
	default:
		panic("unknown message type")
	}

}

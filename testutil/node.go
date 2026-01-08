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

// BasicNode is a simple representation of an instance running an epoch. It does not know about any particular custom types, its sole responsibility is to
// run the epoch and handle messages.
type BasicNode struct {
	currentTime atomic.Int64
	shouldStop  atomic.Bool
	lock        sync.RWMutex
	running     sync.WaitGroup
	E           *simplex.Epoch

	ingress chan struct {
		msg  *simplex.Message
		from simplex.NodeID
	}
	l                *TestLogger
	t                *testing.T
	messageTypesSent map[string]uint64
}

func (b *BasicNode) Start() {
	b.running.Add(1)
	go b.handleMessages()
	require.NoError(b.t, b.E.Start())
}

func (b *BasicNode) AdvanceTime(duration time.Duration) {
	now := time.UnixMilli(b.currentTime.Load()).Add(duration)
	b.currentTime.Store(now.UnixMilli())
	b.E.AdvanceTime(now)
}

func (b *BasicNode) Silence() {
	b.l.Silence()
}

func (b *BasicNode) SilenceExceptKeywords(keywords ...string) {
	b.l.SilenceExceptKeywords(keywords...)
}

func (b *BasicNode) HandleMessage(msg *simplex.Message, from simplex.NodeID) error {
	err := b.E.HandleMessage(msg, from)
	require.NoError(b.t, err)
	return err
}

func (b *BasicNode) handleMessages() {
	defer b.running.Done()
	for msg := range b.ingress {
		if b.shouldStop.Load() {
			return
		}
		err := b.HandleMessage(msg.msg, msg.from)
		require.NoError(b.t, err)
		if err != nil {
			return
		}
	}
}

func (b *BasicNode) enqueue(msg *simplex.Message, from, to simplex.NodeID) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.shouldStop.Load() {
		return
	}

	b.RecordMessageTypeSent(msg)

	select {
	case b.ingress <- struct {
		msg  *simplex.Message
		from simplex.NodeID
	}{msg: msg, from: from}:
	default:
		// drop the message if the ingress channel is full
		formattedString := fmt.Sprintf("Ingress channel is too full, failing test. From %v -> to %v", from, to)
		panic(formattedString)
	}
}

func (b *BasicNode) Stop() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.E.Stop()
	b.shouldStop.Store(true)
	close(b.ingress)
	b.running.Wait()
}

func (b *BasicNode) RecordMessageTypeSent(msg *simplex.Message) {
	switch {
	case msg.BlockMessage != nil:
		b.messageTypesSent["BlockMessage"]++
	case msg.VerifiedBlockMessage != nil:
		b.messageTypesSent["VerifiedBlockMessage"]++
	case msg.ReplicationRequest != nil:
		b.messageTypesSent["ReplicationRequest"]++
	case msg.ReplicationResponse != nil:
		b.messageTypesSent["ReplicationResponse"]++
	case msg.Notarization != nil:
		b.messageTypesSent["VerifiedReplicationRequest"]++
	case msg.VerifiedReplicationResponse != nil:
		b.messageTypesSent["VerifiedReplicationResponse"]++
	case msg.Finalization != nil:
		b.messageTypesSent["NotarizationMessage"]++
	case msg.FinalizeVote != nil:
		b.messageTypesSent["FinalizationMessage"]++
	case msg.VoteMessage != nil:
		b.messageTypesSent["VoteMessage"]++
	case msg.EmptyVoteMessage != nil:
		b.messageTypesSent["EmptyVoteMessage"]++
	case msg.EmptyNotarization != nil:
		b.messageTypesSent["EmptyNotarization"]++
	case msg.BlockDigestRequest != nil:
		b.messageTypesSent["BlockDigestRequest"]++
	default:
		panic("unknown message type")
	}
}

func (b *BasicNode) PrintMessageTypesSent() {
	b.t.Logf("Node %s sent message types: %+v", b.E.ID.String(), b.messageTypesSent)
}

func (b *BasicNode) ResetMessageTypesSent() {
	b.messageTypesSent = make(map[string]uint64)
}

func (b *BasicNode) GetMessageTypesSent() map[string]uint64 {
	return b.messageTypesSent
}

type ControlledNode struct {
	*BasicNode
	bb *testControlledBlockBuilder
	WAL *TestWAL
	Storage *InMemStorage
}

func NewBasicNode(t *testing.T, epoch *simplex.Epoch, logger *TestLogger) *BasicNode {
	currentTime := epoch.EpochConfig.StartTime.UnixMilli()
	bn := BasicNode{
		shouldStop: atomic.Bool{},
		lock:       sync.RWMutex{},
		running:    sync.WaitGroup{},
		E:          epoch,
		ingress: make(chan struct {
			msg  *simplex.Message
			from simplex.NodeID
		}, 100000),
		l:                logger,
		t:                t,
		messageTypesSent: make(map[string]uint64),
	}

	bn.currentTime.Store(currentTime)
	return &bn
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
		WAL:              wal,
		bb:               bb,
		Storage:          storage,
	}

	net.addNode(ti)
	return ti
}

func UpdateEpochConfig(epochConfig *simplex.EpochConfig, testConfig *TestNodeConfig) {
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

type TestNodeConfig struct {
	// optional
	InitialStorage     []simplex.VerifiedFinalizedBlock
	Comm               simplex.Communication
	SigAggregator      simplex.SignatureAggregator
	ReplicationEnabled bool
	BlockBuilder       *testControlledBlockBuilder

	// Long Running Tests
	MaxRoundWindow uint64
	Logger         *TestLogger
	WAL            *TestWAL
	Storage        *InMemStorage
	StartTime      int64
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
	t.bb.BlockShouldBeBuilt <- struct{}{}
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

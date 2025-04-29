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
	"time"

	"github.com/stretchr/testify/require"
)

func TestSimplexMultiNodeSimple(t *testing.T) {
	bb := newTestControlledBlockBuilder(t)

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	net := newInMemNetwork(t, nodes)
	newSimplexNode(t, nodes[0], net, bb, nil)
	newSimplexNode(t, nodes[1], net, bb, nil)
	newSimplexNode(t, nodes[2], net, bb, nil)
	newSimplexNode(t, nodes[3], net, bb, nil)

	net.startInstances()

	for seq := 0; seq < 10; seq++ {
		bb.triggerNewBlock()
		for _, n := range net.instances {
			n.storage.waitForBlockCommit(uint64(seq))
		}
	}
}

func (t *testNode) start() {
	go t.handleMessages()
	require.NoError(t.t, t.e.Start())
}

type testNodeConfig struct {
	// optional
	initialStorage     []VerifiedFinalizedBlock
	comm               Communication
	replicationEnabled bool
}

// newSimplexNode creates a new testNode and adds it to [net].
func newSimplexNode(t *testing.T, nodeID NodeID, net *inMemNetwork, bb BlockBuilder, config *testNodeConfig) *testNode {
	comm := newTestComm(nodeID, net, allowAllMessages)

	epochConfig := defaultTestNodeEpochConfig(t, nodeID, comm, bb)

	if config != nil {
		updateEpochConfig(&epochConfig, config)
	}

	e, err := NewEpoch(epochConfig)
	require.NoError(t, err)
	ti := &testNode{
		wal:     epochConfig.WAL.(*testWAL),
		e:       e,
		t:       t,
		storage: epochConfig.Storage.(*InMemStorage),
		ingress: make(chan struct {
			msg  *Message
			from NodeID
		}, 100)}

	net.addNode(ti)
	return ti
}

func updateEpochConfig(epochConfig *EpochConfig, testConfig *testNodeConfig) {
	// set the initial storage
	for _, data := range testConfig.initialStorage {
		epochConfig.Storage.Index(data.VerifiedBlock, data.FCert)
	}

	// TODO: remove optional replication flag
	epochConfig.ReplicationEnabled = testConfig.replicationEnabled

	// custom communication
	if testConfig.comm != nil {
		epochConfig.Comm = testConfig.comm
	}
}

func defaultTestNodeEpochConfig(t *testing.T, nodeID NodeID, comm Communication, bb BlockBuilder) EpochConfig {
	l := testutil.MakeLogger(t, int(nodeID[0]))
	storage := newInMemStorage()
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Comm:                comm,
		Logger:              l,
		ID:                  nodeID,
		Signer:              &testSigner{},
		WAL:                 newTestWAL(t),
		Verifier:            &testVerifier{},
		Storage:             storage,
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
		BlockDeserializer:   &blockDeserializer{},
		QCDeserializer:      &testQCDeserializer{t: t},
		StartTime:           time.Now(),
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

func (tw *testWAL) Clone() *testWAL {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	rawWAL, err := tw.ReadAll()
	require.NoError(tw.t, err)

	wal := newTestWAL(tw.t)

	for _, entry := range rawWAL {
		wal.Append(entry)
	}

	return wal
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
			if binary.BigEndian.Uint16(rawRecord[:2]) == record.EmptyNotarizationRecordType {
				_, vote, err := ParseEmptyNotarizationRecord(rawRecord)
				require.NoError(tw.t, err)

				if vote.Round == round {
					return
				}
			}
		}

		tw.signal.Wait()
	}

}

func (tw *testWAL) assertNotarizationOrFinalization(round uint64, qc QCDeserializer) {
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
			if binary.BigEndian.Uint16(rawRecord[:2]) == record.EmptyNotarizationRecordType {
				_, vote, err := ParseEmptyNotarizationRecord(rawRecord)
				require.NoError(tw.t, err)

				if vote.Round == round {
					return
				}
			}
			if binary.BigEndian.Uint16(rawRecord[:2]) == record.FinalizationRecordType {
				fcert, err := FinalizationCertificateFromRecord(rawRecord, qc)
				require.NoError(tw.t, err)

				if fcert.Finalization.Round == round {
					return
				}
			}

		}

		tw.signal.Wait()
	}

}

func (tw *testWAL) containsEmptyVote(round uint64) bool {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	rawRecords, err := tw.WriteAheadLog.ReadAll()
	require.NoError(tw.t, err)

	for _, rawRecord := range rawRecords {
		if binary.BigEndian.Uint16(rawRecord[:2]) == record.EmptyVoteRecordType {
			vote, err := ParseEmptyVoteRecord(rawRecord)
			require.NoError(tw.t, err)

			if vote.Round == round {
				return true
			}
		}
	}

	return false
}

func (tw *testWAL) containsEmptyNotarization(round uint64) bool {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	rawRecords, err := tw.WriteAheadLog.ReadAll()
	require.NoError(tw.t, err)

	for _, rawRecord := range rawRecords {
		if binary.BigEndian.Uint16(rawRecord[:2]) == record.EmptyNotarizationRecordType {
			_, vote, err := ParseEmptyNotarizationRecord(rawRecord)
			require.NoError(tw.t, err)

			if vote.Round == round {
				return true
			}
		}
	}

	return false
}

// messageFilter defines a function that filters
// certain messages from being sent or broadcasted.
// a message filter should return true if the message is allowed to be sent
type messageFilter func(*Message, NodeID) bool

// allowAllMessages allows every message to be sent
func allowAllMessages(*Message, NodeID) bool {
	return true
}

// denyFinalizationMessages blocks any messages that would cause nodes in
// a network to index a block in storage.
func denyFinalizationMessages(msg *Message, destination NodeID) bool {
	if msg.Finalization != nil {
		return false
	}
	if msg.FinalizationCertificate != nil {
		return false
	}

	return true
}

func onlyAllowEmptyRoundMessages(msg *Message, destination NodeID) bool {
	if msg.EmptyNotarization != nil {
		return true
	}
	if msg.EmptyVoteMessage != nil {
		return true
	}
	return false
}

type testComm struct {
	from          NodeID
	net           *inMemNetwork
	messageFilter messageFilter
	lock          sync.RWMutex
}

func newTestComm(from NodeID, net *inMemNetwork, messageFilter messageFilter) *testComm {
	return &testComm{
		from:          from,
		net:           net,
		messageFilter: messageFilter,
	}
}

func (c *testComm) ListNodes() []NodeID {
	return c.net.nodes
}

func (c *testComm) SendMessage(msg *Message, destination NodeID) {
	if !c.isMessagePermitted(msg, destination) {
		return
	}

	// cannot send if either [from] or [destination] is not connected
	if c.net.IsDisconnected(destination) || c.net.IsDisconnected(c.from) {
		return
	}

	c.maybeTranslateOutoingToIncomingMessageTypes(msg)

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

func (c *testComm) setFilter(filter messageFilter) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.messageFilter = filter
}

func (c *testComm) maybeTranslateOutoingToIncomingMessageTypes(msg *Message) {
	if msg.VerifiedReplicationResponse != nil {
		data := make([]QuorumRound, 0, len(msg.VerifiedReplicationResponse.Data))

		for _, verifiedQuorumRound := range msg.VerifiedReplicationResponse.Data {
			// Outgoing block is of type verified block but incoming block is of type Block,
			// so we do a type cast because the test block implements both.
			quorumRound := QuorumRound{}
			if verifiedQuorumRound.EmptyNotarization != nil {
				quorumRound.EmptyNotarization = verifiedQuorumRound.EmptyNotarization
			} else {
				quorumRound.Block = verifiedQuorumRound.VerifiedBlock.(Block)
				if verifiedQuorumRound.Notarization != nil {
					quorumRound.Notarization = verifiedQuorumRound.Notarization
				}
				if verifiedQuorumRound.FCert != nil {
					quorumRound.FCert = verifiedQuorumRound.FCert
				}
			}

			data = append(data, quorumRound)
		}

		var latestRound *QuorumRound
		if msg.VerifiedReplicationResponse.LatestRound != nil {
			if msg.VerifiedReplicationResponse.LatestRound.EmptyNotarization != nil {
				latestRound = &QuorumRound{
					EmptyNotarization: msg.VerifiedReplicationResponse.LatestRound.EmptyNotarization,
				}
			} else {
				latestRound = &QuorumRound{
					Block:             msg.VerifiedReplicationResponse.LatestRound.VerifiedBlock.(Block),
					Notarization:      msg.VerifiedReplicationResponse.LatestRound.Notarization,
					FCert:             msg.VerifiedReplicationResponse.LatestRound.FCert,
					EmptyNotarization: msg.VerifiedReplicationResponse.LatestRound.EmptyNotarization,
				}
			}
		}

		require.Nil(
			c.net.t,
			msg.ReplicationResponse,
			"message cannot include ReplicationResponse & VerifiedReplicationResponse",
		)

		msg.ReplicationResponse = &ReplicationResponse{
			Data:        data,
			LatestRound: latestRound,
		}
	}

	if msg.VerifiedBlockMessage != nil {
		require.Nil(c.net.t, msg.BlockMessage, "message cannot include BlockMessage & VerifiedBlockMessage")
		msg.BlockMessage = &BlockMessage{
			Block: msg.VerifiedBlockMessage.VerifiedBlock.(Block),
			Vote:  msg.VerifiedBlockMessage.Vote,
		}
	}
}

func (c *testComm) isMessagePermitted(msg *Message, destination NodeID) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.messageFilter(msg, destination)
}

func (c *testComm) Broadcast(msg *Message) {
	if c.net.IsDisconnected(c.from) {
		return
	}

	c.maybeTranslateOutoingToIncomingMessageTypes(msg)

	for _, instance := range c.net.instances {
		if !c.isMessagePermitted(msg, instance.e.ID) {
			return
		}
		// Skip sending the message to yourself or disconnected nodes
		if bytes.Equal(c.from, instance.e.ID) || c.net.IsDisconnected(instance.e.ID) {
			continue
		}

		instance.ingress <- struct {
			msg  *Message
			from NodeID
		}{msg: msg, from: c.from}
	}
}

type inMemNetwork struct {
	t            *testing.T
	nodes        []NodeID
	instances    []*testNode
	lock         sync.RWMutex
	disconnected map[string]struct{}
}

// newInMemNetwork creates an in-memory network. Node IDs must be provided before
// adding instances, as nodes require prior knowledge of all participants.
func newInMemNetwork(t *testing.T, nodes []NodeID) *inMemNetwork {
	net := &inMemNetwork{
		t:            t,
		nodes:        nodes,
		instances:    make([]*testNode, 0),
		disconnected: make(map[string]struct{}),
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

func (n *inMemNetwork) setAllNodesMessageFilter(filter messageFilter) {
	for _, instance := range n.instances {
		instance.e.Comm.(*testComm).setFilter(filter)
	}
}

func (n *inMemNetwork) IsDisconnected(node NodeID) bool {
	n.lock.RLock()
	defer n.lock.RUnlock()

	_, ok := n.disconnected[string(node)]
	return ok
}

func (n *inMemNetwork) Connect(node NodeID) {
	n.lock.Lock()
	defer n.lock.Unlock()

	delete(n.disconnected, string(node))
}

func (n *inMemNetwork) Disconnect(node NodeID) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.disconnected[string(node)] = struct{}{}
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
		testBlockBuilder: testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)},
	}
}

func (t *testControlledBlockBuilder) triggerNewBlock() {
	select {
	case t.control <- struct{}{}:
	default:

	}
}

func (t *testControlledBlockBuilder) BuildBlock(ctx context.Context, metadata ProtocolMetadata) (VerifiedBlock, bool) {
	<-t.control
	return t.testBlockBuilder.BuildBlock(ctx, metadata)
}

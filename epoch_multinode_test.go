// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	. "github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/record"
	"github.com/ava-labs/simplex/testutil"
	"github.com/ava-labs/simplex/wal"
	"go.uber.org/zap"

	"github.com/stretchr/testify/require"
)

func TestSimplexMultiNodeSimple(t *testing.T) {
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	net := newInMemNetwork(t, nodes)
	newSimplexNode(t, nodes[0], net, nil)
	newSimplexNode(t, nodes[1], net, nil)
	newSimplexNode(t, nodes[2], net, nil)
	newSimplexNode(t, nodes[3], net, nil)

	net.startInstances()

	for seq := uint64(0); seq < 10; seq++ {
		net.triggerLeaderBlockBuilder(seq)
		for _, n := range net.instances {
			n.storage.waitForBlockCommit(seq)
		}
	}
}

func TestSimplexMultiNodeBlacklist(t *testing.T) {
	nodes := NodeIDs{{1}, {2}, {3}, {4}}
	net := newInMemNetwork(t, nodes)
	testEpochConfig := &testNodeConfig{
		replicationEnabled: true,
	}
	newSimplexNode(t, nodes[0], net, testEpochConfig)
	newSimplexNode(t, nodes[1], net, testEpochConfig)
	newSimplexNode(t, nodes[2], net, testEpochConfig)
	newSimplexNode(t, nodes[3], net, testEpochConfig)

	for _, n := range net.instances[:3] {
		n.Silence()
	}

	net.startInstances()

	// Advance to the fourth node's turn by building three blocks
	for seq := 0; seq < 3; seq++ {
		net.triggerLeaderBlockBuilder(uint64(seq))
		for _, n := range net.instances {
			n.storage.waitForBlockCommit(uint64(seq))
		}
	}

	// The fourth node is disconnected, so the rest should time out on it.
	net.Disconnect(nodes[3])

	for i := range net.instances[:3] {
		select {
		case net.instances[i].bb.blockShouldBeBuilt <- struct{}{}:
		default:

		}
	}

	for _, n := range net.instances[:3] {
		waitForBlockProposerTimeout(t, n.e, &n.e.StartTime, 3)
	}

	// Build two more blocks, which should blacklist the fourth node.
	// Ensure the fourth node is blacklisted by checking the blacklist on all nodes.

	// Build a block, ensure the blacklist contains the fourth node as a suspect.
	net.triggerLeaderBlockBuilder(uint64(4))
	for _, n := range net.instances[:3] {
		block := n.storage.waitForBlockCommit(uint64(3))
		blacklist := block.Blacklist()
		require.Equal(t, Blacklist{
			NodeCount:      4,
			SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 1, OrbitSuspected: 1}},
			Updates:        []BlacklistUpdate{{NodeIndex: 3, Type: BlacklistOpType_NodeSuspected}},
		}, blacklist)
	}

	// Reconnect the fourth node.
	net.Connect(nodes[3])

	// Build another block, ensure the blacklist contains the fourth node as blacklisted.
	net.instances[1].bb.triggerNewBlock()
	for _, n := range net.instances[:3] {
		block := n.storage.waitForBlockCommit(uint64(4))
		blacklist := block.Blacklist()
		require.Equal(t, Blacklist{
			NodeCount:      4,
			SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 1}},
			Updates:        []BlacklistUpdate{{NodeIndex: 3, Type: BlacklistOpType_NodeSuspected}},
		}, blacklist)
	}

	// Wait for the node to replicate the missing blocks.
	net.instances[3].bb.triggerNewBlock()
	block := net.instances[3].storage.waitForBlockCommit(4)
	require.Equal(t, Blacklist{
		NodeCount:      4,
		SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 1}},
		Updates:        []BlacklistUpdate{{NodeIndex: 3, Type: BlacklistOpType_NodeSuspected}},
	}, block.Blacklist())

	// Make another block.
	net.instances[2].bb.triggerNewBlock()
	for _, n := range net.instances {
		n.storage.waitForBlockCommit(uint64(5))
	}

	// The fourth node is still blacklisted, so it should not be able to propose a block.
	net.instances[3].bb.triggerNewBlock() // This shouldn't be here, this is just to side-step a bug.
	for _, n := range net.instances {
		waitForBlockProposerTimeout(t, n.e, &n.e.StartTime, 7)
	}

	// Disconnect the third node to force messages from the fourth node to be taken into account.
	net.Disconnect(nodes[2])

	// Make two blocks.
	allButThirdNode := []*testNode{net.instances[0], net.instances[1], net.instances[3]}
	for i := 0; i < 2; i++ {
		net.instances[i].bb.triggerNewBlock()
		for _, n := range allButThirdNode {
			n.bb.blockShouldBeBuilt <- struct{}{}
			n.storage.waitForBlockCommit(uint64(6 + i))
		}
	}

	// Skip the third node because it is disconnected.
	for i := range allButThirdNode {
		select {
		case net.instances[i].bb.blockShouldBeBuilt <- struct{}{}:
		default:

		}
	}

	for _, n := range allButThirdNode {
		waitForBlockProposerTimeout(t, n.e, &n.e.StartTime, 10)
	}

	// Since the fourth node is still blacklisted, we should skip this round.
	for _, n := range allButThirdNode {
		waitForBlockProposerTimeout(t, n.e, &n.e.StartTime, 11)
	}

	var lastBlacklist Blacklist

	// Create two blocks
	for i := 0; i < 2; i++ {
		net.instances[i].bb.triggerNewBlock()
		for _, n := range allButThirdNode {
			n.bb.blockShouldBeBuilt <- struct{}{}
			block := n.storage.waitForBlockCommit(uint64(8 + i))
			lastBlacklist = block.Blacklist()
		}
	}

	// The last node is not suspected anymore, it has been redeemed.
	require.False(t, lastBlacklist.IsNodeSuspected(3))

	// The third node will now time out.
	for i := range allButThirdNode {
		select {
		case net.instances[i].bb.blockShouldBeBuilt <- struct{}{}:
		default:

		}
	}

	for _, n := range allButThirdNode {
		waitForBlockProposerTimeout(t, n.e, &n.e.StartTime, 14)
	}

	// The fourth node should now be able to propose a block.
	net.instances[3].bb.triggerNewBlock()
	for _, n := range allButThirdNode {
		n.bb.blockShouldBeBuilt <- struct{}{}
		block := n.storage.waitForBlockCommit(uint64(10))
		lastBlacklist = block.Blacklist()
	}
}

func onlySendBlockProposalsAndVotes(splitNodes []NodeID) messageFilter {
	return func(m *Message, _, to NodeID) bool {
		if m.BlockMessage != nil {
			return true
		}
		for _, splitNode := range splitNodes {
			if to.Equals(splitNode) {
				return false
			}
		}
		return true
	}
}

// TestSplitVotes ensures that nodes who have timeout out, while the rest of the network has
// progressed due to notarizations, are able to collect the notarizations and continue
func TestSplitVotes(t *testing.T) {
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	net := newInMemNetwork(t, nodes)

	config := func(from NodeID) *testNodeConfig {
		return &testNodeConfig{
			comm: newTestComm(from, net, onlySendBlockProposalsAndVotes(nodes[2:])),
		}
	}

	newSimplexNode(t, nodes[0], net, config(nodes[0]))
	newSimplexNode(t, nodes[1], net, config(nodes[1]))
	splitNode2 := newSimplexNode(t, nodes[2], net, config(nodes[2]))
	splitNode3 := newSimplexNode(t, nodes[3], net, config(nodes[3]))

	net.startInstances()

	net.triggerLeaderBlockBuilder(0)
	for _, n := range net.instances {
		n.wal.assertBlockProposal(0)
		n.triggerBlockShouldBeBuilt()

		if n.e.ID.Equals(splitNode2.e.ID) || n.e.ID.Equals(splitNode3.e.ID) {
			require.Equal(t, uint64(0), n.e.Metadata().Round)
			waitForBlockProposerTimeout(t, n.e, &n.e.StartTime, 0)
			require.False(t, n.wal.containsNotarization(0))
		} else {
			n.wal.assertNotarization(0)
			require.Equal(t, uint64(1), n.e.Metadata().Round)
		}
	}

	net.setAllNodesMessageFilter(allowAllMessages)

	time2 := splitNode2.e.StartTime
	time3 := splitNode3.e.StartTime

	for {
		time2 = time2.Add(splitNode2.e.EpochConfig.MaxRebroadcastWait / 3)
		splitNode2.e.AdvanceTime(time2)

		time3 = time3.Add(splitNode3.e.EpochConfig.MaxRebroadcastWait / 3)
		splitNode3.e.AdvanceTime(time3)
		if splitNode2.wal.containsNotarization(0) && splitNode3.wal.containsNotarization(0) {
			break
		}
	}

	// splitNode3 will receive the notarization from splitNode2
	splitNode2.wal.assertNotarization(0)
	splitNode3.wal.assertNotarization(0)

	for _, n := range net.instances {
		require.Equal(t, uint64(0), n.e.Storage.NumBlocks())
		require.Equal(t, uint64(1), n.e.Metadata().Round)
		require.Equal(t, uint64(1), n.e.Metadata().Seq)
	}

	// once the new round gets finalized, it will re-broadcast
	// all past notarizations allowing the nodes to index both seq 0 & 1
	net.triggerLeaderBlockBuilder(1)

	for _, n := range net.instances {
		n.storage.waitForBlockCommit(0)
		n.storage.waitForBlockCommit(1)
		require.Equal(t, uint64(2), n.e.Storage.NumBlocks())
		require.Equal(t, uint64(2), n.e.Metadata().Round)
		require.Equal(t, uint64(2), n.e.Metadata().Seq)
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
func newSimplexNode(t *testing.T, nodeID NodeID, net *inMemNetwork, config *testNodeConfig) *testNode {
	comm := newTestComm(nodeID, net, allowAllMessages)
	bb := newTestControlledBlockBuilder(t)
	epochConfig := defaultTestNodeEpochConfig(t, nodeID, comm, bb)

	if config != nil {
		updateEpochConfig(&epochConfig, config)
	}

	e, err := NewEpoch(epochConfig)
	require.NoError(t, err)
	ti := &testNode{
		l:       epochConfig.Logger.(*testutil.TestLogger),
		wal:     epochConfig.WAL.(*testWAL),
		bb:      bb,
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
		epochConfig.Storage.Index(context.Background(), data.VerifiedBlock, data.Finalization)
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
		MaxRebroadcastWait:  DefaultMaxProposalWaitTime,
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
	l  *testutil.TestLogger
	t  *testing.T
	bb *testControlledBlockBuilder
}

// triggerBlockShouldBeBuilt signals this nodes block builder it is expecting a block to be built.
func (t *testNode) triggerBlockShouldBeBuilt() {
	select {
	case t.bb.blockShouldBeBuilt <- struct{}{}:
	default:
	}
}

func (t *testNode) Silence() {
	t.l.Silence()
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

func (tw *testWAL) assertNotarization(round uint64) uint16 {
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
					return record.NotarizationRecordType
				}
			}
			if binary.BigEndian.Uint16(rawRecord[:2]) == record.EmptyNotarizationRecordType {
				_, vote, err := ParseEmptyNotarizationRecord(rawRecord)
				require.NoError(tw.t, err)

				if vote.Round == round {
					return record.EmptyNotarizationRecordType
				}
			}
		}

		tw.signal.Wait()
	}

}

func (tw *testWAL) assertEmptyVote(round uint64) {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	for {
		rawRecords, err := tw.WriteAheadLog.ReadAll()
		require.NoError(tw.t, err)

		for _, rawRecord := range rawRecords {
			if binary.BigEndian.Uint16(rawRecord[:2]) == record.EmptyVoteRecordType {
				vote, err := ParseEmptyVoteRecord(rawRecord)
				require.NoError(tw.t, err)

				if vote.Round == round {
					return
				}
			}
		}

		tw.signal.Wait()
	}

}

func (tw *testWAL) assertBlockProposal(round uint64) {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	for {
		rawRecords, err := tw.WriteAheadLog.ReadAll()
		require.NoError(tw.t, err)

		for _, rawRecord := range rawRecords {
			if binary.BigEndian.Uint16(rawRecord[:2]) == record.BlockRecordType {
				bh, _, err := ParseBlockRecord(rawRecord)
				require.NoError(tw.t, err)

				if bh.Round == round {
					return
				}
			}
		}

		tw.signal.Wait()
	}

}

func (tw *testWAL) containsNotarization(round uint64) bool {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	rawRecords, err := tw.WriteAheadLog.ReadAll()
	require.NoError(tw.t, err)

	for _, rawRecord := range rawRecords {
		if binary.BigEndian.Uint16(rawRecord[:2]) == record.NotarizationRecordType {
			_, vote, err := ParseNotarizationRecord(rawRecord)
			require.NoError(tw.t, err)

			if vote.Round == round {
				return true
			}
		}
	}

	return false
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

// messageFilter is a function type that determines whether a message can be
// transmitted from one node to another.
// Parameters:
//   - msg: The message being evaluated for transmission
//   - from: The ID of the sending node
//   - to: The ID of the receiving node
//
// Returns:
//   - bool: true if the message can be transmitted, false otherwise
type messageFilter func(msg *Message, from NodeID, to NodeID) bool

// allowAllMessages allows every message to be sent
func allowAllMessages(*Message, NodeID, NodeID) bool {
	return true
}

// denyFinalizationMessages blocks any messages that would cause nodes in
// a network to index a block in storage.
func denyFinalizationMessages(msg *Message, _, _ NodeID) bool {
	if msg.FinalizeVote != nil {
		return false
	}
	if msg.Finalization != nil {
		return false
	}

	return true
}

func onlyAllowEmptyRoundMessages(msg *Message, _, _ NodeID) bool {
	if msg.EmptyNotarization != nil {
		return true
	}
	if msg.EmptyVoteMessage != nil {
		return true
	}
	return false
}

type testNetworkCommunication interface {
	Communication
	setFilter(filter messageFilter)
}

var _ testNetworkCommunication = (*testComm)(nil)

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

func (c *testComm) Nodes() []NodeID {
	return c.net.nodes
}

func (c *testComm) Send(msg *Message, destination NodeID) {
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
				if verifiedQuorumRound.Finalization != nil {
					quorumRound.Finalization = verifiedQuorumRound.Finalization
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
					Finalization:      msg.VerifiedReplicationResponse.LatestRound.Finalization,
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

	return c.messageFilter(msg, c.from, destination)
}

func (c *testComm) Broadcast(msg *Message) {
	if c.net.IsDisconnected(c.from) {
		return
	}

	c.maybeTranslateOutoingToIncomingMessageTypes(msg)

	for _, instance := range c.net.instances {
		if !c.isMessagePermitted(msg, instance.e.ID) {
			continue
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
	simplex.SortNodes(nodes)
	net := &inMemNetwork{
		t:            t,
		nodes:        nodes,
		instances:    make([]*testNode, 0),
		disconnected: make(map[string]struct{}),
	}
	return net
}

func (n *inMemNetwork) triggerLeaderBlockBuilder(round uint64) *testBlock {
	leader := simplex.LeaderForRound(n.nodes, round)
	for _, instance := range n.instances {
		if !instance.e.ID.Equals(leader) {
			continue
		}
		if n.IsDisconnected(leader) {
			instance.e.Logger.Info("triggering block build on disconnected leader", zap.Stringer("leader", leader))
		}

		// wait for the node to enter the round we expect it to propose a block for
		// otherwise we may trigger a build block too early
		waitToEnterRound(n.t, instance.e, round)

		instance.bb.triggerNewBlock()
		return <-instance.bb.out
	}

	// we should always find the leader
	require.Fail(n.t, "leader not found")
	return nil
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
		comm, ok := instance.e.Comm.(testNetworkCommunication)
		if !ok {
			continue
		}
		comm.setFilter(filter)
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

type testControlledBlockBuilder struct {
	t       *testing.T
	control chan struct{}
	testBlockBuilder
}

// newTestControlledBlockBuilder returns a BlockBuilder that only builds a block
// when triggerNewBlock is called.
func newTestControlledBlockBuilder(t *testing.T) *testControlledBlockBuilder {
	return &testControlledBlockBuilder{
		t:                t,
		control:          make(chan struct{}, 1),
		testBlockBuilder: *newTestBlockBuilder(),
	}
}

func (t *testControlledBlockBuilder) triggerNewBlock() {
	select {
	case t.control <- struct{}{}:
	default:

	}
}

func (t *testControlledBlockBuilder) BuildBlock(ctx context.Context, metadata ProtocolMetadata, blacklist Blacklist) (VerifiedBlock, bool) {
	select {
	case <-t.control:
	case <-ctx.Done():
		return nil, false
	}
	return t.testBlockBuilder.BuildBlock(ctx, metadata, blacklist)
}

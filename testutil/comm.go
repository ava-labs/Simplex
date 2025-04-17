package testutil

import (
	"bytes"
	"simplex"
	"sync"

	"github.com/stretchr/testify/require"
)

type NoopComm []simplex.NodeID

func NewNoopComm(nodeIDs []simplex.NodeID) NoopComm {
	return NoopComm(nodeIDs)
}

func (n NoopComm) ListNodes() []simplex.NodeID {
	return n
}

func (n NoopComm) SendMessage(*simplex.Message, simplex.NodeID) {

}

func (n NoopComm) Broadcast(msg *simplex.Message) {

}

// ListnerComm is a comm that listens for incoming messages
// and sends them to the [in] channel
type listnerComm struct {
	NoopComm
	In chan *simplex.Message
}

func NewListenerComm(nodeIDs []simplex.NodeID) *listnerComm {
	return &listnerComm{
		NoopComm: NoopComm(nodeIDs),
		In:       make(chan *simplex.Message, 1),
	}
}

func (b *listnerComm) SendMessage(msg *simplex.Message, id simplex.NodeID) {
	b.In <- msg
}

// messageFilter defines a function that filters
// certain messages from being sent or broadcasted.
type MessageFilter func(*simplex.Message, simplex.NodeID) bool

// allowAllMessages allows every message to be sent
func AllowAllMessages(*simplex.Message, simplex.NodeID) bool {
	return true
}

type testComm struct {
	from          simplex.NodeID
	net           *inMemNetwork
	messageFilter MessageFilter
	lock          sync.RWMutex
}

func NewTestComm(from simplex.NodeID, net *inMemNetwork, messageFilter MessageFilter) *testComm {
	return &testComm{
		from:          from,
		net:           net,
		messageFilter: messageFilter,
	}
}

func (c *testComm) ListNodes() []simplex.NodeID {
	return c.net.nodes
}

func (c *testComm) SendMessage(msg *simplex.Message, destination simplex.NodeID) {
	if !c.isMessagePermitted(msg, destination) {
		return
	}

	// cannot send if either [from] or [destination] is not connected
	if c.net.IsDisconnected(destination) || c.net.IsDisconnected(c.from) {
		return
	}

	c.maybeTranslateOutoingToIncomingMessageTypes(msg)

	for _, instance := range c.net.instances {
		if bytes.Equal(instance.E.ID, destination) {
			instance.ingress <- struct {
				msg  *simplex.Message
				from simplex.NodeID
			}{msg: msg, from: c.from}
			return
		}
	}
}

func (c *testComm) setFilter(filter MessageFilter) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.messageFilter = filter
}

func (c *testComm) maybeTranslateOutoingToIncomingMessageTypes(msg *simplex.Message) {
	if msg.VerifiedReplicationResponse != nil {
		data := make([]simplex.QuorumRound, 0, len(msg.VerifiedReplicationResponse.Data))

		for _, verifiedQuorumRound := range msg.VerifiedReplicationResponse.Data {
			// Outgoing block is of type verified block but incoming block is of type Block,
			// so we do a type cast because the test block implements both.
			quorumRound := simplex.QuorumRound{}
			if verifiedQuorumRound.EmptyNotarization != nil {
				quorumRound.EmptyNotarization = verifiedQuorumRound.EmptyNotarization
			} else {
				quorumRound.Block = verifiedQuorumRound.VerifiedBlock.(simplex.Block)
				if verifiedQuorumRound.Notarization != nil {
					quorumRound.Notarization = verifiedQuorumRound.Notarization
				}
				if verifiedQuorumRound.FCert != nil {
					quorumRound.FCert = verifiedQuorumRound.FCert
				}
			}

			data = append(data, quorumRound)
		}

		var latestRound *simplex.QuorumRound
		if msg.VerifiedReplicationResponse.LatestRound != nil {
			if msg.VerifiedReplicationResponse.LatestRound.EmptyNotarization != nil {
				latestRound = &simplex.QuorumRound{
					EmptyNotarization: msg.VerifiedReplicationResponse.LatestRound.EmptyNotarization,
				}
			} else {
				latestRound = &simplex.QuorumRound{
					Block:             msg.VerifiedReplicationResponse.LatestRound.VerifiedBlock.(simplex.Block),
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

		msg.ReplicationResponse = &simplex.ReplicationResponse{
			Data:        data,
			LatestRound: latestRound,
		}
	}

	if msg.VerifiedBlockMessage != nil {
		require.Nil(c.net.t, msg.BlockMessage, "message cannot include BlockMessage & VerifiedBlockMessage")
		msg.BlockMessage = &simplex.BlockMessage{
			Block: msg.VerifiedBlockMessage.VerifiedBlock.(simplex.Block),
			Vote:  msg.VerifiedBlockMessage.Vote,
		}
	}
}

func (c *testComm) isMessagePermitted(msg *simplex.Message, destination simplex.NodeID) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.messageFilter(msg, destination)
}

func (c *testComm) Broadcast(msg *simplex.Message) {
	if c.net.IsDisconnected(c.from) {
		return
	}

	c.maybeTranslateOutoingToIncomingMessageTypes(msg)

	for _, instance := range c.net.instances {
		if !c.isMessagePermitted(msg, instance.E.ID) {
			return
		}
		// Skip sending the message to yourself or disconnected nodes
		if bytes.Equal(c.from, instance.E.ID) || c.net.IsDisconnected(instance.E.ID) {
			continue
		}

		instance.ingress <- struct {
			msg  *simplex.Message
			from simplex.NodeID
		}{msg: msg, from: c.from}
	}
}

type RecordingComm struct {
	simplex.Communication
	BroadcastMessages chan *simplex.Message
}

func (rc *RecordingComm) Broadcast(msg *simplex.Message) {
	rc.BroadcastMessages <- msg
	rc.Communication.Broadcast(msg)
}

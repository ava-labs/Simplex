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
	in chan *simplex.Message
}

func NewListenerComm(nodeIDs []simplex.NodeID) *listnerComm {
	return &listnerComm{
		NoopComm: NoopComm(nodeIDs),
		in:       make(chan *simplex.Message, 1),
	}
}

func (b *listnerComm) SendMessage(msg *simplex.Message, id simplex.NodeID) {
	b.in <- msg
}

// messageFilter defines a function that filters
// certain messages from being sent or broadcasted.
type messageFilter func(*simplex.Message, simplex.NodeID) bool

// allowAllMessages allows every message to be sent
func allowAllMessages(*simplex.Message, simplex.NodeID) bool {
	return true
}

// denyFinalizationMessages blocks any messages that would cause nodes in
// a network to index a block in storage.
func denyFinalizationMessages(msg *simplex.Message, destination simplex.NodeID) bool {
	if msg.Finalization != nil {
		return false
	}
	if msg.FinalizationCertificate != nil {
		return false
	}

	return true
}

func onlyAllowEmptyRoundMessages(msg *simplex.Message, destination simplex.NodeID) bool {
	if msg.EmptyNotarization != nil {
		return true
	}
	if msg.EmptyVoteMessage != nil {
		return true
	}
	return false
}

type testComm struct {
	from          simplex.NodeID
	net           *inMemNetwork
	messageFilter messageFilter
	lock          sync.RWMutex
}

func newTestComm(from simplex.NodeID, net *inMemNetwork, messageFilter messageFilter) *testComm {
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
		if bytes.Equal(instance.e.ID, destination) {
			instance.ingress <- struct {
				msg  *simplex.Message
				from simplex.NodeID
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
		if !c.isMessagePermitted(msg, instance.e.ID) {
			return
		}
		// Skip sending the message to yourself or disconnected nodes
		if bytes.Equal(c.from, instance.e.ID) || c.net.IsDisconnected(instance.e.ID) {
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

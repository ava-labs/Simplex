package testutil

import (
	"bytes"
	"sync"

	"github.com/ava-labs/simplex"
	"github.com/stretchr/testify/require"
)

// MessageFilter is a function type that determines whether a message can be
// transmitted from one node to another.
// Parameters:
//   - msg: The message being evaluated for transmission
//   - from: The ID of the sending node
//   - to: The ID of the receiving node
//
// Returns:
//   - bool: true if the message can be transmitted, false otherwise
type MessageFilter func(msg *simplex.Message, from simplex.NodeID, to simplex.NodeID) bool

type NoopComm []simplex.NodeID

func (n NoopComm) Nodes() []simplex.NodeID {
	return n
}

func (n NoopComm) Send(*simplex.Message, simplex.NodeID) {

}

func (n NoopComm) Broadcast(msg *simplex.Message) {

}

type TestComm struct {
	from          simplex.NodeID
	net           *InMemNetwork
	messageFilter MessageFilter
	lock          sync.RWMutex
}

func NewTestComm(from simplex.NodeID, net *InMemNetwork, messageFilter MessageFilter) *TestComm {
	return &TestComm{
		from:          from,
		net:           net,
		messageFilter: messageFilter,
	}
}

func (c *TestComm) Nodes() []simplex.NodeID {
	return c.net.nodes
}

func (c *TestComm) Send(msg *simplex.Message, destination simplex.NodeID) {
	if !c.isMessagePermitted(msg, destination) {
		return
	}

	// cannot send if either [from] or [destination] is not connected
	if c.net.IsDisconnected(destination) || c.net.IsDisconnected(c.from) {
		return
	}

	c.maybeTranslateOutoingToIncomingMessageTypes(msg)

	for _, instance := range c.net.Instances {
		if bytes.Equal(instance.E.ID, destination) {
			instance.ingress <- struct {
				msg  *simplex.Message
				from simplex.NodeID
			}{msg: msg, from: c.from}
			return
		}
	}
}

func (c *TestComm) SetFilter(filter MessageFilter) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.messageFilter = filter
}

func (c *TestComm) maybeTranslateOutoingToIncomingMessageTypes(msg *simplex.Message) {
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
				if verifiedQuorumRound.Finalization != nil {
					quorumRound.Finalization = verifiedQuorumRound.Finalization
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

func (c *TestComm) isMessagePermitted(msg *simplex.Message, destination simplex.NodeID) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.messageFilter(msg, c.from, destination)
}

func (c *TestComm) Broadcast(msg *simplex.Message) {
	if c.net.IsDisconnected(c.from) {
		return
	}

	c.maybeTranslateOutoingToIncomingMessageTypes(msg)

	for _, instance := range c.net.Instances {
		if !c.isMessagePermitted(msg, instance.E.ID) {
			continue
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

// AllowAllMessages allows every message to be sent
func AllowAllMessages(*simplex.Message, simplex.NodeID, simplex.NodeID) bool {
	return true
}

func NewNoopComm(nodes []simplex.NodeID) NoopComm {
	return NoopComm(nodes)
}

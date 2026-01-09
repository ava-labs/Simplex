package random_network

import (
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

type Node struct {
	*testutil.BasicNode

	storage *testutil.InMemStorage
	bb      *RandomNetworkBlockBuilder

	logger *testutil.TestLogger
}

func NewNode(t *testing.T, net *testutil.BasicInMemoryNetwork, config *FuzzConfig, nodeID simplex.NodeID) *Node {
	l := testutil.MakeLogger(t, int(nodeID[0]))
	bb := NewNetworkBlockBuilder(config, l)
	comm := testutil.NewTestComm(nodeID, net, testutil.AllowAllMessages)
	epochConfig, _, storage := testutil.DefaultTestNodeEpochConfig(t, nodeID, comm, bb)
	epochConfig.Logger = l
	epochConfig.ReplicationEnabled = true

	e, err := simplex.NewEpoch(epochConfig)
	require.NoError(t, err)

	n := &Node{
		BasicNode: testutil.NewBasicNode(t, e, l),
		storage:   storage,
		bb:        bb,
		logger:    l,
	}

	n.BasicNode.CustomHandler = n.HandleMessage
	
	return n
}

func (n *Node) HandleMessage(msg *simplex.Message, from simplex.NodeID) error {
	switch {
		case msg.BlockMessage != nil:
			msg.BlockMessage.Block.(*Block).mempool = n.bb.mempool
		case msg.ReplicationResponse != nil:
			// convert quorum rounds to our type
			for _, qr := range msg.ReplicationResponse.Data {
				if qr.Block != nil {
					qr.Block.(*Block).mempool = n.bb.mempool
				}
			}

			if msg.ReplicationResponse.LatestRound != nil {
				rr := msg.ReplicationResponse.LatestRound
				if rr.Block != nil {
					rr.Block.(*Block).mempool = n.bb.mempool
				}
			}

			if msg.ReplicationResponse.LatestSeq != nil {
				rr := msg.ReplicationResponse.LatestSeq
				if rr.Block != nil {
					rr.Block.(*Block).mempool = n.bb.mempool
				}
			}
		case msg.VerifiedReplicationResponse != nil:
			panic("not implemented vrr")
		default:
			// no-op
	}
	return n.BasicNode.HandleMessage(msg, from)
}
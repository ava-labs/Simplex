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
			origBlock := msg.BlockMessage.Block.(*Block)
			// create a copy of the block with our mempool
			blockCopy := *origBlock
			blockCopy.mempool = n.bb.mempool
			msg.BlockMessage.Block = &blockCopy
		case msg.ReplicationResponse != nil:
			// convert quorum rounds to our type
			for i, qr := range msg.ReplicationResponse.Data {
				if qr.Block != nil {
					origBlock := qr.Block.(*Block)
					blockCopy := *origBlock
					blockCopy.mempool = n.bb.mempool
					msg.ReplicationResponse.Data[i].Block = &blockCopy
				}
			}

			if msg.ReplicationResponse.LatestRound != nil {
				if msg.ReplicationResponse.LatestRound.Block != nil {
					origBlock := msg.ReplicationResponse.LatestRound.Block.(*Block)
					blockCopy := *origBlock
					blockCopy.mempool = n.bb.mempool
					msg.ReplicationResponse.LatestRound.Block = &blockCopy
				}
			}

			if msg.ReplicationResponse.LatestSeq != nil {
				if msg.ReplicationResponse.LatestSeq.Block != nil {
					origBlock := msg.ReplicationResponse.LatestSeq.Block.(*Block)
					blockCopy := *origBlock
					blockCopy.mempool = n.bb.mempool
					msg.ReplicationResponse.LatestSeq.Block = &blockCopy
				}
			}
		case msg.VerifiedReplicationResponse != nil:
			panic("not implemented vrr")
		default:
			// no-op
	}
	return n.BasicNode.HandleMessage(msg, from)
}
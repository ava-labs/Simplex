package random_network

import (
	"fmt"
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

type Node struct {
	*testutil.BasicNode

	storage *testutil.InMemStorage
	mempool *Mempool

	logger *testutil.TestLogger
}

func NewNode(t *testing.T, net *testutil.BasicInMemoryNetwork, config *FuzzConfig, nodeID simplex.NodeID) *Node {
	l := testutil.MakeLogger(t, int(nodeID[0]))
	mempool := NewMempool(l, config)
	comm := testutil.NewTestComm(nodeID, net, testutil.AllowAllMessages)
	epochConfig, _, storage := testutil.DefaultTestNodeEpochConfig(t, nodeID, comm, mempool)
	epochConfig.Logger = l
	epochConfig.ReplicationEnabled = true
	epochConfig.BlockDeserializer = &BlockDeserializer{
		mempool: mempool,
	}

	e, err := simplex.NewEpoch(epochConfig)
	require.NoError(t, err)

	n := &Node{
		BasicNode: testutil.NewBasicNode(t, e, l),
		storage:   storage,
		mempool:   mempool,
		logger:    l,
	}

	n.BasicNode.CustomHandler = n.HandleMessage
	net.AddNode(n.BasicNode)

	return n
}

func (n *Node) HandleMessage(msg *simplex.Message, from simplex.NodeID) error {
	// Create a copy of the message to avoid mutating shared state in the in-memory network
	msgCopy := *msg

	switch {
	case msgCopy.BlockMessage != nil:
		block := msgCopy.BlockMessage.Block.(*Block)
		block.mempool.lock.Lock()

		// only create a copy if the mempool is different
		if block.mempool != n.mempool {
			blockCopy := *block
			blockCopy.mempool = n.mempool
			blockMsgCopy := *msgCopy.BlockMessage
			blockMsgCopy.Block = &blockCopy
			msgCopy.BlockMessage = &blockMsgCopy
		}

		block.mempool.lock.Unlock()
		fmt.Println("handling block in node", n.BasicNode.E.ID)

	case msgCopy.ReplicationResponse != nil:
		// Create a copy of ReplicationResponse to avoid mutating shared state
		rrCopy := *msgCopy.ReplicationResponse
		// Also copy the Data slice to avoid mutating shared slice
		rrCopy.Data = make([]simplex.QuorumRound, len(msgCopy.ReplicationResponse.Data))
		copy(rrCopy.Data, msgCopy.ReplicationResponse.Data)
		msgCopy.ReplicationResponse = &rrCopy

		// convert quorum rounds to our type
		for i, qr := range msgCopy.ReplicationResponse.Data {
			if qr.Block != nil {
				origBlock := qr.Block.(*Block)
				origBlock.mempool.lock.Lock()
				blockCopy := *origBlock
				blockCopy.mempool = n.mempool
				msgCopy.ReplicationResponse.Data[i].Block = &blockCopy
				origBlock.mempool.lock.Unlock()
			}
		}

		if msgCopy.ReplicationResponse.LatestRound != nil {
			latestRoundCopy := *msgCopy.ReplicationResponse.LatestRound
			msgCopy.ReplicationResponse.LatestRound = &latestRoundCopy
			if latestRoundCopy.Block != nil {
				origBlock := latestRoundCopy.Block.(*Block)
				origBlock.mempool.lock.Lock()
				blockCopy := *origBlock
				blockCopy.mempool = n.mempool
				msgCopy.ReplicationResponse.LatestRound.Block = &blockCopy
				origBlock.mempool.lock.Unlock()
			}
		}

		if msgCopy.ReplicationResponse.LatestSeq != nil {
			latestSeqCopy := *msgCopy.ReplicationResponse.LatestSeq
			msgCopy.ReplicationResponse.LatestSeq = &latestSeqCopy
			if latestSeqCopy.Block != nil {
				origBlock := latestSeqCopy.Block.(*Block)
				origBlock.mempool.lock.Lock()
				blockCopy := *origBlock
				blockCopy.mempool = n.mempool
				msgCopy.ReplicationResponse.LatestSeq.Block = &blockCopy
				origBlock.mempool.lock.Unlock()
			}
		}
		fmt.Println("handling rep in node", n.BasicNode.E.ID)

	case msgCopy.VerifiedReplicationResponse != nil:
		panic("not implemented vrr")
	default:
		// no-op
	}
	return n.BasicNode.HandleMessage(&msgCopy, from)
}

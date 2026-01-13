package random_network

import (
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

type Node struct {
	*testutil.BasicNode

	storage *Storage
	wal    *testutil.TestWAL
	mempool *Mempool

	logger *testutil.TestLogger
}

func NewNode(t *testing.T, net *testutil.BasicInMemoryNetwork, config *FuzzConfig, nodeID simplex.NodeID) *Node {
	l := testutil.MakeLogger(t, int(nodeID[0]))
	mempool := NewMempool(l, config)
	storage := NewStorage(mempool)
	comm := testutil.NewTestComm(nodeID, net, testutil.AllowAllMessages)
	epochConfig, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodeID, comm, mempool)
	epochConfig.Logger = l
	epochConfig.Storage = storage
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
		wal:       wal,
	}

	n.BasicNode.CustomHandler = n.HandleMessage
	net.AddNode(n.BasicNode)

	return n
}

func NewNodeWithExtras(t *testing.T, net *testutil.BasicInMemoryNetwork, nodeID simplex.NodeID, mempool *Mempool, wal *testutil.TestWAL, storage *Storage, logger *testutil.TestLogger) *Node {
	comm := testutil.NewTestComm(nodeID, net, testutil.AllowAllMessages)
	epochConfig, _, _ := testutil.DefaultTestNodeEpochConfig(t, nodeID, comm, mempool)
	epochConfig.Logger = logger
	epochConfig.Storage = storage
	epochConfig.WAL = wal
	epochConfig.ReplicationEnabled = true
	epochConfig.BlockDeserializer = &BlockDeserializer{
		mempool: mempool,
	}

	e, err := simplex.NewEpoch(epochConfig)
	require.NoError(t, err)

	n := &Node{
		BasicNode: testutil.NewBasicNode(t, e, logger),
		storage:   storage,
		mempool:   mempool,
		logger:    logger,
		wal:       wal,
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

		// only create a copy if the mempool is different
		blockCopy := *block
		blockCopy.mempool = n.mempool
		blockMsgCopy := *msgCopy.BlockMessage
		blockMsgCopy.Block = &blockCopy
		msgCopy.BlockMessage = &blockMsgCopy

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
				blockCopy := *origBlock
				blockCopy.mempool = n.mempool
				msgCopy.ReplicationResponse.Data[i].Block = &blockCopy
			}
		}

		if msgCopy.ReplicationResponse.LatestRound != nil {
			latestRoundCopy := *msgCopy.ReplicationResponse.LatestRound
			msgCopy.ReplicationResponse.LatestRound = &latestRoundCopy
			if latestRoundCopy.Block != nil {
				origBlock := latestRoundCopy.Block.(*Block)
				blockCopy := *origBlock
				blockCopy.mempool = n.mempool
				msgCopy.ReplicationResponse.LatestRound.Block = &blockCopy
			}
		}

		if msgCopy.ReplicationResponse.LatestSeq != nil {
			latestSeqCopy := *msgCopy.ReplicationResponse.LatestSeq
			msgCopy.ReplicationResponse.LatestSeq = &latestSeqCopy
			if latestSeqCopy.Block != nil {
				origBlock := latestSeqCopy.Block.(*Block)
				blockCopy := *origBlock
				blockCopy.mempool = n.mempool
				msgCopy.ReplicationResponse.LatestSeq.Block = &blockCopy
			}
		}

	case msgCopy.VerifiedReplicationResponse != nil:
		panic("not implemented vrr")
	default:
		// no-op
	}
	return n.BasicNode.HandleMessage(&msgCopy, from)
}

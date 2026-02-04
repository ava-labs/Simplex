// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package random_network

import (
	"encoding/binary"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

type randomNodeConfig struct {
	mempool *Mempool
	storage *Storage
	wal     *testutil.TestWAL
	logger  *testutil.TestLogger
}

type Node struct {
	*testutil.BasicNode

	storage *Storage
	wal     *testutil.TestWAL
	mempool *Mempool

	logger    *testutil.TestLogger
	isCrashed atomic.Bool
}

func NewNode(t *testing.T, nodeID simplex.NodeID, net *testutil.BasicInMemoryNetwork, config *FuzzConfig, nodeConfig randomNodeConfig) *Node {
	l := CreateNodeLogger(t, config, nodeID)
	mempool := NewMempool(l, config)
	if nodeConfig.mempool != nil {
		mempool = nodeConfig.mempool
	}
	comm := testutil.NewTestComm(nodeID, net, testutil.AllowAllMessages)
	epochConfig, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodeID, comm, mempool)
	epochConfig.Logger = l
	epochConfig.MaxRoundWindow = 100
	epochConfig.ReplicationEnabled = true

	// storage
	storage := NewStorage(mempool)
	if nodeConfig.storage != nil {
		storage = nodeConfig.storage
	}

	// wal
	if nodeConfig.wal != nil {
		wal = nodeConfig.wal
	}

	// logger
	if nodeConfig.logger != nil {
		l = nodeConfig.logger
	}

	epochConfig.Storage = storage
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
		isCrashed: atomic.Bool{},
	}

	n.isCrashed.Store(false)
	n.BasicNode.CustomHandler = n.HandleMessage

	return n
}

func (n *Node) HandleMessage(msg *simplex.Message, from simplex.NodeID) error {
	msgCopy := n.copyMessage(msg)
	return n.BasicNode.HandleMessage(&msgCopy, from)
}

func GenerateNodeIDFromRand(r *rand.Rand) simplex.NodeID {
	b := make([]byte, 32)

	for i := 0; i < len(b); i += 8 {
		binary.LittleEndian.PutUint64(b[i:], r.Uint64())
	}

	return simplex.NodeID(b)
}

func (n *Node) copyMessage(msg *simplex.Message) simplex.Message {
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
	default:
		// no-op
	}
	return msgCopy
}

func (n *Node) areTxsAccepted(txs []*TX) bool {
	n.mempool.lock.Lock()
	defer n.mempool.lock.Unlock()

	for _, tx := range txs {
		if _, exists := n.mempool.acceptedTXs[tx.ID]; !exists {
			return false
		}
	}
	return true
}

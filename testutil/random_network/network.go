package random_network

import (
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"go.uber.org/zap"
)

type Network struct {
	*testutil.BasicInMemoryNetwork
	l simplex.Logger

	nodes      []*Node
	randomness *rand.Rand
	config    *FuzzConfig
	stopped    atomic.Bool
}

func NewNetwork(config *FuzzConfig, t *testing.T, l simplex.Logger) *Network {
	l.Info("Initiating logger with random seed", zap.Int64("seed", config.RandomSeed))
	r := rand.New(rand.NewSource(config.RandomSeed))

	numNodes := r.Intn(config.MaxNodes-config.MinNodes+1) + config.MinNodes
	nodeIds := make([]simplex.NodeID, numNodes)
	for i := range numNodes {
		nodeIds[i] = testutil.GenerateNodeID(t)
	}
	
	nodes := make([]*Node, numNodes)
	
	l.Info("Initiating logger with nodes", zap.Int("num nodes", numNodes))
	basicNetwork := testutil.NewBasicInMemoryNetwork(t, nodeIds)

	for i := range numNodes {
		node := NewNode(t, basicNetwork, config, nodeIds[i])
		nodes[i] = node
		basicNetwork.AddNode(node.BasicNode)
	}

	return &Network{
		BasicInMemoryNetwork: basicNetwork,
		nodes:      nodes,
		randomness: r,
		l:          l,
		config:     config,
	}
}

func (n *Network) StartInstances() {
	n.BasicInMemoryNetwork.StartInstances()

	// start time updater
	amount := simplex.DefaultEmptyVoteRebroadcastTimeout / 5
	go n.UpdateTime(n.config.TimeUpdateFrequency, amount)
}

func (n *Network) UpdateTime(frequency time.Duration, increment time.Duration) {
	for !n.stopped.Load() {
		n.BasicInMemoryNetwork.AdvanceTime(increment)
		time.Sleep(frequency)
	}
}

func (n *Network) IssueTxs() {
		// Implementation of block building logic goes here
	numTxs := rand.Intn(n.config.MaxTxsPerBlock-n.config.MinTxsPerBlock+1) + n.config.MinTxsPerBlock // randomize between min and max inclusive
	txs := make([]*TX, 0, numTxs)

	for range numTxs {
		tx := CreateNewTX()
		if rand.Intn(100) < n.config.TxVVerificationFailure {
			n.l.Info("Building a block that will fail verification due to tx", zap.Stringer("txID", tx))
			tx.SetShouldFailVerification()
		}

		txs = append(txs, tx)
	}

	for _, node := range n.nodes {
		node.bb.mempool.AddPendingTXs(txs...)
	}
}
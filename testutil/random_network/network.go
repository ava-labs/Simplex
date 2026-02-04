// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package random_network

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Network struct {
	*testutil.BasicInMemoryNetwork
	l simplex.Logger
	t *testing.T

	lock       sync.Mutex
	nodes      []*Node
	numNodes   uint64
	randomness *rand.Rand
	config     *FuzzConfig

	// tx stats
	allIssuedTxs int
	failedTxs    int
}

func NewNetwork(config *FuzzConfig, t *testing.T, l simplex.Logger) *Network {
	// Use file-based logger if LogDirectory is configured
	if config.LogDirectory != "" {
		l = CreateNetworkLogger(t, config)
	}

	l.Info("Initiating logger with random seed", zap.Int64("seed", config.RandomSeed))
	r := rand.New(rand.NewSource(config.RandomSeed))

	numNodes := r.Intn(config.MaxNodes-config.MinNodes+1) + config.MinNodes
	nodeIds := make([]simplex.NodeID, numNodes)
	for i := range numNodes {
		nodeIds[i] = GenerateNodeIDFromRand(r)
	}

	nodes := make([]*Node, numNodes)

	l.Info("Initiating logger with nodes", zap.Int("num nodes", numNodes))
	basicNetwork := testutil.NewBasicInMemoryNetwork(t, nodeIds)

	for i := range numNodes {
		node := NewNode(t, nodeIds[i], basicNetwork, config, randomNodeConfig{})
		basicNetwork.AddNode(node.BasicNode)
		nodes[i] = node
	}

	return &Network{
		BasicInMemoryNetwork: basicNetwork,
		nodes:                nodes,
		randomness:           r,
		l:                    l,
		t:                    t,
		lock:                 sync.Mutex{},
		config:               config,
		numNodes:             uint64(numNodes),
	}
}

func (n *Network) StartInstances() {
	panic("Call Run() Instead")
}

func (n *Network) UpdateTime(frequency time.Duration, increment time.Duration, stop chan struct{}) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for {
		n.lock.Lock()
		n.BasicInMemoryNetwork.AdvanceTime(increment)
		n.lock.Unlock()

		select {
		case <-stop:
			return
		case <-ticker.C:
			continue
		}
	}
}

func (n *Network) IssueTxs() []*TX {
	n.lock.Lock()
	defer n.lock.Unlock()

	// Implementation of block building logic goes here
	numTxs := n.randomness.Intn(n.config.MaxTxsPerBlock-n.config.MinTxsPerBlock+1) + n.config.MinTxsPerBlock // randomize between min and max inclusive
	txs := make([]*TX, 0, numTxs)

	for range numTxs {
		tx := CreateNewTX()
		if n.randomness.Float64() < n.config.TxVerificationFailure {
			n.l.Info("Building a block that will fail verification due to tx", zap.Stringer("txID", tx))
			tx.SetShouldFailVerification()
			n.failedTxs++
		}

		txs = append(txs, tx)
		n.allIssuedTxs++
	}

	// first add to all mempools
	for _, node := range n.nodes {
		node.mempool.AddPendingTXs(txs...)
	}

	// then notify all mempools that new txs are ready
	for _, node := range n.nodes {
		node.mempool.NotifyTxsReady()
	}

	return txs
}

func (n *Network) Run() {
	n.BasicInMemoryNetwork.StartInstances()

	targetHeight := uint64(n.config.NumFinalizedBlocks)

	for n.getMinHeight() < targetHeight {
		n.crashAndRecoverNodes()
		txs := n.IssueTxs()
		n.waitForTxAcceptance(txs)

		time.Sleep(100 * time.Millisecond)
	}

	n.BasicInMemoryNetwork.StopInstances()
}

func (n *Network) waitForTxAcceptance(txs []*TX) {
	for {
		allAccepted := true
		for _, node := range n.nodes {
			if node.isCrashed.Load() {
				continue
			}

			if allAccepted := node.areTxsAccepted(txs); !allAccepted {
				allAccepted = false
				break
			}
		}

		if allAccepted {
			return
		}

		// advance time of all the nodes
		for _, node := range n.nodes {
			node.AdvanceTime(n.config.AdvanceTimeTickAmount)
		}
		time.Sleep(1 * time.Second)
	}
}

func (n *Network) numCrashedNodes() uint64 {
	numCrashed := 0
	for _, node := range n.nodes {
		if node.isCrashed.Load() {
			numCrashed++
		}
	}
	return uint64(numCrashed)
}

func (n *Network) crashAndRecoverNodes() {
	if n.config.NodeCrashPercentage == 0 {
		return
	}

	f := (int(n.numNodes) - 1) / 3

	if f == 0 {
		n.l.Info("Not enough nodes for crash testing", zap.Uint64("numNodes", n.numNodes))
		return
	}

	maxLeftToCrash := f - int(n.numCrashedNodes())
	// go through each node, randomly decide to crash based on NodeCrashPercentage
	for i, node := range n.nodes {
		isCrashed := node.isCrashed.Load()
		if isCrashed {
			// randomly decide to recover based on NodeRecoverPercentage
			if n.randomness.Float64() < n.config.NodeRecoverPercentage {
				n.l.Info("Recovering crashed node", zap.Stringer("NodeID", node.E.ID))
				n.startNode(i)

				maxLeftToCrash++
			}
			continue
		}

		// check if we can still crash more nodes
		if maxLeftToCrash <= 0 {
			continue
		}

		// randomly decide to crash the node
		if n.randomness.Float64() < n.config.NodeCrashPercentage {
			n.l.Info("Crashing node", zap.Stringer("NodeID", node.E.ID))
			maxLeftToCrash--
			n.crashNode(i)
		}
	}
}

func (n *Network) getMinHeight() uint64 {
	n.lock.Lock()
	defer n.lock.Unlock()

	minHeight := n.nodes[0].storage.NumBlocks()
	for _, node := range n.nodes[1:] {
		height := node.storage.NumBlocks()

		if height < minHeight {
			minHeight = height
		}
	}
	return minHeight
}

func (n *Network) SetInfoLog() {
	n.lock.Lock()
	defer n.lock.Unlock()

	for _, node := range n.nodes {
		node.logger.SetLevel(zapcore.InfoLevel)
	}
}

func (n *Network) PrintStatus() {
	n.lock.Lock()
	defer n.lock.Unlock()

	// prints the number of nodes
	n.l.Info("Network Status", zap.Int("num nodes", len(n.nodes)), zap.Int64("Seed", n.config.RandomSeed))

	// prints the number of txs in each node's mempool
	for _, node := range n.nodes {
		n.l.Info("Node Status", zap.Stringer("nodeID", node.E.ID), zap.Int("Short", int(node.E.ID[0])), zap.Uint64("Round", node.E.Metadata().Round), zap.Uint64("Height", node.storage.NumBlocks()))
	}

	// prints total issued txs and failed txs
	n.l.Info("Transaction Stats", zap.Int("total issued txs", n.allIssuedTxs), zap.Int("total failed txs", n.failedTxs))
}

func (n *Network) crashNode(idx int) {
	instance := n.nodes[idx]
	instance.isCrashed.Store(true)
	instance.Stop()
}

func (n *Network) startNode(idx int) {
	n.lock.Lock()

	instance := n.nodes[idx]
	nodeID := instance.E.ID
	mempool := instance.mempool
	clonedWal := instance.wal.Clone()
	clonedStorage := instance.storage.Clone()
	mempool.Clear()

	newNode := NewNode(n.t, nodeID, n.BasicInMemoryNetwork, n.config, randomNodeConfig{
		mempool: mempool,
		wal:     clonedWal,
		storage: clonedStorage,
		logger:  instance.logger,
	})
	n.nodes[idx] = newNode
	n.BasicInMemoryNetwork.ReplaceNode(newNode.BasicNode)
	n.lock.Unlock()

	newNode.Start()
}

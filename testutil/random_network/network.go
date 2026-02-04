// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package random_network

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Network struct {
	*testutil.BasicInMemoryNetwork
	logger *testutil.TestLogger
	t      *testing.T

	lock       sync.Mutex
	nodes      []*Node
	numNodes   uint64
	randomness *rand.Rand
	config     *FuzzConfig

	// tx stats
	allIssuedTxs int
	failedTxs    int
}

func NewNetwork(config *FuzzConfig, t *testing.T) *Network {
	if config.LogDirectory == "" {
		panic("Log Directory must be set")
	}

	logger := CreateNetworkLogger(t, config)
	logger.Info("Initiating logger with random seed", zap.Int64("seed", config.RandomSeed))
	r := rand.New(rand.NewSource(config.RandomSeed))

	numNodes := r.Intn(config.MaxNodes-config.MinNodes+1) + config.MinNodes
	nodeIds := make([]simplex.NodeID, numNodes)
	for i := range numNodes {
		nodeIds[i] = GenerateNodeIDFromRand(r)
	}

	nodes := make([]*Node, numNodes)

	logger.Info("Initiating logger with nodes", zap.Int("num nodes", numNodes))
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
		logger:               logger,
		t:                    t,
		lock:                 sync.Mutex{},
		config:               config,
		numNodes:             uint64(numNodes),
	}
}

func (n *Network) StartInstances() {
	panic("Call Run() Instead")
}

func (n *Network) Run() {
	n.BasicInMemoryNetwork.StartInstances()

	targetHeight := uint64(n.config.NumFinalizedBlocks)

	prevHeight := 0
	for n.getMinHeight() < targetHeight {
		n.crashAndRecoverNodes()
		txs := n.IssueTxs()
		n.waitForTxAcceptance(txs)

		// get the max height and ensure all nodes recover to that height
		maxHeight := n.getMaxHeight()
		n.recoverToHeight(maxHeight)
		n.logger.Info("Issued Transactions", zap.Int("count", len(txs)), zap.Uint64("min height", n.getMinHeight()), zap.Uint64("max height", n.getMaxHeight()))

		if prevHeight == int(n.getMaxHeight()) {
			panic(fmt.Sprintf(
				"not supposed to be equal: prevHeight=%d, maxHeight=%d",
				prevHeight,
				n.getMaxHeight(),
			))
		}

		prevHeight = int(n.getMaxHeight())
	}

	n.BasicInMemoryNetwork.StopInstances()
}

func (n *Network) recoverToHeight(height uint64) {
	for n.getMinHeight() < height {
		n.logger.Info("Advancing network time")
		for i, node := range n.nodes {
			isCrashed := node.isCrashed.Load()
			if isCrashed {
				// randomly decide to recover based on NodeRecoverPercentage
				if n.randomness.Float64() < n.config.NodeRecoverPercentage {
					n.startNode(i)
				}
			}
		}

		n.lock.Lock()
		n.BasicInMemoryNetwork.AdvanceTime(n.config.AdvanceTimeTickAmount)
		n.lock.Unlock()
	}

}

func (n *Network) IssueTxs() []*TX {
	n.lock.Lock()
	defer n.lock.Unlock()

	numTxs := n.randomness.Intn(n.config.MaxTxsPerBlock-n.config.MinTxsPerBlock+1) + n.config.MinTxsPerBlock // randomize between min and max inclusive
	txs := make([]*TX, 0, numTxs)

	for range numTxs {
		tx := CreateNewTX()
		// if n.randomness.Float64() < n.config.TxVerificationFailure {
		// 	n.logger.Info("Building a block that will fail verification due to tx", zap.Stringer("txID", tx))
		// 	tx.SetShouldFailVerification()
		// 	n.failedTxs++
		// }

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

func (n *Network) waitForTxAcceptance(txs []*TX) {
	for {
		allAccepted := true
		for _, node := range n.nodes {
			if node.isCrashed.Load() {
				continue
			}
			if accepted := node.areTxsAccepted(txs); !accepted {
				allAccepted = false
			}
		}

		if allAccepted {
			return
		}

		fmt.Println("advancing time")
		n.lock.Lock()
		n.BasicInMemoryNetwork.AdvanceTime(n.config.AdvanceTimeTickAmount)
		n.lock.Unlock()
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
		n.logger.Info("Not enough nodes for crash testing", zap.Uint64("numNodes", n.numNodes))
		return
	}

	crashedNodes := []string{}
	recoveredNodes := []string{}
	maxLeftToCrash := f - int(n.numCrashedNodes())
	// go through each node, randomly decide to crash based on NodeCrashPercentage
	for i, node := range n.nodes {
		isCrashed := node.isCrashed.Load()
		if isCrashed {
			// randomly decide to recover based on NodeRecoverPercentage
			if n.randomness.Float64() < n.config.NodeRecoverPercentage {
				n.startNode(i)
				recoveredNodes = append(recoveredNodes, node.E.ID.String())
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
			maxLeftToCrash--
			n.crashNode(i)
			crashedNodes = append(crashedNodes, node.E.ID.String())
		}
	}

	if len(recoveredNodes)+len(crashedNodes) > 0 {
		n.logger.Info("Recovered and crashed nodes", zap.Strings("crashed", crashedNodes), zap.Strings("recovered", recoveredNodes), zap.Uint64("num crashed", n.numCrashedNodes()))
	}
}

func (n *Network) getMinHeight() uint64 {
	minHeight := n.nodes[0].storage.NumBlocks()
	for _, node := range n.nodes[1:] {
		height := node.storage.NumBlocks()

		if height < minHeight {
			minHeight = height
		}
	}
	return minHeight
}

func (n *Network) getMaxHeight() uint64 {
	maxHeight := n.nodes[0].storage.NumBlocks()
	for _, node := range n.nodes[1:] {
		height := node.storage.NumBlocks()

		if height > maxHeight {
			maxHeight = height
		}
	}
	return maxHeight
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
	n.logger.Info("Network Status", zap.Int("num nodes", len(n.nodes)), zap.Int64("Seed", n.config.RandomSeed))

	// prints the number of txs in each node's mempool
	for _, node := range n.nodes {
		n.logger.Info("Node Status", zap.Stringer("nodeID", node.E.ID), zap.Int("Short", int(node.E.ID[0])), zap.Uint64("Round", node.E.Metadata().Round), zap.Uint64("Height", node.storage.NumBlocks()))
	}

	// prints total issued txs and failed txs
	n.logger.Info("Transaction Stats", zap.Int("total issued txs", n.allIssuedTxs), zap.Int("total failed txs", n.failedTxs))
}

func (n *Network) crashNode(idx int) {
	instance := n.nodes[idx]
	instance.isCrashed.Store(true)
	instance.Stop()
}

func (n *Network) startNode(idx int) {
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

	newNode.Start()
}

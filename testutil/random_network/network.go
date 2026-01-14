package random_network

import (
	"math/rand"
	"sync"
	"sync/atomic"
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

	lock 	 sync.Mutex
	nodes      []*Node
	randomness *rand.Rand
	config     *FuzzConfig
	stopped    atomic.Bool
	issuedTxs []*TX
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
		nodeIds[i] = testutil.GenerateNodeID(t)
	}

	nodes := make([]*Node, numNodes)

	l.Info("Initiating logger with nodes", zap.Int("num nodes", numNodes))
	basicNetwork := testutil.NewBasicInMemoryNetwork(t, nodeIds)

	for i := range numNodes {
		node := NewNode(t, basicNetwork, config, nodeIds[i])
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
		issuedTxs:            make([]*TX, 0),
	}
}

func (n *Network) StartInstances() {
	n.BasicInMemoryNetwork.StartInstances()

	// start time updater
	amount := simplex.DefaultEmptyVoteRebroadcastTimeout / 10
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
			// n.l.Info("Building a block that will fail verification due to tx", zap.Stringer("txID", tx))
			// tx.SetShouldFailVerification()
		}

		txs = append(txs, tx)
		n.issuedTxs = append(n.issuedTxs, tx)
	}

	for _, node := range n.nodes {
		node.mempool.AddPendingTXs(txs...)
	}

	for _, node := range n.nodes {
		node.mempool.NotifyTxsReady()
	}
}

func (n *Network) Run() {
	stop := make(chan struct{})
	defer close(stop)

	n.startTransactionIssuance(stop)
	n.startCrashRestartCycle(stop)
	n.waitForTargetHeight()
}

func (n *Network) startTransactionIssuance(stop chan struct{}) {
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				n.IssueTxs()

				delay := n.calculateTxIssuanceDelay()
				select {
				case <-time.After(delay):
				case <-stop:
					return
				}
			}
		}
	}()
}

func (n *Network) calculateTxIssuanceDelay() time.Duration {
	delay := n.config.MinTxIssuanceDelay
	if n.config.MaxTxIssuanceDelay > n.config.MinTxIssuanceDelay {
		randomDelta := n.randomness.Int63n(int64(n.config.MaxTxIssuanceDelay - n.config.MinTxIssuanceDelay))
		delay += time.Duration(randomDelta)
	}
	return delay
}

func (n *Network) startCrashRestartCycle(stop chan struct{}) {
	if n.config.CrashInterval == 0 {
		return
	}

	f := (len(n.nodes) - 1) / 3
	if f == 0 {
		n.l.Info("Not enough nodes for crash testing", zap.Int("numNodes", len(n.nodes)))
		return
	}

	go func() {
		var crashedNodes []uint64
		crashPhase := true

		ticker := time.NewTicker(n.config.CrashInterval)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if crashPhase {
					crashedNodes = n.crashRandomNodes(f, crashedNodes)
				} else {
					n.restartCrashedNodes(crashedNodes)
					crashedNodes = nil
				}
				crashPhase = !crashPhase
			}
		}
	}()
}

func (n *Network) crashRandomNodes(f int, crashedNodes []uint64) []uint64 {
	// Calculate how many more nodes we can crash (max f total)
	maxAdditionalCrashes := f - len(crashedNodes)
	if maxAdditionalCrashes <= 0 {
		n.l.Info("Already at maximum crashed nodes", zap.Int("crashedNodes", len(crashedNodes)), zap.Int("f", f))
		return crashedNodes
	}

	availableNodes := n.getAvailableNodes(crashedNodes)
	if len(availableNodes) == 0 {
		return crashedNodes
	}

	// Randomly select 1 to maxAdditionalCrashes nodes to crash
	numToCrash := n.randomness.Intn(maxAdditionalCrashes) + 1
	numToCrash = min(numToCrash, len(availableNodes))

	n.randomness.Shuffle(len(availableNodes), func(i, j int) {
		availableNodes[i], availableNodes[j] = availableNodes[j], availableNodes[i]
	})

	nodesToCrash := availableNodes[:numToCrash]
	n.l.Info("Crashing nodes",
		zap.Int("numToCrash", numToCrash),
		zap.Int("totalCrashed", len(crashedNodes)+numToCrash),
		zap.Int("f", f),
		zap.Uint64s("nodeIndexes", nodesToCrash))
	n.CrashNodes(nodesToCrash...)

	return append(crashedNodes, nodesToCrash...)
}

func (n *Network) getAvailableNodes(crashedNodes []uint64) []uint64 {
	availableNodes := make([]uint64, 0, len(n.nodes))
	for i := range n.nodes {
		nodeIdx := uint64(i)
		isCrashed := false
		for _, crashed := range crashedNodes {
			if nodeIdx == crashed {
				isCrashed = true
				break
			}
		}
		if !isCrashed {
			availableNodes = append(availableNodes, nodeIdx)
		}
	}
	return availableNodes
}

func (n *Network) restartCrashedNodes(crashedNodes []uint64) {
	if len(crashedNodes) == 0 {
		return
	}

	n.l.Info("Restarting nodes",
		zap.Int("numNodes", len(crashedNodes)),
		zap.Uint64s("nodeIndexes", crashedNodes))
	n.StartNodes(crashedNodes...)
}

func (n *Network) waitForTargetHeight() {
	const pollInterval = 100 * time.Millisecond
	targetHeight := uint64(n.config.NumFinalizedBlocks)

	for {
		maxHeight := n.getMaxHeight()
		if maxHeight >= targetHeight {
			n.l.Info("Target height reached",
				zap.Uint64("height", maxHeight),
				zap.Uint64("target", targetHeight))
			return
		}
		time.Sleep(pollInterval)
	}
}

func (n *Network) getMaxHeight() uint64 {
	maxHeight := uint64(0)
	for _, node := range n.nodes {
		height := node.storage.NumBlocks()
		if height > maxHeight {
			maxHeight = height
		}
	}
	return maxHeight
}

func (n *Network) WaitForTxAcceptance() {
	const pollInterval = 100 * time.Millisecond
	const timeout = 30 * time.Second

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		allAccepted := true

		// Check each issued transaction
		for _, tx := range n.issuedTxs {
			if tx.shouldFailVerification {
				// Ensure tx was NOT accepted by any node
				for _, node := range n.nodes {
					if node.mempool.IsTxAccepted(tx.ID) {
						n.l.Error("Transaction that should fail verification was accepted",
							zap.Stringer("txID", tx),
							zap.Stringer("nodeID", node.E.ID))
						allAccepted = false
						break
					}
				}
			} else {
				// Ensure tx was accepted by all nodes
				for _, node := range n.nodes {
					if !node.mempool.IsTxAccepted(tx.ID) {
						allAccepted = false
						break
					}
				}
			}

			if !allAccepted {
				break
			}
		}

		for _, node := range n.nodes {
			node.logger.Info("Ensuring no verified & unaccepted transactions remain...", zap.Int("num verified but not accepted", node.mempool.NumVerifiedBlocks()))
			if node.mempool.NumVerifiedBlocks() > 0 {
				allAccepted = false
			}
		}

		if allAccepted {
			n.l.Info("All transactions accepted as expected")
			return
		}

	
		time.Sleep(pollInterval)
	}

	// Timeout - log detailed status
	n.l.Warn("WaitForTxAcceptance timeout - printing detailed status")
	for _, tx := range n.issuedTxs {
		acceptanceStatus := make([]bool, len(n.nodes))
		for i, node := range n.nodes {
			acceptanceStatus[i] = node.mempool.IsTxAccepted(tx.ID)
		}

		n.l.Warn("Transaction acceptance status",
			zap.Stringer("txID", tx),
			zap.Bool("shouldFail", tx.shouldFailVerification),
			zap.Bools("acceptedByNodes", acceptanceStatus))
	}
}

func (n *Network) SetInfoLog() {
	for _, node := range n.nodes {
		node.logger.SetLevel(zapcore.InfoLevel)
	}
}

func (n *Network) PrintStatus() {
	// prints the number of nodes
	n.l.Info("Network Status", zap.Int("num nodes", len(n.nodes)), zap.Int64("Seed", n.config.RandomSeed))

	// prints the number of txs in each node's mempool
	for _, node := range n.nodes {
		numPendingTxs := len(node.mempool.unacceptedTxs)
		numVerifiedButNotAcceptedTxs := node.mempool.NumVerifiedBlocks()
		numAcceptedTxs := len(node.mempool.acceptedTXs)
		n.l.Info("Node Status", zap.Stringer("nodeID", node.E.ID), zap.Int("Short", int(node.E.ID[0])), zap.Int("pending txs", numPendingTxs), zap.Int("verified but not accepted txs", numVerifiedButNotAcceptedTxs), zap.Int("accepted txs", numAcceptedTxs), zap.Uint64("Height", node.storage.NumBlocks()))
	}
}


func (n *Network) CrashNodes(nodeIndexes ...uint64) {
	for _, idx := range nodeIndexes {
		n.lock.Lock()
		instance := n.nodes[idx]
		instance.Stop()

		n.lock.Unlock()
	}
}

func (n *Network) StartNodes(nodeIndexes ...uint64) {
	for _, idx := range nodeIndexes {
		n.lock.Lock()
		instance := n.nodes[idx]

		nodeID := instance.E.ID
		mempool := instance.mempool
		clonedWal := instance.wal.Clone()
		clonedStorage := instance.storage.Clone()
		mempool.Clear()

		// Remove the old stopped instance from the network's instances list
		n.BasicInMemoryNetwork.RemoveNode(instance.BasicNode)

		newNode := NewNodeWithExtras(n.t, n.BasicInMemoryNetwork, nodeID, mempool, clonedWal, clonedStorage, instance.logger)

		n.nodes[idx] = newNode
		n.lock.Unlock()
		newNode.Start()
	}
}
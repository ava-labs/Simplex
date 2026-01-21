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
	}
}

func (n *Network) StartInstances() {
	panic("Call Run() Instead")
}

func (n *Network) UpdateTime(frequency time.Duration, increment time.Duration, stop chan struct{}) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for {
		n.BasicInMemoryNetwork.AdvanceTime(increment)

		select {
		case <-stop:
			return
		case <-ticker.C:
			continue
		}
	}
}

func (n *Network) IssueTxs() {
	// Implementation of block building logic goes here
	numTxs := rand.Intn(n.config.MaxTxsPerBlock-n.config.MinTxsPerBlock+1) + n.config.MinTxsPerBlock // randomize between min and max inclusive
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
}

func (n *Network) Run() {
	stop := make(chan struct{})

	n.BasicInMemoryNetwork.StartInstances()
	go n.UpdateTime(n.config.TimeUpdateFrequency, n.config.TimeUpdateAmount, stop)
	go n.startTransactionIssuance(stop)
	go n.startCrashRestartCycle(stop)

	n.waitForTargetHeight()

	close(stop)
	n.BasicInMemoryNetwork.StopInstances()

}

func (n *Network) startTransactionIssuance(stop chan struct{}) {
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
}

func (n *Network) calculateTxIssuanceDelay() time.Duration {
	delay := n.config.MinTxIssuanceDelay
	if n.config.MaxTxIssuanceDelay > n.config.MinTxIssuanceDelay {
		randomDelta := n.randomness.Int63n(int64(n.config.MaxTxIssuanceDelay - n.config.MinTxIssuanceDelay))
		delay += time.Duration(randomDelta)
	}
	return delay
}

// startCrashRestartCycle periodically crashes and restarts nodes in the network
// it crashes up to f nodes at a time, where f is the maximum number of faulty nodes tolerated by the network
// it alternates between crashing and restarting nodes at each interval
func (n *Network) startCrashRestartCycle(stop chan struct{}) {
	if n.config.CrashInterval == 0 {
		return
	}

	f := (len(n.nodes) - 1) / 3
	if f == 0 {
		n.l.Info("Not enough nodes for crash testing", zap.Int("numNodes", len(n.nodes)))
		return
	}

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
		minHeight := n.getMinHeight()
		if minHeight >= targetHeight {
			n.l.Info("Target height reached",
				zap.Uint64("height", minHeight),
				zap.Uint64("target", targetHeight))
			return
		}
		time.Sleep(pollInterval)
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
		n.l.Info("Node Status", zap.Stringer("nodeID", node.E.ID), zap.Int("Short", int(node.E.ID[0])), zap.Uint64("Round", node.E.Metadata().Round), zap.Uint64("Height", node.storage.NumBlocks()))
	}

	// prints total issued txs and failed txs
	n.l.Info("Transaction Stats", zap.Int("total issued txs", n.allIssuedTxs), zap.Int("total failed txs", n.failedTxs))
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
}

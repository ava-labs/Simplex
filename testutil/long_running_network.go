package testutil

import (
	"context"
	"crypto/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/stretchr/testify/require"
)

// Set this to a higher value so we don't overload lagging nodes with replication requests
const longRunningMaxRoundWindow = 100

type LongRunningInMemoryNetwork struct {
	*InMemNetwork

	stopped atomic.Bool
}

func NewDefaultLongRunningNetwork(t *testing.T, numNodes int) *LongRunningInMemoryNetwork {
	nodes := make([]simplex.NodeID, numNodes)
	for i := range numNodes {
		nodes[i] = generateNodeID(t)
	}

	net := NewInMemNetwork(t, nodes)
	for _, nodeID := range nodes {
		node := NewSimplexNode(t, nodeID, net, &TestNodeConfig{
			BlockBuilder:       NewNetworkBlockBuilder(t),
			ReplicationEnabled: true,
			MaxRoundWindow:     longRunningMaxRoundWindow,
		})
		node.l.SetPanicOnError(true)
		node.l.SetPanicOnWarn(true)
	}
	return &LongRunningInMemoryNetwork{
		InMemNetwork: net,
	}
}

func (n *LongRunningInMemoryNetwork) StartInstances() {
	require.Equal(n.t, len(n.nodes), len(n.Instances))

	for i := len(n.nodes) - 1; i >= 0; i-- {
		n.Instances[i].Start()
	}

	amount := simplex.DefaultEmptyVoteRebroadcastTimeout / 5
	go n.UpdateTime(100*time.Millisecond, amount)
}

func (n *LongRunningInMemoryNetwork) UpdateTime(frequency time.Duration, amount time.Duration) {
	for !n.stopped.Load() {
		for _, instance := range n.Instances {
			instance.AdvanceTime(amount)
		}
		time.Sleep(frequency)
	}
}

func (n *LongRunningInMemoryNetwork) CrashNodes(nodeIndexes ...uint64) {
	for _, idx := range nodeIndexes {
		instance := n.Instances[idx]
		instance.E.Stop()
	}
}

func (n *LongRunningInMemoryNetwork) RestartNodes(nodeIndexes ...uint64) {
	for _, idx := range nodeIndexes {
		instance := n.Instances[idx]
		nodeID := instance.E.ID
		logger := instance.l
		bb := instance.BB
		clonedWal := instance.WAL.Clone()
		clonedStorage := instance.Storage.Clone()

		newNode := newTestNode(n.t, nodeID, n.InMemNetwork, &TestNodeConfig{
			BlockBuilder:       bb,
			ReplicationEnabled: true,
			MaxRoundWindow:     longRunningMaxRoundWindow,
			Logger:             logger,
			WAL:                clonedWal,
			Storage:            clonedStorage,
			StartTime:          instance.currentTime.Load(),
		})

		n.Instances[idx] = newNode
		newNode.Start()
	}
}

func (n *LongRunningInMemoryNetwork) NoMoreBlocks() {
	for _, instance := range n.Instances {
		bb, ok := instance.BB.(*NetworkBlockBuilder)
		if !ok {
			continue
		}
		bb.TriggerBlockShouldNotBeBuilt()
	}
}

func (n *LongRunningInMemoryNetwork) ContinueBlocks() {
	for _, instance := range n.Instances {
		bb, ok := instance.BB.(*NetworkBlockBuilder)
		if !ok {
			continue
		}
		bb.TriggerBlockShouldBeBuilt()
	}
}

func (n *LongRunningInMemoryNetwork) WaitForNodesToEnterRound(round uint64, nodeIndexes ...uint64) {
	// check if nodeIndexes have length 0
	if len(nodeIndexes) == 0 {
		for _, instance := range n.Instances {
			WaitToEnterRoundWithTimeout(n.t, instance.E, round, 3*time.Second)
		}
	}

	for _, idx := range nodeIndexes {
		WaitToEnterRoundWithTimeout(n.t, n.Instances[idx].E, round, 3*time.Second)
	}
}

func (n *LongRunningInMemoryNetwork) DisconnectNodes(nodeIndexes ...uint64) {
	for _, idx := range nodeIndexes {
		nodeID := n.Instances[idx].E.ID
		n.Disconnect(nodeID)
	}
}

func (n *LongRunningInMemoryNetwork) ConnectNodes(nodeIndexes ...uint64) {
	for _, idx := range nodeIndexes {
		nodeID := n.Instances[idx].E.ID
		n.Connect(nodeID)
	}
}

// StopAndAssert stops all nodes and asserts their storage and WALs are consistent.
// If tailingMessages is true, it will also assert that no extra messages are being sent
// after telling the network no more blocks should be built.
// TailingMessages is useful for debugging, but is currently flakey because NoMoreBlocks does not stop block building
// atomically for all nodes in the network. Therefore, some nodes may timeout on a block they think should be built, and keep resending empty votes.
func (n *LongRunningInMemoryNetwork) StopAndAssert(tailingMessages bool) {
	n.NoMoreBlocks()

	// check all the nodes have the same wal, storage, etc
	for i, instance := range n.Instances {
		instance.WAL.AssertHealthy(instance.E.BlockDeserializer, instance.E.QCDeserializer)
		if i != 0 {
			instance.Storage.Compare(n.Instances[0].Storage)
		}
		instance.E.Stop()
	}

	// print summary of messages sent
	for i, instance := range n.Instances {
		comm, ok := instance.E.Comm.(*TestComm)
		if !ok {
			continue
		}
		n.t.Logf("Node %d (%s) sent message types: %+v", i, instance.E.ID.String(), comm.messageTypesSent)
	}

	if tailingMessages {
		// assert no extra messages/requests are being sent
		time.Sleep(3 * time.Second)
		for _, instance := range n.Instances {
			comm, ok := instance.E.Comm.(*TestComm)
			if !ok {
				continue
			}
			comm.messageTypesSent = make(map[string]uint64)
		}
		time.Sleep(3 * time.Second)
		for _, instance := range n.Instances {
			comm, ok := instance.E.Comm.(*TestComm)
			if !ok {
				continue
			}
			require.Empty(n.t, comm.messageTypesSent, "expected no messages to be sent after telling the network no more blocks should be built, node ID: %s\n msg %+v", instance.E.ID.String(), comm.messageTypesSent)
		}
	}

	n.stopped.Store(true)
}

func generateNodeID(t *testing.T) simplex.NodeID {
	b := make([]byte, 32)

	// fill with cryptographically secure random data
	_, err := rand.Read(b)
	require.NoError(t, err)

	return simplex.NodeID(b)
}

var _ simplex.BlockBuilder = (*NetworkBlockBuilder)(nil)

type NetworkBlockBuilder struct {
	t  *testing.T
	mu sync.Mutex

	blockPending bool
	cond         *sync.Cond
}

func NewNetworkBlockBuilder(t *testing.T) *NetworkBlockBuilder {
	bb := &NetworkBlockBuilder{
		t:            t,
		blockPending: true,
	}
	bb.cond = sync.NewCond(&bb.mu)
	return bb
}

func (b *NetworkBlockBuilder) BuildBlock(ctx context.Context, metadata simplex.ProtocolMetadata, blacklist simplex.Blacklist) (simplex.VerifiedBlock, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for !b.blockPending {
		b.cond.Wait()
	}

	return NewTestBlock(metadata, blacklist), true
}

func (b *NetworkBlockBuilder) WaitForPendingBlock(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for !b.blockPending {
		b.cond.Wait()
	}
}

func (b *NetworkBlockBuilder) TriggerBlockShouldBeBuilt() {
	b.mu.Lock()
	b.blockPending = true
	b.mu.Unlock()
	b.cond.Broadcast()
}

func (b *NetworkBlockBuilder) TriggerBlockShouldNotBeBuilt() {
	b.mu.Lock()
	b.blockPending = false
	b.mu.Unlock()
	b.cond.Broadcast()
}

func (b *NetworkBlockBuilder) ShouldBlockBeBuilt() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.blockPending
}

func (b *NetworkBlockBuilder) TriggerNewBlock() {}

package random_network

import (
	"math/rand"
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"go.uber.org/zap"
)

type Network struct {
	*testutil.InMemNetwork
	l simplex.Logger

	nodes      []*Node
	randomness *rand.Rand
}

func NewNetwork(config *FuzzConfig, t *testing.T, l simplex.Logger) *Network {
	l.Info("Initiating logger with random seed", zap.Int64("seed", config.RandomSeed))
	r := rand.New(rand.NewSource(config.RandomSeed))

	numNodes := r.Intn(config.MaxNodes-config.MinNodes+1) + config.MinNodes
	nodes := make([]*Node, numNodes)

	l.Info("Initiating logger with nodes", zap.Int("num nodes", numNodes))
	for i := range numNodes {
		nodes[i] = NewNode()
	}

	return &Network{
		nodes:      nodes,
		randomness: r,
		l:          l,
	}
}

func NewRandomNetowrkNode(t *testing.T, nodeID simplex.NodeID, net *testutil.InMemNetwork) *Node {
	// node := testutil.NewSimplexNode(t, nodeID, net, &TestNodeConfig{
	// 		BlockBuilder:       NewNetworkBlockBuilder(t),
	// 		ReplicationEnabled: true,
	// 		MaxRoundWindow:     longRunningMaxRoundWindow,
	// 	})
	// return node
	return nil
}

func (n *Network) Start() error {
	return nil
}

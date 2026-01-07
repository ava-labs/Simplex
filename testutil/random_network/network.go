package random_network

import (
	"math/rand"

	"github.com/ava-labs/simplex"
	"go.uber.org/zap"
)

type Network struct {
	l simplex.Logger
	
	nodes []*Node
	randomness *rand.Rand
}

func NewNetwork(config *FuzzConfig, l simplex.Logger) *Network {
	l.Info("Initiating logger with random seed", zap.Int64("seed", config.RandomSeed))
	r := rand.New(rand.NewSource(config.RandomSeed))

	numNodes := r.Intn(config.MaxNodes - config.MinNodes + 1) + config.MinNodes
	nodes := make([]*Node, numNodes)

	l.Info("Initiating logger with nodes", zap.Int("num nodes", numNodes))
	for i := range numNodes {
		nodes[i] = NewNode()
	}
	
	return &Network{
		nodes: nodes,
		randomness: r,
		l: l,
	}
}

func (n *Network) Start() error {
	return nil
}

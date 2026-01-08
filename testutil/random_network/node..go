package random_network

import (
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

type Node struct {
	*testutil.BasicNode
	
	storage *testutil.InMemStorage
	bb 	*RandomNetworkBlockBuilder

	logger *testutil.TestLogger
}

func NewNode(t *testing.T, net *testutil.BasicInMemoryNetwork, config *FuzzConfig, nodeID simplex.NodeID) *Node {
	bb := NewNetworkBlockBuilder(config, nil)
	comm := testutil.NewTestComm(nodeID, net, testutil.AllowAllMessages)
	epochConfig, _, storage := testutil.DefaultTestNodeEpochConfig(t, nodeID, comm, bb)
	e, err := simplex.NewEpoch(epochConfig)
	require.NoError(t, err)
	
	logger := epochConfig.Logger.(*testutil.TestLogger)
	bb.l = logger

	return &Node{
		BasicNode: testutil.NewBasicNode(t, e, logger),
		storage:   storage,
		bb:        bb,
		logger:    logger,
	}
}

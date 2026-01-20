package long_running

import (
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

type LongRunningNode struct {
	*testutil.BasicNode
	bb      *LongRunningBlockBuilder
	wal     *testutil.TestWAL
	Storage *testutil.InMemStorage
	logger  *testutil.TestLogger
}

func NewLongRunningNode(t *testing.T, nodeID simplex.NodeID, basicNetwork *testutil.BasicInMemoryNetwork) *LongRunningNode {
	comm := testutil.NewTestComm(nodeID, basicNetwork, testutil.AllowAllMessages)
	bb := NewNetworkBlockBuilder(t)
	epochConfig, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodeID, comm, bb)
	epochConfig.ReplicationEnabled = true
	epochConfig.MaxRoundWindow = longRunningMaxRoundWindow
	e, err := simplex.NewEpoch(epochConfig)
	require.NoError(t, err)

	logger := epochConfig.Logger.(*testutil.TestLogger)
	logger.SetPanicOnError(true)
	logger.SetPanicOnWarn(true)

	ti := &LongRunningNode{
		BasicNode: testutil.NewBasicNode(t, e, logger),
		bb:        bb,
		wal:       wal,
		Storage:   storage,
		logger:    logger,
	}

	return ti
}

func NewLongRunningNodeWithExtras(t *testing.T, nodeID simplex.NodeID, basicNetwork *testutil.BasicInMemoryNetwork, bb *LongRunningBlockBuilder, wal *testutil.TestWAL, storage *testutil.InMemStorage, logger *testutil.TestLogger) *LongRunningNode {
	comm := testutil.NewTestComm(nodeID, basicNetwork, testutil.AllowAllMessages)
	epochConfig, _, _ := testutil.DefaultTestNodeEpochConfig(t, nodeID, comm, bb)
	epochConfig.ReplicationEnabled = true
	epochConfig.MaxRoundWindow = longRunningMaxRoundWindow

	// set wals
	epochConfig.WAL = wal
	epochConfig.Storage = storage
	epochConfig.Logger = logger

	e, err := simplex.NewEpoch(epochConfig)
	require.NoError(t, err)

	ti := &LongRunningNode{
		BasicNode: testutil.NewBasicNode(t, e, logger),
		bb:        bb,
		wal:       wal,
		Storage:   storage,
		logger:    logger,
	}

	return ti
}

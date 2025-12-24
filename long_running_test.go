package simplex_test

import (
	"math"
	"testing"

	"github.com/ava-labs/simplex/testutil"
)

func TestLongRunningSimple(t *testing.T) {
	net := testutil.NewDefaultLongRunningNetwork(t, 5)

	net.StartInstances()
	net.WaitForNodesToEnterRound(40)
	net.StopAndAssert(false)
}

func TestLongRunningReplication(t *testing.T) {
	net := testutil.NewDefaultLongRunningNetwork(t, 10)
	for _, instance := range net.Instances {
		instance.SilenceExceptKeywords("Received replication response", "Resending replication requests for missing rounds/sequences", "1111")
	}
	net.StartInstances()

	net.WaitForNodesToEnterRound(40)
	net.NoMoreBlocks()
	net.DisconnectNodes(2)
	net.ContinueBlocks()
	net.WaitForNodesToEnterRound(70, 1, 3, 4, 5, 6)
	net.DisconnectNodes(4)
	net.WaitForNodesToEnterRound(90, 1, 3, 5, 6, 7, 8, 9)
	net.ConnectNodes(2, 4)
	net.WaitForNodesToEnterRound(150)
	net.StopAndAssert(false)
}

func TestLongRunningCrash(t *testing.T) {
	net := testutil.NewDefaultLongRunningNetwork(t, 10)
	for i, instance := range net.Instances {
		if i == 3 {
			instance.SilenceExceptKeywords("WAL")
			continue
		}
		instance.Silence()
	}

	net.StartInstances()
	net.WaitForNodesToEnterRound(30)
	net.CrashNodes(3)
	crashedNodeLatestBlock := net.Instances[3].Storage.NumBlocks()

	net.WaitForNodesToEnterRound(80, 1, 2, 4, 5, 6, 7, 8, 9)
	net.RestartNodes(3)

	waitForRound := math.Max(float64(crashedNodeLatestBlock*2), 150)
	net.WaitForNodesToEnterRound(uint64(waitForRound))
	net.StopAndAssert(false)
}

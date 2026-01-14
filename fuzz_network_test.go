package simplex_test

import (
	"testing"

	"github.com/ava-labs/simplex/testutil"
	"github.com/ava-labs/simplex/testutil/random_network"
)

func TestNetworkSimpleFuzz(t *testing.T) {
	l := testutil.MakeLogger(t)
	config := random_network.DefaultFuzzConfig()
	// Uncomment to enable file logging to tmp/ directory:
	// config.LogDirectory = "tmp/"
	network := random_network.NewNetwork(config, t, l)
	// network.SetInfoLog()
	network.StartInstances()
	// network.IssueTxs()

	// network.WaitForTxAcceptance()
	// network.StopInstances()
	network.Run()
	network.PrintStatus()
}

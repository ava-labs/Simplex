package simplex_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ava-labs/simplex/testutil"
	"github.com/ava-labs/simplex/testutil/random_network"
)

func TestNetworkSimpleFuzz(t *testing.T) {
	l := testutil.MakeLogger(t)
	config := random_network.DefaultFuzzConfig()
	network := random_network.NewNetwork(config, t, l)
	// network.SetInfoLog()
	network.StartInstances()
	network.IssueTxs()

	time.Sleep(1 * time.Second)

	fmt.Println("Stopping network...")
	network.StopInstances()
	fmt.Println("Network stopped.")
	time.Sleep(1 * time.Second)
	network.PrintStatus()
}

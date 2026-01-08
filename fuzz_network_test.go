package simplex_test

import (
	"testing"
	"time"

	"github.com/ava-labs/simplex/testutil"
	"github.com/ava-labs/simplex/testutil/random_network"
)

func TestNetworkSimpleFuzz(t *testing.T) {
	l := testutil.MakeLogger(t)
	config := random_network.DefaultFuzzConfig()
	network := random_network.NewNetwork(config, t, l)
	network.StartInstances()

	network.IssueTxs()

	time.Sleep(100 * time.Millisecond)
}

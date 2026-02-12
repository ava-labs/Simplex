package simplex_test

import (
	"testing"

	"github.com/ava-labs/simplex/testutil/random_network"
)

func TestNetworkSimpleFuzz(t *testing.T) {
	for range 100 {
		config := random_network.DefaultFuzzConfig()
		config.RandomSeed = 1770220909588
		network := random_network.NewNetwork(config, t)
		network.Run()
		network.PrintStatus()
	}
}

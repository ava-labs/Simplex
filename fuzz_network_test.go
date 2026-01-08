package simplex_test

import (
	"testing"

	"github.com/ava-labs/simplex/testutil"
	"github.com/ava-labs/simplex/testutil/random_network"
	"github.com/stretchr/testify/require"
)

func NetworkSimpleFuzz(t *testing.T) {
	require := require.New(t)

	l := testutil.MakeLogger(t)
	config := random_network.DefaultFuzzConfig()
	network := random_network.NewNetwork(config, t, l)
	require.NoError(network.Start())

}

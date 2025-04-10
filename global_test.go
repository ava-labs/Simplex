// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNodeIDs(t *testing.T) {
	nodeIDs := NodeIDs{
		{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
	}

	for i := range nodeIDs {
		require.NotContains(t, nodeIDs, nodeIDs.Remove(nodeIDs[i]))
		for j := range nodeIDs {
			if i == j {
				continue
			}
			require.Contains(t, nodeIDs, nodeIDs[j])
		}
	}
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOrbit(t *testing.T) {
	nodes := []uint16{0, 1, 2, 3}

	for _, testCase := range []struct {
		round               uint64
		expectedNodeToOrbit map[uint16]uint64
	}{
		{
			round: 0,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 0,
				1: 0,
				2: 0,
				3: 0,
			},
		},
		{
			round: 1,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 1,
				1: 0,
				2: 0,
				3: 0,
			},
		},
		{
			round: 2,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 1,
				1: 1,
				2: 0,
				3: 0,
			},
		},
		{
			round: 3,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 1,
				1: 1,
				2: 1,
				3: 0,
			},
		},
		{
			round: 4,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 1,
				1: 1,
				2: 1,
				3: 1,
			},
		},
		{
			round: 5,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 2,
				1: 1,
				2: 1,
				3: 1,
			},
		},
		{
			round: 6,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 2,
				1: 2,
				2: 1,
				3: 1,
			},
		},
		{
			round: 7,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 2,
				1: 2,
				2: 2,
				3: 1,
			},
		},
		{
			round: 8,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 2,
				1: 2,
				2: 2,
				3: 2,
			},
		},
	} {
		t.Run(fmt.Sprintf("%d", testCase.round), func(t *testing.T) {
			for node, expectedOrbit := range testCase.expectedNodeToOrbit {
				require.Equal(t, expectedOrbit, Orbit(testCase.round, node, uint16(len(nodes))), "unexpected orbitSuspected for node %d", node)
			}
		})
	}
}

func TestBlacklist(t *testing.T) {
	bl := Blacklist{
		NodeCount: 4,
	}
	require.False(t, bl.IsNodeSuspected(2))

	bl.NodeSuspected(1, 2)
	require.False(t, bl.IsNodeSuspected(2))

	bl.NodeSuspected(1, 2)
	require.True(t, bl.IsNodeSuspected(2))

	bl.advanceOrbit(2, 2)
	require.True(t, bl.IsNodeSuspected(2), "Suspected node is still suspected even if its orbitSuspected advanced")

	bl.NodeRedeemed(3, 2)
	require.True(t, bl.IsNodeSuspected(2))

	bl.NodeRedeemed(3, 2)
	require.False(t, bl.IsNodeSuspected(2))

}

func TestBlacklistBytes(t *testing.T) {
	bl := Blacklist{
		NodeCount: 5,
		suspectedNodes: []suspectedNode{
			{nodeIndex: 1, suspectingCount: 2, orbitSuspected: 3, redeemingCount: 4, orbitToRedeem: 5},
			{nodeIndex: 6, suspectingCount: 7, orbitSuspected: 8, redeemingCount: 9, orbitToRedeem: 10},
		},
		Updates: []BlacklistUpdate{
			{Type: BlacklistOpType_NodeSuspected, NodeIndex: 11},
			{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 12},
		},
	}
	bytes, err := bl.Bytes()
	require.NoError(t, err)

	var bl2 Blacklist
	err = bl2.FromBytes(bytes)
	require.NoError(t, err)

	require.Equal(t, bl, bl2)
}
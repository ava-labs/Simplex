// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package common

import (
	"testing"

	"github.com/stretchr/testify/require"
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

// TestNodesEqual checks Equal ignores order but compares
// length, Id, PK and Weight of every node.
func TestNodesEqual(t *testing.T) {
	a := Node{Id: NodeID{1}, Weight: 10, PK: PublicKeyBytes{0xa}}
	b := Node{Id: NodeID{2}, Weight: 20, PK: PublicKeyBytes{0xb}}
	c := Node{Id: NodeID{3}, Weight: 30, PK: PublicKeyBytes{0xc}}

	testCases := []struct {
		name  string
		nws   Nodes
		other Nodes
		equal bool
	}{
		{name: "both nil", equal: true},
		{name: "nil and empty", other: Nodes{}, equal: true},
		{name: "same order", nws: Nodes{a, b, c}, other: Nodes{a, b, c}, equal: true},
		{name: "different order", nws: Nodes{a, b, c}, other: Nodes{c, a, b}, equal: true},
		{name: "different length", nws: Nodes{a, b}, other: Nodes{a, b, c}, equal: false},
		{name: "empty and non empty", nws: Nodes{}, other: Nodes{a}, equal: false},
		{name: "different id", nws: Nodes{a, b}, other: Nodes{a, {Id: NodeID{9}, Weight: b.Weight, PK: b.PK}}, equal: false},
		{name: "different weight", nws: Nodes{a, b}, other: Nodes{a, {Id: b.Id, Weight: 99, PK: b.PK}}, equal: false},
		{name: "different pk", nws: Nodes{a, b}, other: Nodes{a, {Id: b.Id, Weight: b.Weight, PK: PublicKeyBytes{0xff}}}, equal: false},
		{name: "duplicate vs distinct", nws: Nodes{a, a}, other: Nodes{a, b}, equal: false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.equal, testCase.nws.Equal(testCase.other))
			require.Equal(t, testCase.equal, testCase.other.Equal(testCase.nws))
		})
	}
}

// TestNodesEqualDoesNotMutate checks Equal sorts clones and
// leaves the receiver and argument in their original order.
func TestNodesEqualDoesNotMutate(t *testing.T) {
	a := Node{Id: NodeID{1}, Weight: 10, PK: PublicKeyBytes{0xa}}
	b := Node{Id: NodeID{2}, Weight: 20, PK: PublicKeyBytes{0xb}}

	nws := Nodes{b, a}
	other := Nodes{a, b}
	require.True(t, nws.Equal(other))
	require.Equal(t, Nodes{b, a}, nws)
	require.Equal(t, Nodes{a, b}, other)
}

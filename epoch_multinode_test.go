// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	. "simplex"
	"simplex/testutil"
	"testing"
)

func TestSimplexMultiNodeSimple(t *testing.T) {
	bb := testutil.NewTestControlledBlockBuilder(t)

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	net := testutil.NewInMemNetwork(t, nodes)
	testutil.NewSimplexNode(t, nodes[0], net, bb, nil)
	testutil.NewSimplexNode(t, nodes[1], net, bb, nil)
	testutil.NewSimplexNode(t, nodes[2], net, bb, nil)
	testutil.NewSimplexNode(t, nodes[3], net, bb, nil)

	net.StartInstances()

	for seq := 0; seq < 10; seq++ {
		bb.TriggerNewBlock()
		for _, n := range net.Instances() {
			n.Storage.WaitForBlockCommit(uint64(seq))
		}
	}
}

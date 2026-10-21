// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package common

import (
	"crypto/rand"
	"fmt"
	"math"
	mathrand "math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlacklistVerifyProposedBlacklist(t *testing.T) {
	for _, testCase := range []struct {
		name              string
		blacklist         Blacklist
		proposedBlacklist Blacklist
		nodeCount         int
		round             uint64
		expectedErr       error
	}{
		{
			name:      "node count mismatch",
			nodeCount: 5,
			proposedBlacklist: Blacklist{
				nodeCount: 13,
			},
			expectedErr: errBlacklistInvalidNodeCount,
		},
		{
			name: "max blacklisted rounds increased by more than node count",
			blacklist: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 40,
			},
			nodeCount: 4,
			proposedBlacklist: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 45,
			},
			expectedErr: errBlacklistInvalidMaxBlacklistedRounds,
		},
		{
			name: "max blacklisted rounds decreased by more than node count",
			blacklist: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 40,
			},
			nodeCount: 4,
			proposedBlacklist: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 35,
			},
			expectedErr: errBlacklistInvalidMaxBlacklistedRounds,
		},
		{
			name: "max blacklisted rounds decreased to zero",
			blacklist: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 4,
			},
			nodeCount: 4,
			proposedBlacklist: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 0,
			},
			expectedErr: errBlacklistInvalidMaxBlacklistedRounds,
		},
		{
			name: "max blacklisted rounds overflows",
			blacklist: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: math.MaxUint16,
			},
			nodeCount: 4,
			proposedBlacklist: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 0,
			},
			expectedErr: errBlacklistInvalidMaxBlacklistedRounds,
		},
		{
			name: "invalid blacklist update",
			blacklist: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 4,
				suspectedNodes:       []suspectedNode{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 6, RedeemingCount: 0, OrbitToRedeem: 0}},
			},
			nodeCount: 4,
			round:     29,
			proposedBlacklist: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 4,
				suspectedNodes:       []suspectedNode{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 6, RedeemingCount: 0, OrbitToRedeem: 0}},
				updates:              []blacklistUpdate{{Type: blacklistOpType_NodeSuspected, NodeIndex: 3}},
			},
			expectedErr: errBlacklistInvalidUpdates,
		},
		{
			name: "node redeemed in the same orbit it was suspected in",
			blacklist: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 4,
				suspectedNodes:       []suspectedNode{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 7, RedeemingCount: 0, OrbitToRedeem: 0}},
			},
			nodeCount: 4,
			round:     29,
			proposedBlacklist: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 4,
				suspectedNodes:       []suspectedNode{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 7, RedeemingCount: 1, OrbitToRedeem: 7}},
				updates:              []blacklistUpdate{{Type: blacklistOpType_NodeRedeemed, NodeIndex: 3}},
			},
			expectedErr: errBlacklistInvalidBlacklist,
		},
		{
			name: "blacklist mismatch",
			blacklist: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 4,
				suspectedNodes:       []suspectedNode{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 6, RedeemingCount: 0, OrbitToRedeem: 0}},
			},
			nodeCount: 4,
			round:     29,
			proposedBlacklist: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 4,
				suspectedNodes:       []suspectedNode{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 6, RedeemingCount: 0, OrbitToRedeem: 7}},
				updates:              []blacklistUpdate{{Type: blacklistOpType_NodeRedeemed, NodeIndex: 3}},
			},
			expectedErr: errBlacklistInvalidBlacklist,
		},
		{
			name: "blacklist mismatch - too many blacklisted",
			blacklist: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 4,
				suspectedNodes:       []suspectedNode{{NodeIndex: 3, SuspectingCount: 2}, {NodeIndex: 2, SuspectingCount: 2}},
			},
			nodeCount: 4,
			round:     29,
			proposedBlacklist: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 4,
				suspectedNodes:       []suspectedNode{{NodeIndex: 3, SuspectingCount: 2}, {NodeIndex: 2, SuspectingCount: 2}, {NodeIndex: 1, SuspectingCount: 1}},
				updates:              []blacklistUpdate{{Type: blacklistOpType_NodeSuspected, NodeIndex: 1}},
			},
			expectedErr: errBlacklistInvalidBlacklist,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.blacklist.VerifyProposedBlacklist(testCase.proposedBlacklist, testCase.round)
			require.ErrorContains(t, err, testCase.expectedErr.Error())
		})
	}
}

func TestOrbit(t *testing.T) {
	nodes := []uint16{0, 1, 2, 3}

	for _, testCase := range []struct {
		round               uint64
		expectedNodeToOrbit map[uint16]uint64
	}{
		{
			round: 0,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 1,
				1: 0,
				2: 0,
				3: 0,
			},
		},
		{
			round: 1,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 1,
				1: 1,
				2: 0,
				3: 0,
			},
		},
		{
			round: 2,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 1,
				1: 1,
				2: 1,
				3: 0,
			},
		},
		{
			round: 3,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 1,
				1: 1,
				2: 1,
				3: 1,
			},
		},
		{
			round: 4,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 2,
				1: 1,
				2: 1,
				3: 1,
			},
		},
		{
			round: 5,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 2,
				1: 2,
				2: 1,
				3: 1,
			},
		},
		{
			round: 6,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 2,
				1: 2,
				2: 2,
				3: 1,
			},
		},
		{
			round: 7,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 2,
				1: 2,
				2: 2,
				3: 2,
			},
		},
		{
			round: 8,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 3,
				1: 2,
				2: 2,
				3: 2,
			},
		},
		{
			round: 9,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 3,
				1: 3,
				2: 2,
				3: 2,
			},
		},
		{
			round: 10,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 3,
				1: 3,
				2: 3,
				3: 2,
			},
		},
		{
			round: 11,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 3,
				1: 3,
				2: 3,
				3: 3,
			},
		},
	} {
		t.Run(fmt.Sprintf("%d", testCase.round), func(t *testing.T) {
			for node, expectedOrbit := range testCase.expectedNodeToOrbit {
				require.Equal(t, expectedOrbit, nodeOrbit(testCase.round, node, uint16(len(nodes))), "unexpected OrbitSuspected for node %d", node)
			}
		})
	}
}

func TestBlacklistFromBytes(t *testing.T) {
	populated := Blacklist{
		nodeCount:            4,
		maxBlacklistedRounds: 7,
		suspectedNodes:       []suspectedNode{{NodeIndex: 3, SuspectingCount: 1, OrbitSuspected: 1}},
		updates:              []blacklistUpdate{{NodeIndex: 3, Type: blacklistOpType_NodeSuspected}},
	}

	for _, testCase := range []struct {
		name              string
		data              []byte
		expectErr         bool
		expectedBlacklist Blacklist
	}{
		{
			name:              "empty blacklist",
			data:              (&Blacklist{nodeCount: 100}).Bytes(),
			expectedBlacklist: Blacklist{nodeCount: 100},
		},
		{
			name:              "populated blacklist",
			data:              populated.Bytes(),
			expectedBlacklist: populated,
		},
		{
			name:      "malformed data",
			data:      []byte{0, 1, 2},
			expectErr: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var bl Blacklist
			err := bl.FromBytes(testCase.data)
			if testCase.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, testCase.expectedBlacklist.Equals(&bl))
		})
	}
}

func TestBlacklistSimpleFlow(t *testing.T) {
	bl := Blacklist{
		nodeCount: 4,
	}
	require.False(t, bl.IsNodeSuspected(2))

	bl.setNodeSuspected(1, 2)
	require.False(t, bl.IsNodeSuspected(2))

	bl.setNodeSuspected(1, 2)
	require.True(t, bl.IsNodeSuspected(2))

	bl.suspectedNodes = bl.garbageCollectSuspectedNodes(5)
	require.True(t, bl.IsNodeSuspected(2), "Suspected node is still suspected even if its OrbitSuspected advanced")

	bl.setNodeRedeemed(3, 2)
	require.True(t, bl.IsNodeSuspected(2))

	bl.setNodeRedeemed(3, 2)
	require.False(t, bl.IsNodeSuspected(2))
}

// TestBlacklistCapAppliesToPendingSuspects asserts that at most f nodes can be blacklisted.
func TestBlacklistCapAppliesToPendingSuspects(t *testing.T) {
	const nodeCount = 4
	f := (nodeCount - 1) / 3
	require.Equal(t, 1, f)

	countBlacklisted := func(bl Blacklist) int {
		blacklisted := 0
		for node := uint16(0); node < nodeCount; node++ {
			if bl.IsNodeSuspected(node) {
				blacklisted++
			}
		}
		return blacklisted
	}

	t.Run("setNodeSuspected", func(t *testing.T) {
		bl := Blacklist{nodeCount: nodeCount}

		// One accusation each: both nodes are pending, neither is blacklisted.
		bl.setNodeSuspected(1, 2)
		bl.setNodeSuspected(1, 3)
		require.Len(t, bl.suspectedNodes, 2)
		require.Zero(t, countBlacklisted(bl))

		// A second accusation blacklists node 2 and fills the cap.
		bl.setNodeSuspected(1, 2)
		require.True(t, bl.IsNodeSuspected(2))

		// A second accusation of the pending node 3 must not blacklist it: f nodes are already blacklisted.
		bl.setNodeSuspected(1, 3)
		require.False(t, bl.IsNodeSuspected(3))
		require.LessOrEqual(t, countBlacklisted(bl), f, "%s", bl.String())
	})

	t.Run("applyUpdates", func(t *testing.T) {
		// Two consecutive rounds within one orbit for both accused nodes, as two successive leaders
		// would produce when accusing the same pair.
		const firstRound, secondRound = 2, 3
		for _, node := range []uint16{0, 1} {
			require.Equal(t, nodeOrbit(firstRound, node, nodeCount), nodeOrbit(secondRound, node, nodeCount))
		}
		accuse := []blacklistUpdate{
			{Type: blacklistOpType_NodeSuspected, NodeIndex: 0},
			{Type: blacklistOpType_NodeSuspected, NodeIndex: 1},
		}

		bl := Blacklist{nodeCount: nodeCount}
		bl = bl.applyUpdates(accuse, firstRound)
		require.Len(t, bl.suspectedNodes, 2)
		require.Zero(t, countBlacklisted(bl))

		bl = bl.applyUpdates(accuse, secondRound)
		require.LessOrEqual(t, countBlacklisted(bl), f, "%s", bl.String())
	})
}

func FuzzBlacklistFromBytes(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var blacklist Blacklist
		err := blacklist.FromBytes(data)
		if err == nil {
			data2 := blacklist.Bytes()
			require.Equal(t, data, data2)
			return
		}
		require.Error(t, err)
	})
}

func TestBlacklistBytes(t *testing.T) {
	bl := Blacklist{
		nodeCount: 5,
		suspectedNodes: []suspectedNode{
			{NodeIndex: 1, SuspectingCount: 2, OrbitSuspected: 3, RedeemingCount: 4, OrbitToRedeem: 5},
			{NodeIndex: 6, SuspectingCount: 7, OrbitSuspected: 8, RedeemingCount: 9, OrbitToRedeem: 10},
		},
		updates: []blacklistUpdate{
			{Type: blacklistOpType_NodeSuspected, NodeIndex: 11},
			{Type: blacklistOpType_NodeRedeemed, NodeIndex: 12},
		},
	}
	bytes := bl.Bytes()

	var bl2 Blacklist
	err := bl2.FromBytes(bytes)
	require.NoError(t, err)

	require.True(t, bl.Equals(&bl2))
}

func TestComputeBlacklistUpdates(t *testing.T) {
	for _, testCase := range []struct {
		name               string
		round              uint64
		suspectedNodes     []suspectedNode
		expectedUpdates    []blacklistUpdate
		timedOut, redeemed map[uint16]uint64
	}{
		{
			name:            "empty suspected nodes, a node has timed out",
			expectedUpdates: []blacklistUpdate{{Type: blacklistOpType_NodeSuspected, NodeIndex: 2}},
			timedOut:        map[uint16]uint64{2: 10},
			round:           13,
		},
		{
			name:            "empty suspected nodes, a node has timed out and later has been redeemed",
			expectedUpdates: make([]blacklistUpdate, 0, 4),
			timedOut:        map[uint16]uint64{2: 10},
			redeemed:        map[uint16]uint64{2: 11},
			round:           13,
		},
		{
			name:            "a node is suspected and has timed out again",
			suspectedNodes:  []suspectedNode{{NodeIndex: 2, SuspectingCount: 2}},
			expectedUpdates: make([]blacklistUpdate, 0, 4),
			timedOut:        map[uint16]uint64{2: 10},
			round:           13,
		},
		{
			name:            "a node is suspected and has been redeemed",
			suspectedNodes:  []suspectedNode{{NodeIndex: 2, SuspectingCount: 2}},
			expectedUpdates: []blacklistUpdate{{Type: blacklistOpType_NodeRedeemed, NodeIndex: 2}},
			timedOut:        map[uint16]uint64{2: 10},
			redeemed:        map[uint16]uint64{2: 11},
			round:           13,
		},
		{
			name:            "a node is not suspected and is now suspected",
			suspectedNodes:  []suspectedNode{{NodeIndex: 2, SuspectingCount: 1}},
			expectedUpdates: []blacklistUpdate{{Type: blacklistOpType_NodeSuspected, NodeIndex: 2}},
			timedOut:        map[uint16]uint64{2: 10},
			redeemed:        map[uint16]uint64{2: 9},
			round:           13,
		},
		{
			name:            "a node is not suspected and is suspected and then redeemed",
			expectedUpdates: make([]blacklistUpdate, 0, 4),
			suspectedNodes:  []suspectedNode{{NodeIndex: 2, SuspectingCount: 1}},
			timedOut:        map[uint16]uint64{2: 10},
			redeemed:        map[uint16]uint64{2: 11},
			round:           13,
		},
		{
			name:            "a node is not suspected and also hasn't timed out",
			suspectedNodes:  []suspectedNode{{NodeIndex: 2, SuspectingCount: 1}},
			expectedUpdates: make([]blacklistUpdate, 0, 4),
			timedOut:        map[uint16]uint64{2: 9},
		},
		{
			name:            "a node is suspected and hasn't been redeemed",
			suspectedNodes:  []suspectedNode{{NodeIndex: 2, SuspectingCount: 2}},
			expectedUpdates: make([]blacklistUpdate, 0, 4),
			timedOut:        map[uint16]uint64{2: 11},
			redeemed:        map[uint16]uint64{2: 10},
			round:           13,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			blacklist := Blacklist{
				nodeCount:      4,
				suspectedNodes: testCase.suspectedNodes,
			}

			update := blacklist.computeBlacklistUpdates(testCase.round, testCase.timedOut, testCase.redeemed)
			require.Equal(t, testCase.expectedUpdates, update)

		})
	}
}

func TestAdvanceRound(t *testing.T) {
	nodes := []uint16{0, 1, 2, 3}

	// NodeIDs 0, 2 are suspected.
	// NodeIDs 1 and 3 are not suspected.
	// Node 2 can be redeemed.
	suspectedNodesBefore := []suspectedNode{
		{NodeIndex: 0, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 1, OrbitToRedeem: 1},
		{NodeIndex: 1, SuspectingCount: 1, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
		{NodeIndex: 2, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 2, OrbitToRedeem: 1},
		{NodeIndex: 3, SuspectingCount: 1, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
	}

	testCases := []struct {
		suspectedNodesAfter []suspectedNode
		noChange            bool
	}{
		{
			// round 0
			noChange: true,
		},
		{
			// round 1
			noChange: true,
		},
		{
			// round 2
			noChange: true,
		},
		{
			// round 3
			noChange: true,
		},
		{
			// round 4
			suspectedNodesAfter: []suspectedNode{
				{NodeIndex: 0, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
				{NodeIndex: 1, SuspectingCount: 1, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
				{NodeIndex: 2, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 2, OrbitToRedeem: 1},
				{NodeIndex: 3, SuspectingCount: 1, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
			},
		},
		{
			// round 5
			suspectedNodesAfter: []suspectedNode{
				{NodeIndex: 0, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
				{NodeIndex: 2, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 2, OrbitToRedeem: 1},
				{NodeIndex: 3, SuspectingCount: 1, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
			},
		},
		{
			// round 6
			suspectedNodesAfter: []suspectedNode{
				{NodeIndex: 0, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
				{NodeIndex: 3, SuspectingCount: 1, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
			},
		},
		{
			// round 7
			suspectedNodesAfter: []suspectedNode{
				{NodeIndex: 0, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
			},
		},
		{
			// round 8
			suspectedNodesAfter: []suspectedNode{
				{NodeIndex: 0, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
			},
		},
		{
			// round 9
			suspectedNodesAfter: []suspectedNode{
				{NodeIndex: 0, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
			},
		},
	}

	for round := uint64(0); round < uint64(len(testCases)); round++ {
		t.Run(fmt.Sprintf("round %d", round), func(t *testing.T) {
			blacklist := Blacklist{
				nodeCount:            uint16(len(nodes)),
				maxBlacklistedRounds: 10 * uint16(len(nodes)),
				suspectedNodes:       suspectedNodesBefore,
			}

			suspectingNodes := blacklist.garbageCollectSuspectedNodes(round)

			if testCases[round].noChange {
				require.Equal(t, suspectedNodesBefore, suspectingNodes)
			} else {
				require.Equal(t, testCases[round].suspectedNodesAfter, suspectingNodes)
			}
		})
	}
}

func TestVerifyBlacklistUpdates(t *testing.T) {
	testBlacklist := Blacklist{
		nodeCount: 4,
	}

	for _, testCase := range []struct {
		name        string
		Blacklist   Blacklist
		updates     []blacklistUpdate
		expectedErr error
	}{
		{
			name: "too many updates",
			updates: []blacklistUpdate{
				{Type: blacklistOpType_NodeSuspected, NodeIndex: 1},
				{Type: blacklistOpType_NodeRedeemed, NodeIndex: 2},
				{Type: blacklistOpType_NodeRedeemed, NodeIndex: 3},
				{Type: blacklistOpType_NodeRedeemed, NodeIndex: 4},
				{Type: blacklistOpType_NodeRedeemed, NodeIndex: 5},
			},
			expectedErr: errBlacklistTooManyUpdates,
		},
		{
			name: "invalid type",
			updates: []blacklistUpdate{
				{Type: 3, NodeIndex: 1},
			},
			expectedErr: errBlacklistInvalidUpdateType,
			Blacklist:   testBlacklist,
		},
		{
			name: "invalid index",
			updates: []blacklistUpdate{
				{Type: blacklistOpType_NodeRedeemed, NodeIndex: 4},
			},
			expectedErr: errBlacklistInvalidNodeIndex,
			Blacklist:   testBlacklist,
		},
		{
			name: "double vote",
			updates: []blacklistUpdate{
				{Type: blacklistOpType_NodeSuspected, NodeIndex: 3},
				{Type: blacklistOpType_NodeSuspected, NodeIndex: 2},
				{Type: blacklistOpType_NodeSuspected, NodeIndex: 3},
			},
			expectedErr: errBlacklistNodeIndexAlreadyUpdated,
			Blacklist:   testBlacklist,
		},
		{
			name: "already blacklisted",
			Blacklist: Blacklist{
				nodeCount:      4,
				suspectedNodes: []suspectedNode{{NodeIndex: 3, SuspectingCount: 2}},
			},
			updates: []blacklistUpdate{
				{Type: blacklistOpType_NodeSuspected, NodeIndex: 3},
				{Type: blacklistOpType_NodeSuspected, NodeIndex: 2},
			},
			expectedErr: errBlacklistNodeAlreadyBlacklisted,
		},
		{
			name: "not blacklisted, cannot be redeemed",
			Blacklist: Blacklist{
				nodeCount:      4,
				suspectedNodes: []suspectedNode{{NodeIndex: 3, SuspectingCount: 1}},
			},
			updates: []blacklistUpdate{
				{Type: blacklistOpType_NodeRedeemed, NodeIndex: 3},
			},
			expectedErr: errBlacklistNodeNotBlacklisted,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.Blacklist.verifyBlacklistUpdates(testCase.updates)
			require.ErrorContains(t, err, testCase.expectedErr.Error())
		})
	}
}

func TestBlacklistMultipleAttemptsOverSeveralOrbits(t *testing.T) {
	// This test tests that even though a node is suspected continuously across several rounds,
	// as long as the rounds are too far from each other, the node will not be blacklisted.

	blacklist := Blacklist{
		nodeCount: 4,
	}

	blacklist = blacklist.applyUpdates([]blacklistUpdate{{Type: blacklistOpType_NodeSuspected, NodeIndex: 3}}, 4)
	require.False(t, blacklist.IsNodeSuspected(3))

	blacklist = blacklist.applyUpdates([]blacklistUpdate{{Type: blacklistOpType_NodeSuspected, NodeIndex: 3}}, 7)
	require.False(t, blacklist.IsNodeSuspected(3))

	blacklist = blacklist.applyUpdates([]blacklistUpdate{{Type: blacklistOpType_NodeSuspected, NodeIndex: 3}}, 8)
	require.True(t, blacklist.IsNodeSuspected(3))
}

func TestBlacklistSimulateNetwork(t *testing.T) {
	for nodes := 1; nodes <= 128; nodes *= 2 {
		for rounds := 10; rounds <= 1000; rounds *= 10 {
			t.Run(fmt.Sprintf("n=%d/r=%d", nodes, rounds), func(t *testing.T) {
				t.Parallel()
				testBlacklistSimulateNetwork(t, nodes, rounds)
			})
		}
	}
}

func testBlacklistSimulateNetwork(t *testing.T, nodeCount int, rounds int) {
	// This test simulates a network of 'nodeCount' nodes for 'rounds' rounds.
	// It ensures that blacklists that are created by a leader node,
	// can be verified by other nodes.

	var blacklist Blacklist
	blacklist.nodeCount = uint16(nodeCount)
	blacklist.maxBlacklistedRounds = uint16(nodeCount)

	crashedNodes := make(map[uint16]struct{})

	type (
		coinTossResult bool
	)

	const (
		tails coinTossResult = false
		heads coinTossResult = true
	)

	coinToss := func() coinTossResult {
		var b [1]byte
		_, err := rand.Read(b[:])
		require.NoError(t, err)
		return b[0]&1 == 0
	}

	leaderCrashedInRound := make(map[uint16]map[uint64]struct{})
	leaderRecoveredInRound := make(map[uint16]map[uint64]struct{})

	for node := uint16(0); node < uint16(nodeCount); node++ {
		leaderCrashedInRound[node] = make(map[uint64]struct{})
		leaderRecoveredInRound[node] = make(map[uint64]struct{})
	}

	for round := uint64(0); round < uint64(rounds); round++ {
		f := (nodeCount - 1) / 3
		// Try to crash up to `f` nodes.
		for len(crashedNodes) < f {
			if coinToss() == heads {
				nodeToCrash := uint16(mathrand.Intn(nodeCount))
				if _, crashed := crashedNodes[nodeToCrash]; !crashed {
					crashedNodes[nodeToCrash] = struct{}{}
					leaderCrashedInRound[nodeToCrash][round] = struct{}{}
				}
			}
		}

		// Try to recover each crashed node with probability of 1/2
		for node := range crashedNodes {
			if coinToss() == heads {
				delete(crashedNodes, node)
				leaderRecoveredInRound[node][round] = struct{}{}
				delete(crashedNodes, node)
				delete(leaderCrashedInRound[node], round)
			}
		}

		blacklist = simulateRound(t, blacklistRoundSimulationInput{
			round:            round,
			crashedInRound:   leaderCrashedInRound,
			recoveredInRound: leaderRecoveredInRound,
			crashedNodes:     crashedNodes,
			nodeCount:        nodeCount,
			prevBlacklist:    blacklist,
		})
	}
}

type blacklistRoundSimulationInput struct {
	round            uint64
	crashedInRound   map[uint16]map[uint64]struct{}
	recoveredInRound map[uint16]map[uint64]struct{}
	crashedNodes     map[uint16]struct{}
	nodeCount        int
	prevBlacklist    Blacklist
}

func simulateRound(t *testing.T, blrsi blacklistRoundSimulationInput) Blacklist {
	round := blrsi.round
	crashedInRound := blrsi.crashedInRound
	recoveredInRound := blrsi.recoveredInRound
	crashedNodes := blrsi.crashedNodes
	nodeCount := blrsi.nodeCount
	prevBlacklist := blrsi.prevBlacklist

	leader := uint16(round % uint64(nodeCount))
	if _, leaderCrashed := crashedNodes[leader]; leaderCrashed {
		// leader is crashed, nothing further that can be done.
		newBlacklist := Blacklist{
			nodeCount:            prevBlacklist.nodeCount,
			maxBlacklistedRounds: prevBlacklist.maxBlacklistedRounds,
			suspectedNodes:       make([]suspectedNode, len(prevBlacklist.suspectedNodes)),
			updates:              make([]blacklistUpdate, 0, len(prevBlacklist.updates)),
		}
		copy(newBlacklist.suspectedNodes, prevBlacklist.suspectedNodes)
		return newBlacklist
	}

	// find the last round in which the node has crashed or recovered
	timedOut := make(map[uint16]uint64)
	redeemed := make(map[uint16]uint64)

	for r := round; r > 0; r-- {
		for node := uint16(0); node < uint16(nodeCount); node++ {
			if _, crashed := crashedInRound[node][r]; crashed && len(timedOut) == 0 {
				timedOut[node] = r
			}
			if _, recovered := recoveredInRound[node][r]; recovered && len(redeemed) == 0 {
				redeemed[node] = r
			}
		}
	}

	updates := prevBlacklist.computeBlacklistUpdates(round, timedOut, redeemed)

	newBlacklist := prevBlacklist.applyUpdates(updates, round)

	err := prevBlacklist.VerifyProposedBlacklist(newBlacklist, round)
	require.NoError(t, err, "round %d", round)

	return newBlacklist
}

func TestBlacklistEquals(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		bl1, bl2 Blacklist
		equals   bool
	}{
		{
			name: "different node count",
			bl1: Blacklist{
				nodeCount: 4,
			},
			bl2: Blacklist{
				nodeCount: 5,
			},
		},
		{
			name: "different max blacklisted rounds",
			bl1: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 4,
			},
			bl2: Blacklist{
				nodeCount:            4,
				maxBlacklistedRounds: 2,
			},
		},
		{
			name: "different suspected nodes count",
			bl1: Blacklist{
				nodeCount:      4,
				suspectedNodes: []suspectedNode{{NodeIndex: 3, SuspectingCount: 2}},
			},
			bl2: Blacklist{
				nodeCount:      4,
				suspectedNodes: []suspectedNode{{NodeIndex: 3, SuspectingCount: 2}, {NodeIndex: 2, SuspectingCount: 1}},
			},
		},
		{
			name: "different suspected nodes content",
			bl1: Blacklist{
				nodeCount:      4,
				suspectedNodes: []suspectedNode{{NodeIndex: 3, SuspectingCount: 2}},
			},
			bl2: Blacklist{
				nodeCount:      4,
				suspectedNodes: []suspectedNode{{NodeIndex: 3, SuspectingCount: 1}},
			},
		},
		{
			name: "different updates count",
			bl1: Blacklist{
				nodeCount:      4,
				suspectedNodes: []suspectedNode{{NodeIndex: 3, SuspectingCount: 2}},
				updates:        []blacklistUpdate{{Type: blacklistOpType_NodeSuspected, NodeIndex: 2}},
			},
			bl2: Blacklist{
				nodeCount:      4,
				suspectedNodes: []suspectedNode{{NodeIndex: 3, SuspectingCount: 2}},
				updates:        []blacklistUpdate{{Type: blacklistOpType_NodeSuspected, NodeIndex: 2}, {Type: blacklistOpType_NodeRedeemed, NodeIndex: 3}},
			},
		},
		{
			name: "different updates content",
			bl1: Blacklist{
				nodeCount:      4,
				suspectedNodes: []suspectedNode{{NodeIndex: 3, SuspectingCount: 2}},
				updates:        []blacklistUpdate{{Type: blacklistOpType_NodeSuspected, NodeIndex: 2}},
			},
			bl2: Blacklist{
				nodeCount:      4,
				suspectedNodes: []suspectedNode{{NodeIndex: 3, SuspectingCount: 2}},
				updates:        []blacklistUpdate{{Type: blacklistOpType_NodeRedeemed, NodeIndex: 2}},
			},
		},
		{
			name: "equal blacklists",
			bl1: Blacklist{
				nodeCount:      4,
				suspectedNodes: []suspectedNode{{NodeIndex: 3, SuspectingCount: 2}},
				updates:        []blacklistUpdate{{Type: blacklistOpType_NodeSuspected, NodeIndex: 2}},
			},
			bl2: Blacklist{
				nodeCount:      4,
				suspectedNodes: []suspectedNode{{NodeIndex: 3, SuspectingCount: 2}},
				updates:        []blacklistUpdate{{Type: blacklistOpType_NodeSuspected, NodeIndex: 2}},
			},
			equals: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.equals, testCase.bl1.Equals(&testCase.bl2))
		})
	}
}

func TestNewBlacklistMaxBlacklistedRounds(t *testing.T) {
	bl := NewBlacklist(4, 4620)
	require.Equal(t, uint16(4), bl.nodeCount)
	require.Equal(t, uint16(4620), bl.maxBlacklistedRounds)

	bl = NewBlacklist(4, 0)
	require.Equal(t, uint16(1), bl.maxBlacklistedRounds, "zero rounds should be raised to one")
}

func TestBlacklistMaxBlacklistedRoundsIsPreserved(t *testing.T) {
	bl := NewBlacklist(4, 4620)

	applied := bl.applyUpdates([]blacklistUpdate{{Type: blacklistOpType_NodeSuspected, NodeIndex: 1}}, 5)
	require.Equal(t, bl.maxBlacklistedRounds, applied.maxBlacklistedRounds, "applyUpdates should carry over maxBlacklistedRounds")

	cloned := applied.Clone()
	require.Equal(t, bl.maxBlacklistedRounds, cloned.maxBlacklistedRounds, "Clone should carry over maxBlacklistedRounds")
	require.True(t, applied.Equals(&cloned))

	var decoded Blacklist
	require.NoError(t, decoded.FromBytes(applied.Bytes()))
	require.Equal(t, bl.maxBlacklistedRounds, decoded.maxBlacklistedRounds, "serialization should preserve maxBlacklistedRounds")
	require.True(t, applied.Equals(&decoded))
}

func TestBlacklistedNodeExpiresAfterMaxBlacklistedRounds(t *testing.T) {
	const (
		nodeCount            = 4
		maxBlacklistedRounds = 8 // 2 orbits
		blacklistedNode      = 1
		orbitSuspected       = 3
	)

	// With 4 nodes, node 1 is in orbit k during rounds 4(k-1)+1 ... 4k.
	// Node 1 is blacklisted in orbit 3, and is allowed to remain blacklisted for 8 more rounds (orbits 4 and 5).
	// It should be removed from the blacklist once orbit 6 is reached.
	blacklisted := suspectedNode{NodeIndex: blacklistedNode, SuspectingCount: 2, OrbitSuspected: orbitSuspected}

	for _, testCase := range []struct {
		name          string
		round         uint64
		suspectedNode suspectedNode
		expectExpired bool
	}{
		{
			name:          "same orbit it was suspected in",
			round:         12, // orbit 3
			suspectedNode: blacklisted,
		},
		{
			name:          "one orbit later",
			round:         16, // orbit 4
			suspectedNode: blacklisted,
		},
		{
			name:          "last orbit it is allowed to remain blacklisted in",
			round:         20, // orbit 5
			suspectedNode: blacklisted,
		},
		{
			name:          "first round of the orbit after the last allowed orbit",
			round:         21, // orbit 6
			suspectedNode: blacklisted,
			expectExpired: true,
		},
		{
			name:          "long after the last allowed orbit",
			round:         1000,
			suspectedNode: blacklisted,
			expectExpired: true,
		},
		{
			name:          "partial redeeming votes do not prevent expiry",
			round:         21, // orbit 6
			suspectedNode: suspectedNode{NodeIndex: blacklistedNode, SuspectingCount: 2, OrbitSuspected: orbitSuspected, RedeemingCount: 1, OrbitToRedeem: 5},
			expectExpired: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			bl := Blacklist{
				nodeCount:            nodeCount,
				maxBlacklistedRounds: maxBlacklistedRounds,
				suspectedNodes:       []suspectedNode{testCase.suspectedNode},
			}
			require.True(t, bl.IsNodeSuspected(blacklistedNode))

			next := bl.applyUpdates(nil, testCase.round)
			require.Equal(t, uint16(maxBlacklistedRounds), next.maxBlacklistedRounds)

			if testCase.expectExpired {
				require.False(t, next.IsNodeSuspected(blacklistedNode))
				require.Empty(t, next.suspectedNodes)
			} else {
				require.True(t, next.IsNodeSuspected(blacklistedNode))
				require.Len(t, next.suspectedNodes, 1)
			}

			// The blacklist of the previous round must not be modified.
			require.True(t, bl.IsNodeSuspected(blacklistedNode))
		})
	}
}

func TestBlacklistedNodeExpiryEndToEnd(t *testing.T) {
	const (
		nodeCount            = 4
		maxBlacklistedRounds = 8 // 2 orbits
		blacklistedNode      = 1
	)

	bl := NewBlacklist(nodeCount, maxBlacklistedRounds)

	suspect := []blacklistUpdate{{Type: blacklistOpType_NodeSuspected, NodeIndex: blacklistedNode}}

	// Round 2 and 3 are in orbit 1 for node 1. Two accusations blacklist it (f+1 = 2).
	bl = bl.applyUpdates(suspect, 2)
	require.False(t, bl.IsNodeSuspected(blacklistedNode))
	bl = bl.applyUpdates(suspect, 3)
	require.True(t, bl.IsNodeSuspected(blacklistedNode))

	// Round 4 is still orbit 1 for node 1, and orbits 2 and 3 span rounds 5-12: still blacklisted.
	for round := uint64(4); round <= 12; round++ {
		bl = bl.applyUpdates(nil, round)
		require.True(t, bl.IsNodeSuspected(blacklistedNode), "round %d", round)
	}

	// Orbit 4 (round 13): expired.
	bl = bl.applyUpdates(nil, 13)
	require.False(t, bl.IsNodeSuspected(blacklistedNode))
	require.Empty(t, bl.suspectedNodes)

	// A redeem update for an expired node is rejected, as the node is no longer blacklisted.
	redeem := []blacklistUpdate{{Type: blacklistOpType_NodeRedeemed, NodeIndex: blacklistedNode}}
	require.ErrorContains(t, bl.verifyBlacklistUpdates(redeem), errBlacklistNodeNotBlacklisted.Error())

	// An expired node can be suspected again from scratch.
	bl = bl.applyUpdates(suspect, 14)
	require.False(t, bl.IsNodeSuspected(blacklistedNode))
	require.Equal(t, []suspectedNode{{NodeIndex: blacklistedNode, SuspectingCount: 1, OrbitSuspected: 4}}, bl.suspectedNodes)
}

// TestBlacklistVerifyProposedBlacklistMaxBlacklistedRoundsStep checks that a child may move
// maxBlacklistedRounds by up to nodeCount rounds in either direction, as long as it stays non-zero.
func TestBlacklistVerifyProposedBlacklistMaxBlacklistedRoundsStep(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		prev     uint16
		proposed uint16
	}{
		{name: "unchanged", prev: 40, proposed: 40},
		{name: "increased by node count", prev: 40, proposed: 44},
		{name: "decreased by node count", prev: 40, proposed: 36},
		{name: "decreased to the minimum", prev: 5, proposed: 1},
		{name: "increased to the maximum", prev: math.MaxUint16 - 4, proposed: math.MaxUint16},
		{name: "increased from zero", prev: 0, proposed: 4},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			prev := Blacklist{nodeCount: 4, maxBlacklistedRounds: testCase.prev}
			proposed := prev.applyUpdates(nil, 5)
			proposed.maxBlacklistedRounds = testCase.proposed
			require.NoError(t, prev.VerifyProposedBlacklist(proposed, 5))
		})
	}
}

// TestBlacklistNextMaxBlacklistedRounds checks that Next moves maxBlacklistedRounds by up to nodeCount
// rounds towards the target, without overshooting it or dropping below one.
func TestBlacklistNextMaxBlacklistedRounds(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		current  uint16
		target   uint16
		expected uint16
	}{
		{name: "equal to target", current: 40, target: 40, expected: 40},
		{name: "below target", current: 40, target: 100, expected: 44},
		{name: "above target", current: 40, target: 10, expected: 36},
		{name: "does not overshoot target", current: 40, target: 42, expected: 42},
		{name: "never goes below one", current: 3, target: 0, expected: 1},
		{name: "never overflows", current: math.MaxUint16, target: math.MaxUint16, expected: math.MaxUint16},
		{name: "zero moves up to target", current: 0, target: 3, expected: 3},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			bl := Blacklist{nodeCount: 4, maxBlacklistedRounds: testCase.current}
			next := bl.Next(5, nil, nil, testCase.target)
			require.Equal(t, testCase.expected, next.maxBlacklistedRounds)
		})
	}
}

// TestBlacklistForEpoch checks that a parent's blacklist carries over within an epoch, and is otherwise
// replaced by one without suspected nodes that keeps maxBlacklistedRounds, or takes it from the config if missing.
func TestBlacklistForEpoch(t *testing.T) {
	parent := Blacklist{
		nodeCount:            4,
		maxBlacklistedRounds: 40,
		suspectedNodes:       []suspectedNode{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 1}},
		updates:              []blacklistUpdate{{Type: blacklistOpType_NodeSuspected, NodeIndex: 3}},
	}

	for _, testCase := range []struct {
		name        string
		parent      Blacklist
		parentEpoch uint64
		nodeCount   uint16
		expected    Blacklist
	}{
		{name: "same epoch", parent: parent, parentEpoch: 1, nodeCount: 4, expected: parent},
		{name: "earlier epoch", parent: parent, parentEpoch: 0, nodeCount: 8, expected: Blacklist{nodeCount: 8, maxBlacklistedRounds: 40}},
		{name: "no parent", parentEpoch: 1, nodeCount: 4, expected: Blacklist{nodeCount: 4, maxBlacklistedRounds: 12}},
		{name: "parent without max blacklisted rounds", parent: Blacklist{nodeCount: 4}, parentEpoch: 0, nodeCount: 4, expected: Blacklist{nodeCount: 4, maxBlacklistedRounds: 12}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			actual := testCase.parent.ForEpoch(testCase.parentEpoch, 1, testCase.nodeCount, 12)
			require.True(t, testCase.expected.Equals(&actual), "expected %s, got %s", testCase.expected.String(), actual.String())
		})
	}
}

func TestBlacklistMaxBlacklistedRoundsRollingUpgrade(t *testing.T) {
	// This test simulates a rolling upgrade where every node is reconfigured, one after another,
	// from oldTarget to newTarget rounds. Each leader moves the value towards its own target,
	// and every block must be accepted by all nodes regardless of their configuration.
	const (
		nodeCount = 7
		oldTarget = 5 * nodeCount
		newTarget = 20 * nodeCount
	)

	targets := make([]uint16, nodeCount)
	for i := range targets {
		targets[i] = oldTarget
	}

	bl := Blacklist{nodeCount: nodeCount, maxBlacklistedRounds: oldTarget}
	round := uint64(0)

	buildAndVerify := func() {
		leader := round % nodeCount
		next := bl.Next(round, nil, nil, targets[leader])
		require.NoError(t, bl.VerifyProposedBlacklist(next, round), "round %d", round)
		bl = next
		round++
	}

	// Upgrade the nodes one by one. While the upgraded nodes are a minority, the value can only
	// be pushed above the old target by the upgraded nodes that lead consecutively, and is then pulled back.
	// Once they are a majority, the value drifts towards the new target.
	for upgraded := 1; upgraded < nodeCount; upgraded++ {
		targets[upgraded-1] = newTarget
		for range 3 * nodeCount {
			buildAndVerify()
			require.GreaterOrEqual(t, bl.maxBlacklistedRounds, uint16(oldTarget), "round %d", round)
			require.LessOrEqual(t, bl.maxBlacklistedRounds, uint16(newTarget), "round %d", round)
			if upgraded <= nodeCount-upgraded {
				require.LessOrEqual(t, bl.maxBlacklistedRounds, uint16(oldTarget+upgraded*nodeCount), "round %d", round)
			}
		}
	}

	// Once all nodes are upgraded, the value moves by nodeCount rounds per block until it reaches the new target.
	targets[nodeCount-1] = newTarget
	for range (newTarget - oldTarget) / nodeCount {
		buildAndVerify()
	}
	require.Equal(t, uint16(newTarget), bl.maxBlacklistedRounds)

	// And it stays there.
	for range 2 * nodeCount {
		buildAndVerify()
		require.Equal(t, uint16(newTarget), bl.maxBlacklistedRounds)
	}
}

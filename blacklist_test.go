// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"crypto/rand"
	"fmt"
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
				NodeCount: 13,
			},
			expectedErr: errBlacklistInvalidNodeCount,
		},
		{
			name: "invalid blacklist update",
			blacklist: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 6, RedeemingCount: 0, OrbitToRedeem: 0}},
			},
			nodeCount: 4,
			round:     29,
			proposedBlacklist: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 6, RedeemingCount: 0, OrbitToRedeem: 0}},
				Updates:        []BlacklistUpdate{{Type: BlacklistOpType_NodeSuspected, NodeIndex: 3}},
			},
			expectedErr: errBlacklistInvalidUpdates,
		},
		{
			name: "node redeemed in the same orbit it was suspected in",
			blacklist: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 7, RedeemingCount: 0, OrbitToRedeem: 0}},
			},
			nodeCount: 4,
			round:     29,
			proposedBlacklist: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 7, RedeemingCount: 1, OrbitToRedeem: 7}},
				Updates:        []BlacklistUpdate{{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 3}},
			},
			expectedErr: errBlacklistInvalidBlacklist,
		},
		{
			name: "blacklist mismatch",
			blacklist: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 6, RedeemingCount: 0, OrbitToRedeem: 0}},
			},
			nodeCount: 4,
			round:     29,
			proposedBlacklist: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 6, RedeemingCount: 0, OrbitToRedeem: 7}},
				Updates:        []BlacklistUpdate{{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 3}},
			},
			expectedErr: errBlacklistInvalidBlacklist,
		},
		{
			name: "blacklist mismatch - too many blacklisted",
			blacklist: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2}, {NodeIndex: 2, SuspectingCount: 2}},
			},
			nodeCount: 4,
			round:     29,
			proposedBlacklist: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2}, {NodeIndex: 2, SuspectingCount: 2}, {NodeIndex: 1, SuspectingCount: 1}},
				Updates:        []BlacklistUpdate{{Type: BlacklistOpType_NodeSuspected, NodeIndex: 1}},
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
				require.Equal(t, expectedOrbit, Orbit(testCase.round, node, uint16(len(nodes))), "unexpected OrbitSuspected for node %d", node)
			}
		})
	}
}

func TestBlacklistFromBytes(t *testing.T) {
	for _, testCase := range []struct {
		name              string
		data              []byte
		expectedErr       string
		expectedBlacklist Blacklist
	}{
		{
			name: "empty blacklist",
			data: []byte{0, 0x64},
			expectedBlacklist: Blacklist{
				NodeCount:      100,
				SuspectedNodes: SuspectedNodes{},
				Updates:        BlacklistUpdates{},
			},
		},
		{
			name:        "too short data",
			data:        []byte{0, 1, 2},
			expectedErr: "buffer too short (3) to contain blacklist",
		},
		{
			name:        "invalid suspected nodes",
			data:        []byte{0, 4, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expectedErr: "failed to parse suspected nodes: failed to read suspected node at position 0: invalid bitmask: lower 4 bits must be zero, got 00000010",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var bl Blacklist
			err := bl.FromBytes(testCase.data)
			if testCase.expectedErr == "" {
				require.NoError(t, err)
				require.Equal(t, testCase.expectedBlacklist, bl)
				return
			}
			require.EqualError(t, err, testCase.expectedErr)
		})
	}
}

func TestBlacklistSimpleFlow(t *testing.T) {
	bl := Blacklist{
		NodeCount: 4,
	}
	require.False(t, bl.IsNodeSuspected(2))

	bl.setNodeSuspected(1, 2)
	require.False(t, bl.IsNodeSuspected(2))

	bl.setNodeSuspected(1, 2)
	require.True(t, bl.IsNodeSuspected(2))

	bl.SuspectedNodes = bl.garbageCollectSuspectedNodes(5)
	require.True(t, bl.IsNodeSuspected(2), "Suspected node is still suspected even if its OrbitSuspected advanced")

	bl.setNodeRedeemed(3, 2)
	require.True(t, bl.IsNodeSuspected(2))

	bl.setNodeRedeemed(3, 2)
	require.False(t, bl.IsNodeSuspected(2))
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

func FuzzSuspectedNodes(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var sn SuspectedNodes
		err := sn.FromBytes(data)
		if err == nil {
			require.Equal(t, data, sn.Bytes())
			return
		}
		require.Error(t, err)
	})
}

func FuzzBlacklistUpdates(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var updates BlacklistUpdates
		err := updates.FromBytes(data)
		if err == nil {
			require.Equal(t, data, updates.Bytes())
			require.Len(t, updates.Bytes(), len(updates)*3)
			return
		}
		require.Error(t, err)
	})
}

func TestSuspectedNodesTrailer(t *testing.T) {
	sns := SuspectedNodes{
		{NodeIndex: 1, SuspectingCount: 2, OrbitSuspected: 3, RedeemingCount: 4, OrbitToRedeem: 5},
		{NodeIndex: 6, SuspectingCount: 7, OrbitSuspected: 8, RedeemingCount: 9, OrbitToRedeem: 10},
	}
	bytes := sns.Bytes()

	suspectedNode := SuspectedNode{
		NodeIndex:       uint16(mathrand.Intn(1000)),
		SuspectingCount: uint16(mathrand.Intn(10)),
		OrbitSuspected:  uint64(mathrand.Intn(100)),
		RedeemingCount:  uint16(mathrand.Intn(10)),
		OrbitToRedeem:   uint64(mathrand.Intn(100)),
	}

	trailer := make([]byte, suspectedNode.Len())

	for {
		_, err := rand.Read(trailer)
		require.NoError(t, err)

		// Is this a valid encoding of a SuspectedNode?
		var sn SuspectedNode
		_, err = sn.Read(trailer)
		if err != nil {
			break
		}
	}

	data := append(bytes, trailer...)

	var sns2 SuspectedNodes
	err := sns2.FromBytes(data)
	require.Error(t, err)
}

func TestBlacklistBytes(t *testing.T) {
	bl := Blacklist{
		NodeCount: 5,
		SuspectedNodes: []SuspectedNode{
			{NodeIndex: 1, SuspectingCount: 2, OrbitSuspected: 3, RedeemingCount: 4, OrbitToRedeem: 5},
			{NodeIndex: 6, SuspectingCount: 7, OrbitSuspected: 8, RedeemingCount: 9, OrbitToRedeem: 10},
		},
		Updates: []BlacklistUpdate{
			{Type: BlacklistOpType_NodeSuspected, NodeIndex: 11},
			{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 12},
		},
	}
	bytes := bl.Bytes()

	var sns SuspectedNodes
	er := sns.FromBytes(bl.SuspectedNodes.Bytes())
	require.NoError(t, er)

	var bl2 Blacklist
	err := bl2.FromBytes(bytes)
	require.NoError(t, err)

	require.Equal(t, bl, bl2)
}

func TestComputeBlacklistUpdates(t *testing.T) {
	for _, testCase := range []struct {
		name               string
		round              uint64
		suspectedNodes     []SuspectedNode
		expectedUpdates    []BlacklistUpdate
		timedOut, redeemed map[uint16]uint64
	}{
		{
			name:            "empty suspected nodes, a node has timed out",
			expectedUpdates: []BlacklistUpdate{{Type: BlacklistOpType_NodeSuspected, NodeIndex: 2}},
			timedOut:        map[uint16]uint64{2: 10},
			round:           13,
		},
		{
			name:            "empty suspected nodes, a node has timed out and later has been redeemed",
			expectedUpdates: make([]BlacklistUpdate, 0, 4),
			timedOut:        map[uint16]uint64{2: 10},
			redeemed:        map[uint16]uint64{2: 11},
			round:           13,
		},
		{
			name:            "a node is suspected and has timed out again",
			suspectedNodes:  []SuspectedNode{{NodeIndex: 2, SuspectingCount: 2}},
			expectedUpdates: make([]BlacklistUpdate, 0, 4),
			timedOut:        map[uint16]uint64{2: 10},
			round:           13,
		},
		{
			name:            "a node is suspected and has been redeemed",
			suspectedNodes:  []SuspectedNode{{NodeIndex: 2, SuspectingCount: 2}},
			expectedUpdates: []BlacklistUpdate{{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 2}},
			timedOut:        map[uint16]uint64{2: 10},
			redeemed:        map[uint16]uint64{2: 11},
			round:           13,
		},
		{
			name:            "a node is not suspected and is now suspected",
			suspectedNodes:  []SuspectedNode{{NodeIndex: 2, SuspectingCount: 1}},
			expectedUpdates: []BlacklistUpdate{{Type: BlacklistOpType_NodeSuspected, NodeIndex: 2}},
			timedOut:        map[uint16]uint64{2: 10},
			redeemed:        map[uint16]uint64{2: 9},
			round:           13,
		},
		{
			name:            "a node is not suspected and is suspected and then redeemed",
			expectedUpdates: make([]BlacklistUpdate, 0, 4),
			suspectedNodes:  []SuspectedNode{{NodeIndex: 2, SuspectingCount: 1}},
			timedOut:        map[uint16]uint64{2: 10},
			redeemed:        map[uint16]uint64{2: 11},
			round:           13,
		},
		{
			name:            "a node is not suspected and also hasn't timed out",
			suspectedNodes:  []SuspectedNode{{NodeIndex: 2, SuspectingCount: 1}},
			expectedUpdates: make([]BlacklistUpdate, 0, 4),
			timedOut:        map[uint16]uint64{2: 9},
		},
		{
			name:            "a node is suspected and hasn't been redeemed",
			suspectedNodes:  []SuspectedNode{{NodeIndex: 2, SuspectingCount: 2}},
			expectedUpdates: make([]BlacklistUpdate, 0, 4),
			timedOut:        map[uint16]uint64{2: 11},
			redeemed:        map[uint16]uint64{2: 10},
			round:           13,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			blacklist := Blacklist{
				NodeCount:      4,
				SuspectedNodes: testCase.suspectedNodes,
			}

			update := blacklist.ComputeBlacklistUpdates(testCase.round, 4, testCase.timedOut, testCase.redeemed)
			require.Equal(t, testCase.expectedUpdates, update)

		})
	}
}

func TestAdvanceRound(t *testing.T) {
	nodes := []uint16{0, 1, 2, 3}

	// Nodes 0, 2 are suspected.
	// Nodes 1 and 3 are not suspected.
	// Node 2 can be redeemed.
	suspectedNodesBefore := SuspectedNodes{
		{NodeIndex: 0, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 1, OrbitToRedeem: 1},
		{NodeIndex: 1, SuspectingCount: 1, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
		{NodeIndex: 2, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 2, OrbitToRedeem: 1},
		{NodeIndex: 3, SuspectingCount: 1, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
	}

	testCases := []struct {
		suspectedNodesAfter SuspectedNodes
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
			suspectedNodesAfter: SuspectedNodes{
				{NodeIndex: 0, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
				{NodeIndex: 1, SuspectingCount: 1, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
				{NodeIndex: 2, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 2, OrbitToRedeem: 1},
				{NodeIndex: 3, SuspectingCount: 1, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
			},
		},
		{
			// round 5
			suspectedNodesAfter: SuspectedNodes{
				{NodeIndex: 0, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
				{NodeIndex: 2, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 2, OrbitToRedeem: 1},
				{NodeIndex: 3, SuspectingCount: 1, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
			},
		},
		{
			// round 6
			suspectedNodesAfter: SuspectedNodes{
				{NodeIndex: 0, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
				{NodeIndex: 3, SuspectingCount: 1, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
			},
		},
		{
			// round 7
			suspectedNodesAfter: SuspectedNodes{
				{NodeIndex: 0, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
			},
		},
		{
			// round 8
			suspectedNodesAfter: SuspectedNodes{
				{NodeIndex: 0, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
			},
		},
		{
			// round 9
			suspectedNodesAfter: SuspectedNodes{
				{NodeIndex: 0, SuspectingCount: 2, OrbitSuspected: 1, RedeemingCount: 0, OrbitToRedeem: 0},
			},
		},
	}

	for round := uint64(0); round < uint64(len(testCases)); round++ {
		t.Run(fmt.Sprintf("round %d", round), func(t *testing.T) {
			blacklist := Blacklist{
				NodeCount:      uint16(len(nodes)),
				SuspectedNodes: suspectedNodesBefore,
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

func TestUpdateBytesEqualsLen(t *testing.T) {
	var updates BlacklistUpdates
	update := BlacklistUpdate{}
	update1 := BlacklistUpdate{
		Type:      BlacklistOpType_NodeRedeemed,
		NodeIndex: 1,
	}
	update2 := BlacklistUpdate{
		Type:      BlacklistOpType_NodeSuspected,
		NodeIndex: 2,
	}
	update3 := BlacklistUpdate{
		NodeIndex: 3,
	}

	updates = append(updates, update)
	updates = append(updates, update1)
	updates = append(updates, update2)
	updates = append(updates, update3)

	require.Equal(t, len(updates.Bytes()), updates.Len())
}

func TestVerifyBlacklistUpdates(t *testing.T) {
	testBlacklist := Blacklist{
		NodeCount: 4,
	}

	for _, testCase := range []struct {
		name        string
		Blacklist   Blacklist
		updates     []BlacklistUpdate
		expectedErr error
	}{
		{
			name: "too many updates",
			updates: []BlacklistUpdate{
				{Type: BlacklistOpType_NodeSuspected, NodeIndex: 1},
				{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 2},
				{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 3},
				{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 4},
				{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 5},
			},
			expectedErr: errBlacklistTooManyUpdates,
		},
		{
			name: "invalid type",
			updates: []BlacklistUpdate{
				{Type: 3, NodeIndex: 1},
			},
			expectedErr: errBlacklistInvalidUpdateType,
			Blacklist:   testBlacklist,
		},
		{
			name: "invalid index",
			updates: []BlacklistUpdate{
				{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 4},
			},
			expectedErr: errBlacklistInvalidNodeIndex,
			Blacklist:   testBlacklist,
		},
		{
			name: "double vote",
			updates: []BlacklistUpdate{
				{Type: BlacklistOpType_NodeSuspected, NodeIndex: 3},
				{Type: BlacklistOpType_NodeSuspected, NodeIndex: 2},
				{Type: BlacklistOpType_NodeSuspected, NodeIndex: 3},
			},
			expectedErr: errBlacklistNodeIndexAlreadyUpdated,
			Blacklist:   testBlacklist,
		},
		{
			name: "already blacklisted",
			Blacklist: Blacklist{
				NodeCount:      4,
				SuspectedNodes: []SuspectedNode{{NodeIndex: 3, SuspectingCount: 2}},
			},
			updates: []BlacklistUpdate{
				{Type: BlacklistOpType_NodeSuspected, NodeIndex: 3},
				{Type: BlacklistOpType_NodeSuspected, NodeIndex: 2},
			},
			expectedErr: errBlacklistNodeAlreadyBlacklisted,
		},
		{
			name: "not blacklisted, cannot be redeemed",
			Blacklist: Blacklist{
				NodeCount:      4,
				SuspectedNodes: []SuspectedNode{{NodeIndex: 3, SuspectingCount: 1}},
			},
			updates: []BlacklistUpdate{
				{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 3},
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
		NodeCount: 4,
	}

	blacklist = blacklist.ApplyUpdates([]BlacklistUpdate{{Type: BlacklistOpType_NodeSuspected, NodeIndex: 3}}, 4)
	require.False(t, blacklist.IsNodeSuspected(3))

	blacklist = blacklist.ApplyUpdates([]BlacklistUpdate{{Type: BlacklistOpType_NodeSuspected, NodeIndex: 3}}, 7)
	require.False(t, blacklist.IsNodeSuspected(3))

	blacklist = blacklist.ApplyUpdates([]BlacklistUpdate{{Type: BlacklistOpType_NodeSuspected, NodeIndex: 3}}, 8)
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
	blacklist.NodeCount = uint16(nodeCount)

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
			NodeCount:      prevBlacklist.NodeCount,
			SuspectedNodes: make([]SuspectedNode, len(prevBlacklist.SuspectedNodes)),
			Updates:        make([]BlacklistUpdate, 0, len(prevBlacklist.Updates)),
		}
		copy(newBlacklist.SuspectedNodes, prevBlacklist.SuspectedNodes)
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

	updates := prevBlacklist.ComputeBlacklistUpdates(round, uint16(nodeCount), timedOut, redeemed)

	newBlacklist := prevBlacklist.ApplyUpdates(updates, round)

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
				NodeCount: 4,
			},
			bl2: Blacklist{
				NodeCount: 5,
			},
		},
		{
			name: "different suspected nodes count",
			bl1: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2}},
			},
			bl2: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2}, {NodeIndex: 2, SuspectingCount: 1}},
			},
		},
		{
			name: "different suspected nodes content",
			bl1: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2}},
			},
			bl2: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 1}},
			},
		},
		{
			name: "different updates count",
			bl1: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2}},
				Updates:        []BlacklistUpdate{{Type: BlacklistOpType_NodeSuspected, NodeIndex: 2}},
			},
			bl2: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2}},
				Updates:        []BlacklistUpdate{{Type: BlacklistOpType_NodeSuspected, NodeIndex: 2}, {Type: BlacklistOpType_NodeRedeemed, NodeIndex: 3}},
			},
		},
		{
			name: "different updates content",
			bl1: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2}},
				Updates:        []BlacklistUpdate{{Type: BlacklistOpType_NodeSuspected, NodeIndex: 2}},
			},
			bl2: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2}},
				Updates:        []BlacklistUpdate{{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 2}},
			},
		},
		{
			name: "equal blacklists",
			bl1: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2}},
				Updates:        []BlacklistUpdate{{Type: BlacklistOpType_NodeSuspected, NodeIndex: 2}},
			},
			bl2: Blacklist{
				NodeCount:      4,
				SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 2}},
				Updates:        []BlacklistUpdate{{Type: BlacklistOpType_NodeSuspected, NodeIndex: 2}},
			},
			equals: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.equals, testCase.bl1.Equals(&testCase.bl2))
		})
	}
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"github.com/stretchr/testify/require"
	"math"
	"strings"
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
		{
			round: 9,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 3,
				1: 2,
				2: 2,
				3: 2,
			},
		},
		{
			round: 10,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 3,
				1: 3,
				2: 2,
				3: 2,
			},
		},
		{
			round: 11,
			expectedNodeToOrbit: map[uint16]uint64{
				0: 3,
				1: 3,
				2: 3,
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

func TestBlacklistSimpleFlow(t *testing.T) {
	bl := Blacklist{
		NodeCount: 4,
	}
	require.False(t, bl.IsNodeSuspected(2))

	bl.nodeSuspected(1, 2)
	require.False(t, bl.IsNodeSuspected(2))

	bl.nodeSuspected(1, 2)
	require.True(t, bl.IsNodeSuspected(2))

	bl.advanceOrbit(2, 2)
	require.True(t, bl.IsNodeSuspected(2), "Suspected node is still suspected even if its orbitSuspected advanced")

	bl.nodeRedeemed(3, 2)
	require.True(t, bl.IsNodeSuspected(2))

	bl.nodeRedeemed(3, 2)
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

func TestComputeBlacklistUpdates(t *testing.T) {
	for _, testCase := range []struct {
		name string
		suspectedNodes []suspectedNode
		expectedUpdates []BlacklistUpdate
		timedOut, redeemed map[uint16]uint64
	}{
		{
			name: "empty suspected nodes, a node has timed out",
			expectedUpdates:  []BlacklistUpdate{{Type: BlacklistOpType_NodeSuspected, NodeIndex: 2}},
			timedOut:         map[uint16]uint64{2: 10},
		},
		{
			name: "empty suspected nodes, a node has timed out and later has been redeemed",
			expectedUpdates: make([]BlacklistUpdate, 0, 4),
			timedOut:         map[uint16]uint64{2: 10},
			redeemed:         map[uint16]uint64{2: 11},
		},
		{
			name: "a node is suspected and has timed out again",
			suspectedNodes: []suspectedNode{{nodeIndex: 2, suspectingCount: 2}},
			expectedUpdates: make([]BlacklistUpdate, 0, 4),
			timedOut:         map[uint16]uint64{2: 10},
		},
		{
			name: "a node is suspected and has been redeemed",
			suspectedNodes: []suspectedNode{{nodeIndex: 2, suspectingCount: 2}},
			expectedUpdates: []BlacklistUpdate{{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 2}},
			timedOut:         map[uint16]uint64{2: 10},
			redeemed:         map[uint16]uint64{2: 11},
		},
		{
			name: "a node is not suspected and is now suspected",
			suspectedNodes: []suspectedNode{{nodeIndex: 2, suspectingCount: 1}},
			expectedUpdates: []BlacklistUpdate{{Type: BlacklistOpType_NodeSuspected, NodeIndex: 2}},
			timedOut:         map[uint16]uint64{2: 10},
			redeemed:         map[uint16]uint64{2: 9},
		},
		{
			name: "a node is not suspected and is suspected and then redeemed",
			expectedUpdates: make([]BlacklistUpdate, 0, 4),
			suspectedNodes: []suspectedNode{{nodeIndex: 2, suspectingCount: 1}},
			timedOut:         map[uint16]uint64{2: 10},
			redeemed:         map[uint16]uint64{2: 11},
		},
		{
			name: "a node is not suspected and also hasn't timed out",
			suspectedNodes: []suspectedNode{{nodeIndex: 2, suspectingCount: 1}},
			expectedUpdates: make([]BlacklistUpdate, 0, 4),
		},
		{
			name: "a node is suspected and hasn't been redeemed",
			suspectedNodes: []suspectedNode{{nodeIndex: 2, suspectingCount: 2}},
			expectedUpdates: make([]BlacklistUpdate, 0, 4),
			timedOut:         map[uint16]uint64{2: 11},
			redeemed:         map[uint16]uint64{2: 10},
		},
	}{
		t.Run(testCase.name, func(t *testing.T) {
			blacklist := Blacklist{
				NodeCount: 4,
				suspectedNodes: testCase.suspectedNodes,
			}

			update := blacklist.ComputeBlacklistUpdates(4, testCase.timedOut, testCase.redeemed)
			require.Equal(t, testCase.expectedUpdates, update)

		})
	}
}

func TestVerifyBlacklistUpdates(t *testing.T) {
	for _, testCase := range []struct {
		name string
		Blacklist Blacklist
		updates []BlacklistUpdate
		expectedErr string
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
			expectedErr: "too many blacklist updates: 5, only 4 nodes exist",
		},
		{
			name: "invalid type",
			updates: []BlacklistUpdate{
				{Type: 3, NodeIndex: 1},
			},
			expectedErr: "invalid blacklist update type: 3",
		},
		{
			name: "invalid index",
			updates: []BlacklistUpdate{
				{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 4},
			},
			expectedErr: "invalid node index in blacklist update: 4, needs to be in [0, 3]",
		},
		{
			name: "double vote",
			updates: []BlacklistUpdate{
				{Type: BlacklistOpType_NodeSuspected, NodeIndex: 3},
				{Type: BlacklistOpType_NodeSuspected, NodeIndex: 2},
				{Type: BlacklistOpType_NodeSuspected, NodeIndex: 3},
			},
			expectedErr: "node index 3 was already updated",
		},
		{
			name: "already blacklisted",
			Blacklist: Blacklist{
				NodeCount: 4,
				suspectedNodes: []suspectedNode{{nodeIndex: 3, suspectingCount: 2}},
			},
			updates: []BlacklistUpdate{
				{Type: BlacklistOpType_NodeSuspected, NodeIndex: 3},
				{Type: BlacklistOpType_NodeSuspected, NodeIndex: 2},
			},
			expectedErr: "node index 3 is already blacklisted",
		},
		{
			name: "not blacklisted, cannot be redeemed",
			Blacklist: Blacklist{
				NodeCount: 4,
				suspectedNodes: []suspectedNode{{nodeIndex: 3, suspectingCount: 1}},
			},
			updates: []BlacklistUpdate{
				{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 3},
			},
			expectedErr: "node index 3 is not blacklisted, cannot be redeemed",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.Blacklist.verifyBlacklistUpdates(testCase.updates, 4)
			require.EqualError(t, err, testCase.expectedErr)
		})
	}
}

func TestBlacklistEvolution(t *testing.T) {
	s := `MB8EGAAEAAMAAgABAAAAAAAAAAEAAAAAAAAAAQQDAgAD
MB8EGAAEAAMAAgABAAAAAAAAAAEAAAAAAAAAAQQDAgAD`

	var blacklist1 Blacklist
	var blacklist2 Blacklist

	bl1, bl2 := strings.Split(s, "\n")[0], strings.Split(s, "\n")[1]
	bytes1, err := base64.StdEncoding.DecodeString(bl1)
	require.NoError(t, err)
	bytes2, err := base64.StdEncoding.DecodeString(bl2)
	require.NoError(t, err)

	err = blacklist1.FromBytes(bytes1)
	require.NoError(t, err)

	err = blacklist2.FromBytes(bytes2)
	require.NoError(t, err)

	blacklist1.Update(blacklist2.Updates, 7)
	require.Equal(t, blacklist1, blacklist2)

	//         	            	block contains an invalid blacklist, expected Blacklist(nodeCount=4, suspectedNodes=[], updates=[{type=2, nodeIndex=3}]), got Blacklist(nodeCount=4, suspectedNodes=[{nodeIndex=3, suspecting=2, redeeming=1, orbitSus=1, orbitRedeem=1}], updates=[{type=2, nodeIndex=3}])
}

func TestBlacklistSimulateNetwork(t *testing.T) {
	// This test simulates a network of 4 nodes for 10000 rounds.
	// It ensures that blacklists that are created by a leader node,
	// can be verified by other nodes.

	var blacklist Blacklist
	blacklist.NodeCount = 4

	var crashed bool
	nodeToCrash := 3

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

	leaderCrashedInRound := make(map[uint64]struct{})
	leaderRecoveredInRound := make(map[uint64]struct{})

	for round := uint64(0); round < 1000; round++ {
		coinTossRes := coinToss()
		// Crash the node with probability of 1/2
		if coinTossRes == heads && ! crashed {
			crashed = true
			fmt.Println("Node", nodeToCrash, "has crashed at round", round)
		}

		// Recover the node with probability of 1/2
		if coinTossRes == tails && crashed {
			crashed = false
			fmt.Println("Node", nodeToCrash, "has recovered at round", round)
			leaderRecoveredInRound[round] = struct{}{}
		}

		if leader := round%4; crashed && leader == uint64(nodeToCrash) {
			leaderCrashedInRound[round] = struct{}{}
		}

		newBlacklist  := simulateRound(round, leaderCrashedInRound, leaderRecoveredInRound, uint64(nodeToCrash), 4, blacklist.Clone())

		fmt.Println("Verifying round", round)

		err := verifyProposedBlacklist(blacklist.Clone(), newBlacklist.Clone(), 4, round)
		if err != nil {
			prevBlacklist := newBlacklist.Clone()
			prevBlacklistBytes, err := prevBlacklist.Bytes()
			require.NoError(t, err)
			fmt.Println("Prev:", base64.StdEncoding.EncodeToString(prevBlacklistBytes))

			newBlacklistBytes, err := newBlacklist.Bytes()
			require.NoError(t, err)

			fmt.Println("New:", base64.StdEncoding.EncodeToString(newBlacklistBytes))

		}
		require.NoError(t, err, "round %d", round)

		blacklist = newBlacklist
	}

}

const (
	noNodeCrashed = math.MaxUint16
)

func simulateRound(round uint64, crashedInRound map[uint64]struct{}, recoveredInRound map[uint64]struct{}, nodeToCrash uint64, nodeCount int, prevBlacklist Blacklist) Blacklist {
	leader := round % uint64(nodeCount)
	if _, crashed := crashedInRound[round]; crashed && leader == nodeToCrash {
		// leader is crashed, nothing further that can be done.
		newBlacklist := prevBlacklist.Clone()
		newBlacklist.Updates = nil
		return newBlacklist
	}

	// find the last round in which the node has crashed or recovered
	timedOut := make(map[uint16]uint64)
	redeemed := make(map[uint16]uint64)

	for r := round; r > 0; r-- {
		if _, crashed := crashedInRound[r]; crashed && len(timedOut) == 0 {
			timedOut[uint16(nodeToCrash)] = r
		}
		if _, recovered := recoveredInRound[r]; recovered && len(redeemed) == 0 {
			redeemed[uint16(nodeToCrash)] = r
		}
	}

	updates := prevBlacklist.ComputeBlacklistUpdates(uint16(nodeCount), timedOut, redeemed)

	prev := prevBlacklist.String()

	prevBlacklist.Update(updates, round)

	fmt.Println("Round:", round, "Prev blacklist:", prev, "Updates:", updates, "Next:", prevBlacklist.String())


	return prevBlacklist
}
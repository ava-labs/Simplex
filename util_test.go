// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"

	"github.com/stretchr/testify/require"
)

func TestRetrieveFromStorage(t *testing.T) {
	brokenStorage := testutil.NewInMemStorage()
	block := testutil.NewTestBlock(ProtocolMetadata{Seq: 43}, emptyBlacklist)
	finalization := Finalization{
		Finalization: ToBeSignedFinalization{
			BlockHeader: block.BlockHeader(),
		},
	}
	brokenStorage.Index(context.Background(), block, finalization)

	block = testutil.NewTestBlock(ProtocolMetadata{Seq: 0}, emptyBlacklist)
	finalization = Finalization{
		Finalization: ToBeSignedFinalization{
			BlockHeader: block.BlockHeader(),
		},
	}
	normalStorage := testutil.NewInMemStorage()
	err := normalStorage.Index(context.Background(), block, finalization)
	require.NoError(t, err)

	for _, testCase := range []struct {
		description           string
		storage               Storage
		expectedErr           error
		expectedVerifiedBlock *VerifiedFinalizedBlock
	}{
		{
			description: "no blocks in storage",
			storage:     testutil.NewInMemStorage(),
		},
		{
			description: "broken storage",
			storage:     brokenStorage,
			expectedErr: ErrBlockNotFound,
		},
		{
			description: "normal storage",
			storage:     normalStorage,
			expectedVerifiedBlock: &VerifiedFinalizedBlock{
				VerifiedBlock: block,
				Finalization:  finalization,
			},
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			lastBlock, err := RetrieveLastIndexFromStorage(testCase.storage)
			require.ErrorIs(t, err, testCase.expectedErr)

			require.Equal(t, testCase.expectedVerifiedBlock, lastBlock)
		})
	}
}

type unverifiableQC struct{}

func (u *unverifiableQC) Verify() error {
	return fmt.Errorf("invalid QC")
}

func TestVerifyQC(t *testing.T) {
	l := testutil.MakeLogger(t, 0)
	nodes := []NodeID{{1}, {2}, {3}, {4}, {5}}
	eligibleSigners := make(map[string]struct{})
	for _, n := range nodes {
		eligibleSigners[string(n)] = struct{}{}
	}
	quorumSize := Quorum(len(nodes))
	signatureAggregator := &testutil.TestSignatureAggregator{}
	// Test
	tests := []struct {
		name         string
		finalization Finalization
		quorumSize   int
		expectedErr  error
		msgInvalid   bool
	}{
		{
			name: "valid finalization",
			finalization: func() Finalization {
				block := testutil.NewTestBlock(ProtocolMetadata{}, emptyBlacklist)
				finalization, _ := testutil.NewFinalizationRecord(t, l, signatureAggregator, block, nodes[:quorumSize])
				return finalization
			}(),
			quorumSize: quorumSize,
		}, {
			name: "not enough signers",
			finalization: func() Finalization {
				block := testutil.NewTestBlock(ProtocolMetadata{}, emptyBlacklist)
				finalization, _ := testutil.NewFinalizationRecord(t, l, signatureAggregator, block, nodes[:quorumSize-1])
				return finalization
			}(),
			quorumSize:  quorumSize,
			expectedErr: fmt.Errorf("finalization certificate signed by insufficient (3 < 4) nodes"),
		},
		{
			name: "signer signed twice",
			finalization: func() Finalization {
				block := testutil.NewTestBlock(ProtocolMetadata{}, emptyBlacklist)
				doubleNodes := []NodeID{{1}, {2}, {3}, {4}, {4}}
				finalization, _ := testutil.NewFinalizationRecord(t, l, signatureAggregator, block, doubleNodes)
				return finalization
			}(),
			quorumSize:  quorumSize,
			expectedErr: fmt.Errorf("finalization is signed by the same node (0400000000000000) more than once"),
		},
		{
			name:         "quorum certificate not in finalization",
			finalization: Finalization{Finalization: ToBeSignedFinalization{}},
			quorumSize:   quorumSize,
			expectedErr:  fmt.Errorf("nil QuorumCertificate"),
		},
		{
			name: "nodes are not eligible signers",
			finalization: func() Finalization {
				block := testutil.NewTestBlock(ProtocolMetadata{}, emptyBlacklist)
				signers := []NodeID{{1}, {2}, {3}, {4}, {6}}
				finalization, _ := testutil.NewFinalizationRecord(t, l, signatureAggregator, block, signers)
				return finalization
			}(), quorumSize: quorumSize,
			expectedErr: fmt.Errorf("finalization quorum certificate contains an unknown signer (0600000000000000)"),
		},
		{
			name: "invalid QC",
			finalization: func() Finalization {
				block := testutil.NewTestBlock(ProtocolMetadata{}, emptyBlacklist)
				finalization, _ := testutil.NewFinalizationRecord(t, l, signatureAggregator, block, nodes[:quorumSize])
				return finalization
			}(),
			quorumSize:  quorumSize,
			msgInvalid:  true,
			expectedErr: fmt.Errorf("invalid QC"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.msgInvalid {
				err := VerifyQC(tt.finalization.QC, l, "Finalization", tt.quorumSize, eligibleSigners, &unverifiableQC{}, nil)
				require.EqualError(t, err, tt.expectedErr.Error())
			} else {
				err := VerifyQC(tt.finalization.QC, l, "Finalization", tt.quorumSize, eligibleSigners, &tt.finalization, nil)
				if tt.expectedErr != nil {
					require.EqualError(t, err, tt.expectedErr.Error())
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestGetHighestQuorumRound(t *testing.T) {
	// Test
	nodes := []NodeID{{1}, {2}, {3}, {4}, {5}}
	l := testutil.MakeLogger(t, 0)
	signatureAggregator := &testutil.TestSignatureAggregator{}

	// seq 1
	block1 := testutil.NewTestBlock(ProtocolMetadata{
		Seq:   1,
		Round: 1,
	}, emptyBlacklist)
	notarization1, err := testutil.NewNotarization(l, signatureAggregator, block1, nodes)
	require.NoError(t, err)
	finalization1, _ := testutil.NewFinalizationRecord(t, l, signatureAggregator, block1, nodes)

	// seq 10
	block10 := testutil.NewTestBlock(ProtocolMetadata{Seq: 10, Round: 10}, emptyBlacklist)
	notarization10, err := testutil.NewNotarization(l, signatureAggregator, block10, nodes)
	require.NoError(t, err)
	finalization10, _ := testutil.NewFinalizationRecord(t, l, signatureAggregator, block10, nodes)

	tests := []struct {
		name       string
		round      *Round
		eNote      *EmptyNotarization
		lastBlock  *VerifiedFinalizedBlock
		expectedQr *VerifiedQuorumRound
	}{
		{
			name:  "only empty notarization",
			eNote: testutil.NewEmptyNotarization(nodes, 1),
			expectedQr: &VerifiedQuorumRound{
				EmptyNotarization: testutil.NewEmptyNotarization(nodes, 1),
			},
		},
		{
			name: "only last block",
			lastBlock: &VerifiedFinalizedBlock{
				VerifiedBlock: block1,
				Finalization:  finalization1,
			},
			expectedQr: &VerifiedQuorumRound{
				VerifiedBlock: block1,
				Finalization:  &finalization1,
			},
		},
		{
			name:  "round",
			round: SetRound(block1, nil, &finalization1),
			expectedQr: &VerifiedQuorumRound{
				VerifiedBlock: block1,
				Finalization:  &finalization1,
			},
		},
		{
			name:  "round with notarization",
			round: SetRound(block1, &notarization1, nil),
			expectedQr: &VerifiedQuorumRound{
				VerifiedBlock: block1,
				Notarization:  &notarization1,
			},
		},
		{
			name:  "higher notarized round than indexed",
			round: SetRound(block10, &notarization10, nil),
			lastBlock: &VerifiedFinalizedBlock{
				VerifiedBlock: block1,
				Finalization:  finalization1,
			},
			expectedQr: &VerifiedQuorumRound{
				VerifiedBlock: block10,
				Notarization:  &notarization10,
			},
		},
		{
			name:  "higher indexed than in round",
			round: SetRound(block1, &notarization1, nil),
			lastBlock: &VerifiedFinalizedBlock{
				VerifiedBlock: block10,
				Finalization:  finalization10,
			},
			expectedQr: &VerifiedQuorumRound{
				VerifiedBlock: block10,
				Finalization:  &finalization10,
			},
		},
		{
			name:  "higher empty notarization",
			eNote: testutil.NewEmptyNotarization(nodes, 100),
			lastBlock: &VerifiedFinalizedBlock{
				VerifiedBlock: block1,
				Finalization:  finalization1,
			},
			round: SetRound(block10, &notarization10, nil),
			expectedQr: &VerifiedQuorumRound{
				EmptyNotarization: testutil.NewEmptyNotarization(nodes, 100),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qr := GetLatestVerifiedQuorumRound(tt.round, tt.eNote, tt.lastBlock)
			require.Equal(t, tt.expectedQr, qr)
		})
	}
}

func TestCompressSequences(t *testing.T) {
	tests := []struct {
		name     string
		input    []uint64
		expected []Segment
	}{
		{
			name:     "empty input",
			input:    []uint64{},
			expected: nil,
		},
		{
			name:  "single element",
			input: []uint64{5},
			expected: []Segment{
				{Start: 5, End: 5},
			},
		},
		{
			name:  "all consecutive",
			input: []uint64{1, 2, 3, 4, 5},
			expected: []Segment{
				{Start: 1, End: 5},
			},
		},
		{
			name:  "no consecutive elements",
			input: []uint64{2, 4, 6, 8},
			expected: []Segment{
				{Start: 2, End: 2},
				{Start: 4, End: 4},
				{Start: 6, End: 6},
				{Start: 8, End: 8},
			},
		},
		{
			name:  "mixed consecutive and non-consecutive",
			input: []uint64{3, 4, 5, 7, 8, 10},
			expected: []Segment{
				{Start: 3, End: 5},
				{Start: 7, End: 8},
				{Start: 10, End: 10},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompressSequences(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestDistributeSequenceRequests(t *testing.T) {
	tests := []struct {
		name     string
		start    uint64
		end      uint64
		numNodes int
		expected []Segment
	}{
		{
			name:     "even distribution",
			start:    0,
			end:      9,
			numNodes: 2,
			expected: []Segment{
				{Start: 0, End: 4},
				{Start: 5, End: 9},
			},
		},
		{
			name:     "uneven distribution",
			start:    0,
			end:      10,
			numNodes: 3,
			expected: []Segment{
				{Start: 0, End: 3},
				{Start: 4, End: 7},
				{Start: 8, End: 10},
			},
		},
		{
			name:     "single node full range",
			start:    5,
			end:      15,
			numNodes: 1,
			expected: []Segment{
				{Start: 5, End: 15},
			},
		},
		{
			name:     "numNodes greater than sequences",
			start:    0,
			end:      2,
			numNodes: 5,
			expected: []Segment{
				{Start: 0, End: 1},
				{Start: 2, End: 2},
			},
		},
		{
			name:     "zero-length range",
			start:    5,
			end:      5,
			numNodes: 3,
			expected: []Segment{
				{Start: 5, End: 5},
			},
		},
		{
			name:     "start > end",
			start:    10,
			end:      5,
			numNodes: 2,
			expected: nil,
		},
		{
			name:     "zero nodes",
			start:    0,
			end:      10,
			numNodes: 0,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DistributeSequenceRequests(tt.start, tt.end, tt.numNodes)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNotarizationTime(t *testing.T) {
	defaultFinalizeVoteRebroadcastTimeout := time.Second * 6

	var round uint64
	var have bool
	var checkedIfWeHaveNotFinalizedRoud int
	haveNotFinalizedRound := func() (uint64, bool) {
		checkedIfWeHaveNotFinalizedRoud++
		return round, have
	}

	var invoked int
	rebroadcastFinalizationVotes := func() {
		invoked++
	}
	nt := NewNotarizationTime(
		defaultFinalizeVoteRebroadcastTimeout,
		haveNotFinalizedRound,
		rebroadcastFinalizationVotes,
		func() uint64 {
			return round
		})

	// First call should set the time and the round.
	have = true
	round = 100
	now := time.Now()
	nt.CheckForNotFinalizedNotarizedBlocks(now)
	require.Zero(t, checkedIfWeHaveNotFinalizedRoud)

	// Next call happens just before we would check if we have not finalized.

	now = now.Add(defaultFinalizeVoteRebroadcastTimeout / 3).Add(-time.Millisecond)
	nt.CheckForNotFinalizedNotarizedBlocks(now)
	require.Equal(t, 0, invoked)
	require.Zero(t, checkedIfWeHaveNotFinalizedRoud)

	// Next call happens just after we would check if we have not finalized.

	now = now.Add(time.Millisecond)
	nt.CheckForNotFinalizedNotarizedBlocks(now)
	require.Equal(t, 0, invoked)
	require.Equal(t, 1, checkedIfWeHaveNotFinalizedRoud)

	// Advance the time some more. We still haven't reached defaultFinalizeVoteRebroadcastTimeout so no rebroadcast just yet.

	now = now.Add(defaultFinalizeVoteRebroadcastTimeout / 3)
	nt.CheckForNotFinalizedNotarizedBlocks(now)
	require.Equal(t, 0, invoked)
	require.Equal(t, 2, checkedIfWeHaveNotFinalizedRoud)

	// We need to wait a full defaultFinalizeVoteRebroadcastTimeout before we rebroadcast.
	// This is because we are unaware when was our last rebroadcast time.

	now = now.Add(defaultFinalizeVoteRebroadcastTimeout)
	nt.CheckForNotFinalizedNotarizedBlocks(now)
	require.Equal(t, 1, invoked)
	require.Equal(t, 3, checkedIfWeHaveNotFinalizedRoud)

	// Next call happens shortly after, no rebroadcast should happen.
	now = now.Add(defaultFinalizeVoteRebroadcastTimeout / 2)
	nt.CheckForNotFinalizedNotarizedBlocks(now)
	require.Equal(t, 1, invoked)
	require.Equal(t, 4, checkedIfWeHaveNotFinalizedRoud)

	// Next rebroadcast happens after exactly the timeout.
	now = now.Add(defaultFinalizeVoteRebroadcastTimeout / 2).Add(time.Millisecond)
	nt.CheckForNotFinalizedNotarizedBlocks(now)
	require.Equal(t, 2, invoked)
	require.Equal(t, 5, checkedIfWeHaveNotFinalizedRoud)

	// We now change the round, even though enough time has passed, no rebroadcast should happen.
	// Since we have advanced the round, we don't check if we have not finalized.
	round = 101
	now = now.Add(2 * defaultFinalizeVoteRebroadcastTimeout)
	nt.CheckForNotFinalizedNotarizedBlocks(now)
	require.Equal(t, 2, invoked)
	require.Equal(t, 5, checkedIfWeHaveNotFinalizedRoud)

	// We now finalized everything, so no rebroadcast should happen.
	have = false
	now = now.Add(defaultFinalizeVoteRebroadcastTimeout)
	nt.CheckForNotFinalizedNotarizedBlocks(now)
	require.Equal(t, 2, invoked)
}

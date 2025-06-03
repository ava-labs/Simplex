// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"errors"
	"simplex"
	. "simplex"
	"simplex/testutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetrieveFromStorage(t *testing.T) {
	brokenStorage := newInMemStorage()
	brokenStorage.data[41] = struct {
		VerifiedBlock
		Finalization
	}{VerifiedBlock: newTestBlock(ProtocolMetadata{Seq: 41})}

	block := newTestBlock(ProtocolMetadata{Seq: 0})
	finalization := Finalization{
		Finalization: ToBeSignedFinalization{
			BlockHeader: block.BlockHeader(),
		},
	}
	normalStorage := newInMemStorage()
	normalStorage.data[0] = struct {
		VerifiedBlock
		Finalization
	}{VerifiedBlock: block, Finalization: finalization}

	for _, testCase := range []struct {
		description           string
		storage               Storage
		expectedErr           error
		expectedVerifiedBlock *VerifiedFinalizedBlock
	}{
		{
			description: "no blocks in storage",
			storage:     newInMemStorage(),
		},
		{
			description: "broken storage",
			storage:     brokenStorage,
			expectedErr: errors.New("failed retrieving last block from storage with seq 0"),
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
			require.Equal(t, testCase.expectedErr, err)

			require.Equal(t, testCase.expectedVerifiedBlock, lastBlock)
		})
	}
}

func TestFinalizationValidation(t *testing.T) {
	l := testutil.MakeLogger(t, 0)
	nodes := []NodeID{{1}, {2}, {3}, {4}, {5}}
	eligibleSigners := make(map[string]struct{})
	for _, n := range nodes {
		eligibleSigners[string(n)] = struct{}{}
	}
	quorumSize := Quorum(len(nodes))
	signatureAggregator := &testSignatureAggregator{}
	// Test
	tests := []struct {
		name         string
		finalization Finalization
		quorumSize   int
		valid        bool
	}{
		{
			name: "valid finalization",
			finalization: func() Finalization {
				block := newTestBlock(ProtocolMetadata{})
				finalization, _ := newFinalizationRecord(t, l, signatureAggregator, block, nodes[:quorumSize])
				return finalization
			}(),
			quorumSize: quorumSize,
			valid:      true,
		}, {
			name: "not enough signers",
			finalization: func() Finalization {
				block := newTestBlock(ProtocolMetadata{})
				finalization, _ := newFinalizationRecord(t, l, signatureAggregator, block, nodes[:quorumSize-1])
				return finalization
			}(),
			quorumSize: quorumSize,
			valid:      false,
		},
		{
			name: "signer signed twice",
			finalization: func() Finalization {
				block := newTestBlock(ProtocolMetadata{})
				doubleNodes := []NodeID{{1}, {2}, {3}, {4}, {4}}
				finalization, _ := newFinalizationRecord(t, l, signatureAggregator, block, doubleNodes)
				return finalization
			}(),
			quorumSize: quorumSize,
			valid:      false,
		},
		{
			name:         "quorum certificate not in finalization",
			finalization: Finalization{Finalization: ToBeSignedFinalization{}},
			quorumSize:   quorumSize,
			valid:        false,
		},
		{
			name: "nodes are not eligible signers",
			finalization: func() Finalization {
				block := newTestBlock(ProtocolMetadata{})
				signers := []NodeID{{1}, {2}, {3}, {4}, {6}}
				finalization, _ := newFinalizationRecord(t, l, signatureAggregator, block, signers)
				return finalization
			}(), quorumSize: quorumSize,
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := simplex.IsFinalizationValid(eligibleSigners, &tt.finalization, tt.quorumSize, l)
			require.Equal(t, tt.valid, valid)
		})
	}
}

func TestGetHighestQuorumRound(t *testing.T) {
	// Test
	nodes := []NodeID{{1}, {2}, {3}, {4}, {5}}
	l := testutil.MakeLogger(t, 0)
	signatureAggregator := &testSignatureAggregator{}

	// seq 1
	block1 := newTestBlock(ProtocolMetadata{
		Seq:   1,
		Round: 1,
	})
	notarization1, err := newNotarization(l, signatureAggregator, block1, nodes)
	require.NoError(t, err)
	finalization1, _ := newFinalizationRecord(t, l, signatureAggregator, block1, nodes)

	// seq 10
	block10 := newTestBlock(ProtocolMetadata{Seq: 10, Round: 10})
	notarization10, err := newNotarization(l, signatureAggregator, block10, nodes)
	require.NoError(t, err)
	finalization10, _ := newFinalizationRecord(t, l, signatureAggregator, block10, nodes)

	tests := []struct {
		name       string
		round      *Round
		eNote      *EmptyNotarization
		lastBlock  *VerifiedFinalizedBlock
		expectedQr *VerifiedQuorumRound
	}{
		{
			name:  "only mpty notarization",
			eNote: newEmptyNotarization(nodes, 1, 1),
			expectedQr: &VerifiedQuorumRound{
				EmptyNotarization: newEmptyNotarization(nodes, 1, 1),
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
			eNote: newEmptyNotarization(nodes, 100, 100),
			lastBlock: &VerifiedFinalizedBlock{
				VerifiedBlock: block1,
				Finalization:  finalization1,
			},
			round: SetRound(block10, &notarization10, nil),
			expectedQr: &VerifiedQuorumRound{
				EmptyNotarization: newEmptyNotarization(nodes, 100, 100),
			},
		},
		{
			name:  "higher empty notarization with same sequence",
			eNote: newEmptyNotarization(nodes, 11, 10),
			lastBlock: &VerifiedFinalizedBlock{
				VerifiedBlock: block10,
				Finalization:  finalization10,
			},
			expectedQr: &VerifiedQuorumRound{
				EmptyNotarization: newEmptyNotarization(nodes, 11, 10),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qr := simplex.GetLatestVerifiedQuorumRound(tt.round, tt.eNote, tt.lastBlock)
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

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ava-labs/simplex"
	. "github.com/ava-labs/simplex/testutil"

	"github.com/stretchr/testify/require"
)

var errorSigAggregation = errors.New("signature error")

func TestNewNotarization(t *testing.T) {
	l := MakeLogger(t, 1)
	testBlock := &TestBlock{}
	tests := []struct {
		name                 string
		votesForCurrentRound map[string]*simplex.Vote
		block                simplex.VerifiedBlock
		expectError          error
		signatureAggregator  simplex.SignatureAggregator
		expectedSigners      []simplex.NodeID
	}{
		{
			name: "valid notarization",
			votesForCurrentRound: func() map[string]*simplex.Vote {
				votes := make(map[string]*simplex.Vote)
				nodeIds := [][]byte{{1}, {2}, {3}, {4}, {5}}
				for _, nodeId := range nodeIds {
					vote, err := NewTestVote(testBlock, nodeId)
					require.NoError(t, err)
					votes[string(nodeId)] = vote
				}
				return votes
			}(),
			block:               testBlock,
			signatureAggregator: &TestSignatureAggregator{},
			expectError:         nil,
			expectedSigners: []simplex.NodeID{
				{1}, {2}, {3}, {4}, {5},
			},
		},
		{
			name: "diverging votes",
			votesForCurrentRound: func() map[string]*simplex.Vote {
				votes := make(map[string]*simplex.Vote)
				nodeIds := [][]byte{{1}, {2}, {3}, {4}, {5}}
				for _, nodeId := range nodeIds {
					vote, err := NewTestVote(testBlock, nodeId)
					require.NoError(t, err)
					votes[string(nodeId)] = vote
				}
				votes[string(simplex.NodeID{2})].Vote.Digest[0]++ // Diverging vote
				return votes
			}(),
			block:               testBlock,
			signatureAggregator: &TestSignatureAggregator{},
			expectError:         nil,
			expectedSigners: []simplex.NodeID{
				{1}, {3}, {4}, {5},
			},
		},
		{
			name:                 "no votes",
			votesForCurrentRound: map[string]*simplex.Vote{},
			block:                testBlock,
			signatureAggregator:  &TestSignatureAggregator{},
			expectError:          simplex.ErrorNoVotes,
		},
		{
			name: "error aggregating",
			votesForCurrentRound: func() map[string]*simplex.Vote {
				votes := make(map[string]*simplex.Vote)
				nodeIds := [][]byte{{1}, {2}, {3}, {4}, {5}}
				for _, nodeId := range nodeIds {
					vote, err := NewTestVote(testBlock, nodeId)
					require.NoError(t, err)
					votes[string(nodeId)] = vote
				}
				return votes
			}(),
			block:               testBlock,
			signatureAggregator: &TestSignatureAggregator{Err: errorSigAggregation},
			expectError:         errorSigAggregation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notarization, err := simplex.NewNotarization(l, tt.signatureAggregator, tt.votesForCurrentRound, tt.block.BlockHeader())
			require.ErrorIs(t, err, tt.expectError, "expected error, got nil")

			if tt.expectError == nil {
				signers := notarization.QC.Signers()
				require.Equal(t, tt.expectedSigners, signers)

				for i, signer := range signers[1:] {
					require.Negative(t, bytes.Compare(signers[i], signer), "signers not in order")
				}
			}
		})
	}

}

func TestNewFinalization(t *testing.T) {
	l := MakeLogger(t, 1)
	tests := []struct {
		name                 string
		finalizeVotes        []*simplex.FinalizeVote
		signatureAggregator  simplex.SignatureAggregator
		expectedFinalization *simplex.ToBeSignedFinalization
		expectedQC           *simplex.QuorumCertificate
		expectError          error
	}{
		{
			name: "valid finalizations in order",
			finalizeVotes: []*simplex.FinalizeVote{
				NewTestFinalizeVote(t, &TestBlock{}, []byte{1}),
				NewTestFinalizeVote(t, &TestBlock{}, []byte{2}),
				NewTestFinalizeVote(t, &TestBlock{}, []byte{3}),
			},
			signatureAggregator:  &TestSignatureAggregator{},
			expectedFinalization: &NewTestFinalizeVote(t, &TestBlock{}, []byte{1}).Finalization,
			expectError:          nil,
		},
		{
			name: "unsorted finalizations",
			finalizeVotes: []*simplex.FinalizeVote{
				NewTestFinalizeVote(t, &TestBlock{}, []byte{3}),
				NewTestFinalizeVote(t, &TestBlock{}, []byte{1}),
				NewTestFinalizeVote(t, &TestBlock{}, []byte{2}),
			},
			signatureAggregator:  &TestSignatureAggregator{},
			expectedFinalization: &NewTestFinalizeVote(t, &TestBlock{}, []byte{1}).Finalization,
			expectError:          nil,
		},
		{
			name: "finalizations with different digests",
			finalizeVotes: []*simplex.FinalizeVote{
				NewTestFinalizeVote(t, &TestBlock{Digest: [32]byte{1}}, []byte{1}),
				NewTestFinalizeVote(t, &TestBlock{Digest: [32]byte{2}}, []byte{2}),
				NewTestFinalizeVote(t, &TestBlock{Digest: [32]byte{3}}, []byte{3}),
			},
			signatureAggregator: &TestSignatureAggregator{},
			expectError:         simplex.ErrorInvalidFinalizationDigest,
		},
		{
			name: "signature aggregator errors",
			finalizeVotes: []*simplex.FinalizeVote{
				NewTestFinalizeVote(t, &TestBlock{}, []byte{1}),
			},
			signatureAggregator: &TestSignatureAggregator{Err: errorSigAggregation},
			expectError:         errorSigAggregation,
		},
		{
			name:                "no votes",
			finalizeVotes:       []*simplex.FinalizeVote{},
			signatureAggregator: &TestSignatureAggregator{},
			expectError:         simplex.ErrorNoVotes,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			finalization, err := simplex.NewFinalization(l, tt.signatureAggregator, tt.finalizeVotes)
			require.ErrorIs(t, err, tt.expectError, "expected error, got nil")

			if tt.expectError == nil {
				require.Equal(t, finalization.Finalization.Digest, tt.expectedFinalization.Digest, "digests not correct")

				signers := finalization.QC.Signers()
				require.Equal(t, len(signers), len(tt.finalizeVotes), "unexpected amount of signers")

				// ensure the qc signers are in order
				for i, signer := range signers[1:] {
					require.Negative(t, bytes.Compare(signers[i], signer), "signers not in order")
				}
			}
		})
	}
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"bytes"
	"errors"
	"simplex"
	"simplex/testutil"
	"testing"

	"github.com/stretchr/testify/require"
)

var errorSigAggregation = errors.New("signature error")

func TestNewNotarization(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	testBlock :=  &testutil.TestBlock{}

	tests := []struct {
		name                 string
		votesForCurrentRound map[string]*simplex.Vote
		block                simplex.VerifiedBlock
		expectError          error
		signatureAggregator  simplex.SignatureAggregator
	}{
		{
			name: "valid notarization",
			votesForCurrentRound: func() map[string]*simplex.Vote {
				votes := make(map[string]*simplex.Vote)
				nodeIds := [][]byte{{1}, {2}, {3}, {4}, {5}}
				for _, nodeId := range nodeIds {
					vote, err := newTestVote(testBlock, nodeId, &testutil.TestSigner{})
					require.NoError(t, err)
					votes[string(nodeId)] = vote
				}
				return votes
			}(),
			block:               testBlock,
			signatureAggregator: &testSignatureAggregator{},
			expectError:         nil,
		},
		{
			name:                 "no votes",
			votesForCurrentRound: map[string]*simplex.Vote{},
			block:                testBlock,
			signatureAggregator:  &testSignatureAggregator{},
			expectError:          simplex.ErrorNoVotes,
		},
		{
			name: "error aggregating",
			votesForCurrentRound: func() map[string]*simplex.Vote {
				votes := make(map[string]*simplex.Vote)
				nodeIds := [][]byte{{1}, {2}, {3}, {4}, {5}}
				for _, nodeId := range nodeIds {
					vote, err := newTestVote(testBlock, nodeId)
					require.NoError(t, err)
					votes[string(nodeId)] = vote
				}
				return votes
			}(),
			block:               testBlock,
			signatureAggregator: &testSignatureAggregator{err: errorSigAggregation},
			expectError:         errorSigAggregation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notarization, err := simplex.NewNotarization(l, tt.signatureAggregator, tt.votesForCurrentRound, tt.block.BlockHeader())
			require.ErrorIs(t, err, tt.expectError, "expected error, got nil")

			if tt.expectError == nil {
				signers := notarization.QC.Signers()
				require.Equal(t, len(signers), len(tt.votesForCurrentRound), "incorrect amount of signers")

				for i, signer := range signers[1:] {
					require.Negative(t, bytes.Compare(signers[i], signer), "signers not in order")
				}
			}
		})
	}

}

func TestNewFinalizationCertificate(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	tests := []struct {
		name                 string
		finalizations        []*simplex.Finalization
		signatureAggregator  simplex.SignatureAggregator
		expectedFinalization *simplex.ToBeSignedFinalization
		expectedQC           *simplex.QuorumCertificate
		expectError          error
	}{
		{
			name: "valid finalizations in order",
			finalizations: []*simplex.Finalization{
				newTestFinalization(t, &testBlock{}, []byte{1}),
				newTestFinalization(t, &testBlock{}, []byte{2}),
				newTestFinalization(t, &testBlock{}, []byte{3}),
			},
			signatureAggregator:  &testSignatureAggregator{},
			expectedFinalization: &newTestFinalization(t, &testBlock{}, []byte{1}).Finalization,
			expectError:          nil,
		},
		{
			name: "unsorted finalizations",
			finalizations: []*simplex.Finalization{
				newTestFinalization(t, &testBlock{}, []byte{3}),
				newTestFinalization(t, &testBlock{}, []byte{1}),
				newTestFinalization(t, &testBlock{}, []byte{2}),
			},
			signatureAggregator:  &testSignatureAggregator{},
			expectedFinalization: &newTestFinalization(t, &testBlock{}, []byte{1}).Finalization,
			expectError:          nil,
		},
		{
			name: "finalizations with different digests",
			finalizations: []*simplex.Finalization{
				newTestFinalization(t, &testBlock{digest: [32]byte{1}}, []byte{1}),
				newTestFinalization(t, &testBlock{digest: [32]byte{2}}, []byte{2}),
				newTestFinalization(t, &testBlock{digest: [32]byte{3}}, []byte{3}),
			},
			signatureAggregator: &testSignatureAggregator{},
			expectError:         simplex.ErrorInvalidFinalizationDigest,
		},
		{
			name: "signature aggregator errors",
			finalizations: []*simplex.Finalization{
				newTestFinalization(t, &testBlock{}, []byte{1}),
			},
			signatureAggregator: &testSignatureAggregator{err: errorSigAggregation},
			expectError:         errorSigAggregation,
		},
		{
			name:                "no votes",
			finalizations:       []*simplex.Finalization{},
			signatureAggregator: &testSignatureAggregator{},
			expectError:         simplex.ErrorNoVotes,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fCert, err := simplex.NewFinalizationCertificate(l, tt.signatureAggregator, tt.finalizations)
			require.ErrorIs(t, err, tt.expectError, "expected error, got nil")

			if tt.expectError == nil {
				require.Equal(t, fCert.Finalization.Digest, tt.expectedFinalization.Digest, "digests not correct")

				signers := fCert.QC.Signers()
				require.Equal(t, len(signers), len(tt.finalizations), "unexpected amount of signers")

				// ensure the qc signers are in order
				for i, signer := range signers[1:] {
					require.Negative(t, bytes.Compare(signers[i], signer), "signers not in order")
				}
			}
		})
	}
}

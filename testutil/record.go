// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package testutil

import (
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/record"

	"github.com/stretchr/testify/require"
)

// NewEmptyNotarization creates a new empty notarization
func NewEmptyNotarization(nodes []simplex.NodeID, round uint64) *simplex.EmptyNotarization {
	var qc TestQC

	for i, node := range nodes {
		qc = append(qc, simplex.Signature{Signer: node, Value: []byte{byte(i)}})
	}

	return &simplex.EmptyNotarization{
		QC: qc,
		Vote: simplex.ToBeSignedEmptyVote{EmptyVoteMetadata: simplex.EmptyVoteMetadata{
			Round: round,
		}},
	}
}

func NewNotarization(logger simplex.Logger, signatureAggregator simplex.SignatureAggregator, block simplex.VerifiedBlock, ids []simplex.NodeID) (simplex.Notarization, error) {
	votesForCurrentRound := make(map[string]*simplex.Vote)
	for _, id := range ids {
		vote, err := NewTestVote(block, id)
		if err != nil {
			return simplex.Notarization{}, err
		}

		votesForCurrentRound[string(id)] = vote
	}

	notarization, err := simplex.NewNotarization(logger, signatureAggregator, votesForCurrentRound, block.BlockHeader())
	return notarization, err
}

func NewNotarizationRecord(logger simplex.Logger, signatureAggregator simplex.SignatureAggregator, block simplex.VerifiedBlock, ids []simplex.NodeID) ([]byte, error) {
	notarization, err := NewNotarization(logger, signatureAggregator, block, ids)
	if err != nil {
		return nil, err
	}

	record := simplex.NewQuorumRecord(notarization.QC.Bytes(), notarization.Vote.Bytes(), record.NotarizationRecordType)
	return record, nil
}

// creates a new finalization
func NewFinalizationRecord(t *testing.T, logger simplex.Logger, signatureAggregator simplex.SignatureAggregator, block simplex.VerifiedBlock, ids []simplex.NodeID) (simplex.Finalization, []byte) {
	finalizations := make([]*simplex.FinalizeVote, len(ids))
	for i, id := range ids {
		finalizations[i] = NewTestFinalizeVote(t, block, id)
	}

	finalization, err := simplex.NewFinalization(logger, signatureAggregator, finalizations)
	require.NoError(t, err)

	record := simplex.NewQuorumRecord(finalization.QC.Bytes(), finalization.Finalization.Bytes(), record.FinalizationRecordType)

	return finalization, record
}

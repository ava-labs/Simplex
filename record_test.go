// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"simplex"
	"simplex/record"
	"testing"

	"github.com/stretchr/testify/require"
)

func newNotarization(logger simplex.Logger, signatureAggregator simplex.SignatureAggregator, block simplex.VerifiedBlock, ids []simplex.NodeID) (simplex.Notarization, error) {
	votesForCurrentRound := make(map[string]*simplex.Vote)
	for _, id := range ids {
		vote, err := newTestVote(block, id)
		if err != nil {
			return simplex.Notarization{}, err
		}

		votesForCurrentRound[string(id)] = vote
	}

	notarization, err := simplex.NewNotarization(logger, signatureAggregator, votesForCurrentRound, block.BlockHeader())
	return notarization, err
}

func newNotarizationRecord(logger simplex.Logger, signatureAggregator simplex.SignatureAggregator, block simplex.VerifiedBlock, ids []simplex.NodeID) ([]byte, error) {
	notarization, err := newNotarization(logger, signatureAggregator, block, ids)
	if err != nil {
		return nil, err
	}

	record := simplex.NewQuorumRecord(notarization.QC.Bytes(), notarization.Vote.Bytes(), record.NotarizationRecordType)
	return record, nil
}

// creates a new finalization
func newFinalizationRecord(t *testing.T, logger simplex.Logger, signatureAggregator simplex.SignatureAggregator, block simplex.VerifiedBlock, ids []simplex.NodeID) (simplex.Finalization, []byte) {
	finalizations := make([]*simplex.FinalizeVote, len(ids))
	for i, id := range ids {
		finalizations[i] = newTestFinalizeVote(t, block, id)
	}

	finalization, err := simplex.NewFinalization(logger, signatureAggregator, finalizations)
	require.NoError(t, err)

	record := simplex.NewQuorumRecord(finalization.QC.Bytes(), finalization.Finalization.Bytes(), record.FinalizationRecordType)

	return finalization, record
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"simplex"
	"simplex/record"
	"testing"

	"github.com/stretchr/testify/require"
)

func newNotarizationRecord(logger simplex.Logger, signatureAggregator simplex.SignatureAggregator, block simplex.Block, ids []simplex.NodeID) ([]byte, error) {
	votesForCurrentRound := make(map[string]*simplex.Vote)
	for _, id := range ids {
		vote, err := newTestVote(block, id)
		if err != nil {
			return nil, err
		}
		votesForCurrentRound[string(id)] = vote
	}

	notarization, err := simplex.NewNotarization(logger, signatureAggregator, votesForCurrentRound, block.BlockHeader())
	if err != nil {
		return nil, err
	}

	record := simplex.NewQuorumRecord(notarization.QC.Bytes(), notarization.Vote.Bytes(), record.NotarizationRecordType)

	return record, nil
}

// creates a new finalization certificate
func newFinalizationRecord(t *testing.T, logger simplex.Logger, signatureAggregator simplex.SignatureAggregator, block simplex.Block, ids []simplex.NodeID) (simplex.FinalizationCertificate, []byte) {
	finalizations := make([]*simplex.Finalization, len(ids))
	for i, id := range ids {
		testBlock := block.(*testBlock)
		finalizations[i] = newTestFinalization(t, testBlock, id)
	}

	fCert, err := simplex.NewFinalizationCertificate(logger, signatureAggregator, finalizations)
	require.NoError(t, err)

	record := simplex.NewQuorumRecord(fCert.QC.Bytes(), fCert.Finalization.Bytes(), record.FinalizationRecordType)

	return fCert, record
}

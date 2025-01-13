// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"fmt"
	"simplex"
	"simplex/record"
)

func newNotarizationRecord(signatureAggregator simplex.SignatureAggregator, block simplex.Block, ids []simplex.NodeID, signer simplex.Signer) ([]byte, error) {
	votesForCurrentRound := make(map[string]*simplex.Vote)
	for _, id := range ids {
		vote, err := newTestVote(block, id, signer)
		if err != nil {
			return nil, err
		}
		votesForCurrentRound[string(id)] = vote
	}

	notarization, err := newNotarization(signatureAggregator, votesForCurrentRound, block)
	if err != nil {
		return nil, err
	}

	record := simplex.NewQuorumRecord(notarization.QC.Bytes(), notarization.Vote.Bytes(), record.NotarizationRecordType)

	return record, nil
}

func newNotarization(signatureAggregator simplex.SignatureAggregator, votesForCurrentRound map[string]*simplex.Vote, block simplex.Block) (simplex.Notarization, error) {
	vote := simplex.ToBeSignedVote{
		simplex.BlockHeader{
			ProtocolMetadata: simplex.ProtocolMetadata{
				Epoch: block.BlockHeader().Epoch,
				Round: block.BlockHeader().Round,
			},
			Digest: block.BlockHeader().Digest,
		},
	}

	voteCount := len(votesForCurrentRound)

	signatures := make([]simplex.Signature, 0, voteCount)
	for _, vote := range votesForCurrentRound {
		signatures = append(signatures, vote.Signature)
	}

	var notarization simplex.Notarization
	var err error
	notarization.Vote = vote
	notarization.QC, err = signatureAggregator.Aggregate(signatures)
	if err != nil {
		return simplex.Notarization{}, fmt.Errorf("could not aggregate signatures for notarization: %w", err)
	}

	return notarization, nil
}

// creates a new finalization certificate
func newFinalizationRecord(signatureAggregator simplex.SignatureAggregator, block simplex.Block, ids []simplex.NodeID) (simplex.FinalizationCertificate, []byte, error) {
	finalizations := make([]*simplex.Finalization, len(ids))
	for i, id := range ids {
		testBlock := block.(*testBlock)
		finalizations[i] = newTestFinalization(testBlock, id)
	}

	fCert, err := newFinalizationCertificate(signatureAggregator, finalizations)
	if err != nil {
		return simplex.FinalizationCertificate{}, nil, err
	}

	record := simplex.NewQuorumRecord(fCert.QC.Bytes(), fCert.Finalization.Bytes(), record.FinalizationRecordType)

	return fCert, record, nil
}

func newFinalizationCertificate(signatureAggregator simplex.SignatureAggregator, finalizations []*simplex.Finalization) (simplex.FinalizationCertificate, error) {
	voteCount := len(finalizations)

	signatures := make([]simplex.Signature, 0, voteCount)
	for _, vote := range finalizations {
		// TODO: ensure all finalizations agree on the same metadata!
		signatures = append(signatures, vote.Signature)
	}

	var fCert simplex.FinalizationCertificate
	var err error
	fCert.Finalization = finalizations[0].Finalization
	fCert.QC, err = signatureAggregator.Aggregate(signatures)
	if err != nil {
		return simplex.FinalizationCertificate{}, fmt.Errorf("could not aggregate signatures for finalization certificate: %w", err)
	}

	return fCert, nil
}

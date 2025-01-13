package simplex

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"go.uber.org/zap"
)

var ErrorInvalidFinalizationDigest = errors.New("finalization digests do not match")

// NewNotarization builds a Notarization for a block described by [blockHeader] from [votesForCurrentRound].
func NewNotarization(logger Logger, signatureAggregator SignatureAggregator, votesForCurrentRound map[string]*Vote, blockHeader BlockHeader) (Notarization, error) {
	vote := ToBeSignedVote{
		BlockHeader{
			ProtocolMetadata: ProtocolMetadata{
				Epoch: blockHeader.Epoch,
				Round: blockHeader.Round,
			},
			Digest: blockHeader.Digest,
		},
	}

	voteCount := len(votesForCurrentRound)

	signatures := make([]Signature, 0, voteCount)
	logger.Info("Collected Quorum of votes", zap.Uint64("round", blockHeader.Round), zap.Int("votes", voteCount))
	for _, vote := range votesForCurrentRound {
		logger.Debug("Collected vote from node", zap.Stringer("NodeID", vote.Signature.Signer))
		signatures = append(signatures, vote.Signature)
	}

	// sort the signatures by Signer to ensure consistent ordering
	slices.SortFunc(signatures, compareSignatures)

	var notarization Notarization
	var err error
	notarization.Vote = vote
	notarization.QC, err = signatureAggregator.Aggregate(signatures)
	if err != nil {
		return Notarization{}, fmt.Errorf("could not aggregate signatures for notarization: %w", err)
	}

	return notarization, nil
}

// NewFinalizationCertificate builds a FinalizationCertificate from [finalizations].
func NewFinalizationCertificate(logger Logger, signatureAggregator SignatureAggregator, finalizations []*Finalization) (FinalizationCertificate, error) {
	voteCount := len(finalizations)

	signatures := make([]Signature, 0, voteCount)
	expectedDigest := finalizations[0].Finalization.Digest
	for _, vote := range finalizations {
		if vote.Finalization.Digest != expectedDigest {
			return FinalizationCertificate{}, ErrorInvalidFinalizationDigest
		}
		logger.Debug("Collected finalization from node", zap.Stringer("NodeID", vote.Signature.Signer))
		signatures = append(signatures, vote.Signature)
	}

	// sort the signatures, as they are not guaranteed to be in the same order
	slices.SortFunc(signatures, compareSignatures)

	var fCert FinalizationCertificate
	var err error
	fCert.Finalization = finalizations[0].Finalization
	fCert.QC, err = signatureAggregator.Aggregate(signatures)
	if err != nil {
		return FinalizationCertificate{}, fmt.Errorf("could not aggregate signatures for finalization certificate: %w", err)
	}

	return fCert, nil
}

// compareSignatures compares two signatures by their Signer field returning -1, 0, 1 if i is less than, equal to, or greater than j.
func compareSignatures(i, j Signature) int {
	return bytes.Compare(i.Signer, j.Signer)
}

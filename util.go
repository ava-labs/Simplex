// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

// RetrieveLastIndexFromStorage retrieves the latest block and fCert from storage.
// Returns an error if it cannot be retrieved but the storage has some block.
// Returns (nil, nil) if the storage is empty.
func RetrieveLastIndexFromStorage(s Storage) (*VerifiedFinalizedBlock, error) {
	height := s.Height()
	if height == 0 {
		return nil, nil
	}
	lastBlock, fCert, retrieved := s.Retrieve(height - 1)
	if !retrieved {
		return nil, fmt.Errorf("failed retrieving last block from storage with seq %d", height-1)
	}
	return &VerifiedFinalizedBlock{
		VerifiedBlock: lastBlock,
		FCert:         fCert,
	}, nil
}

func IsFinalizationCertificateValid(eligibleSigners map[string]struct{}, fCert *FinalizationCertificate, quorumSize int, logger Logger) bool {
	valid := validateFinalizationQC(eligibleSigners, fCert, quorumSize, logger)
	if !valid {
		return false
	}
	if !valid {
		return false
	}

	return true
}

func validateFinalizationQC(eligibleSigners map[string]struct{}, fCert *FinalizationCertificate, quorumSize int, logger Logger) bool {
	if fCert.QC == nil {
		return false
	}

	// Check enough signers signed the finalization certificate
	if quorumSize > len(fCert.QC.Signers()) {
		logger.Debug("ToBeSignedFinalization certificate signed by insufficient nodes",
			zap.Int("count", len(fCert.QC.Signers())),
			zap.Int("Quorum", quorumSize))
		return false
	}

	doubleSigner, signedTwice := hasSomeNodeSignedTwice(fCert.QC.Signers(), logger)

	if signedTwice {
		logger.Debug("Finalization certificate signed twice by the same node", zap.Stringer("signer", doubleSigner))
		return false
	}

	// Finally, check that all signers are eligible of signing, and we don't have made up identities
	for _, signer := range fCert.QC.Signers() {
		if _, exists := eligibleSigners[string(signer)]; !exists {
			logger.Debug("Finalization Quorum Certificate contains an unknown signer", zap.Stringer("signer", signer))
			return false
		}
	}

	if err := fCert.Verify(); err != nil {
		return false
	}

	return true
}

func hasSomeNodeSignedTwice(nodeIDs []NodeID, logger Logger) (NodeID, bool) {
	seen := make(map[string]struct{}, len(nodeIDs))

	for _, nodeID := range nodeIDs {
		if _, alreadySeen := seen[string(nodeID)]; alreadySeen {
			logger.Warn("Observed a signature originating at least twice from the same node")
			return nodeID, true
		}
		seen[string(nodeID)] = struct{}{}
	}

	return NodeID{}, false
}

// GetLatestVerifiedQuorumRound returns the latest verified quorum round given
// a round, empty notarization, and last block. If all are nil, it returns nil.
func GetLatestVerifiedQuorumRound(round *Round, emptyNotarization *EmptyNotarization, lastBlock *VerifiedFinalizedBlock) *VerifiedQuorumRound {
	var verifiedQuorumRound *VerifiedQuorumRound
	var highestRound uint64
	var exists bool

	if round != nil {
		highestRound = round.num
		verifiedQuorumRound = &VerifiedQuorumRound{
			VerifiedBlock: round.block,
			Notarization:  round.notarization,
			FCert:         round.fCert,
		}
		exists = true
	}

	if emptyNotarization != nil {
		emptyNoteRound := emptyNotarization.Vote.Round
		if emptyNoteRound > highestRound || !exists {
			verifiedQuorumRound = &VerifiedQuorumRound{
				EmptyNotarization: emptyNotarization,
			}
			highestRound = emptyNotarization.Vote.ProtocolMetadata.Round
			exists = true
		}
	}

	if lastBlock != nil && (lastBlock.VerifiedBlock.BlockHeader().Round > highestRound || !exists) {
		verifiedQuorumRound = &VerifiedQuorumRound{
			VerifiedBlock: lastBlock.VerifiedBlock,
			FCert:         &lastBlock.FCert,
		}
	}

	return verifiedQuorumRound
}

// SetRound is a helper function that is used for tests to create a round.
func SetRound(block VerifiedBlock, notarization *Notarization, fCert *FinalizationCertificate) *Round {
	round := &Round{
		block:        block,
		notarization: notarization,
		fCert:        fCert,
		num:          block.BlockHeader().Round,
	}

	return round
}

type oneTimeVerifier struct {
	lock    sync.Mutex
	digests map[Digest]verifiedResult
	logger  Logger
}

func newOneTimeVerifier(logger Logger) *oneTimeVerifier {
	return &oneTimeVerifier{
		digests: make(map[Digest]verifiedResult),
		logger:  logger,
	}
}

func (otv *oneTimeVerifier) Wrap(block Block) Block {
	return &oneTimeVerifiedBlock{
		otv:   otv,
		Block: block,
	}
}

type verifiedResult struct {
	vb  VerifiedBlock
	err error
}

type oneTimeVerifiedBlock struct {
	otv *oneTimeVerifier
	Block
}

func (block *oneTimeVerifiedBlock) Verify(ctx context.Context) (VerifiedBlock, error) {
	block.otv.lock.Lock()
	defer block.otv.lock.Unlock()

	header := block.Block.BlockHeader()
	digest := header.Digest
	seq := header.Seq

	// cleanup
	defer func() {
		for _, vr := range block.otv.digests {
			bh := vr.vb.BlockHeader()
			if bh.Seq < seq {
				delete(block.otv.digests, bh.Digest)
			}
		}
	}()

	if result, exists := block.otv.digests[digest]; exists {
		block.otv.logger.Warn("Attempted to verify an already verified block", zap.Uint64("round", header.Round))
		return result.vb, result.err
	}

	vb, err := block.Block.Verify(ctx)

	block.otv.digests[digest] = verifiedResult{
		vb:  vb,
		err: err,
	}

	return vb, err
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

// RetrieveLastIndexFromStorage retrieves the latest block and finalization from storage.
// Returns an error if it cannot be retrieved but the storage has some block.
// Returns (nil, nil) if the storage is empty.
func RetrieveLastIndexFromStorage(s Storage) (*VerifiedFinalizedBlock, error) {
	height := s.Height()
	if height == 0 {
		return nil, nil
	}
	lastBlock, finalization, retrieved := s.Retrieve(height - 1)
	if !retrieved {
		return nil, fmt.Errorf("failed retrieving last block from storage with seq %d", height-1)
	}
	return &VerifiedFinalizedBlock{
		VerifiedBlock: lastBlock,
		Finalization:  finalization,
	}, nil
}

func IsFinalizationValid(eligibleSigners map[string]struct{}, finalization *Finalization, quorumSize int, logger Logger) bool {
	valid := validateFinalizationQC(eligibleSigners, finalization, quorumSize, logger)
	if !valid {
		return false
	}
	if !valid {
		return false
	}

	return true
}

func validateFinalizationQC(eligibleSigners map[string]struct{}, finalization *Finalization, quorumSize int, logger Logger) bool {
	if finalization.QC == nil {
		return false
	}

	// Check enough signers signed the finalization
	if quorumSize > len(finalization.QC.Signers()) {
		logger.Debug("ToBeSignedFinalization signed by insufficient nodes",
			zap.Int("count", len(finalization.QC.Signers())),
			zap.Int("Quorum", quorumSize))
		return false
	}

	doubleSigner, signedTwice := hasSomeNodeSignedTwice(finalization.QC.Signers(), logger)

	if signedTwice {
		logger.Debug("Finalization signed twice by the same node", zap.Stringer("signer", doubleSigner))
		return false
	}

	// Finally, check that all signers are eligible of signing, and we don't have made up identities
	for _, signer := range finalization.QC.Signers() {
		if _, exists := eligibleSigners[string(signer)]; !exists {
			logger.Debug("Finalization Quorum Certificate contains an unknown signer", zap.Stringer("signer", signer))
			return false
		}
	}

	if err := finalization.Verify(); err != nil {
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
			Finalization:  round.finalization,
		}
		exists = true
	}

	if emptyNotarization != nil {
		emptyNoteRound := emptyNotarization.Vote.Round
		if emptyNoteRound > highestRound || !exists {
			verifiedQuorumRound = &VerifiedQuorumRound{
				EmptyNotarization: emptyNotarization,
			}
			highestRound = emptyNotarization.Vote.Round
			exists = true
		}
	}

	if lastBlock != nil && (lastBlock.VerifiedBlock.BlockHeader().Round > highestRound || !exists) {
		verifiedQuorumRound = &VerifiedQuorumRound{
			VerifiedBlock: lastBlock.VerifiedBlock,
			Finalization:  &lastBlock.Finalization,
		}
	}

	return verifiedQuorumRound
}

// SetRound is a helper function that is used for tests to create a round.
func SetRound(block VerifiedBlock, notarization *Notarization, finalization *Finalization) *Round {
	round := &Round{
		block:        block,
		notarization: notarization,
		finalization: finalization,
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
	seq uint64
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
		for digest, vr := range block.otv.digests {
			if vr.seq < seq {
				delete(block.otv.digests, digest)
			}
		}
	}()

	if result, exists := block.otv.digests[digest]; exists {
		block.otv.logger.Warn("Attempted to verify an already verified block", zap.Uint64("round", header.Round))
		return result.vb, result.err
	}

	vb, err := block.Block.Verify(ctx)

	block.otv.digests[digest] = verifiedResult{
		seq: seq,
		vb:  vb,
		err: err,
	}

	return vb, err
}

type Segment struct {
	Start uint64
	End   uint64
}

// compressSequences takes a sorted slice of uint64 values representing
// missing sequence numbers and compresses consecutive numbers into segments.
// Each segment represents a continuous block of missing sequence numbers.
func CompressSequences(missingSeqs []uint64) []Segment {
	var segments []Segment

	if len(missingSeqs) == 0 {
		return segments
	}

	startSeq := missingSeqs[0]
	endSeq := missingSeqs[0]

	for i, currentSeq := range missingSeqs[1:] {
		if currentSeq != missingSeqs[i]+1 {
			segments = append(segments, Segment{
				Start: startSeq,
				End:   endSeq,
			})
			startSeq = currentSeq
		}
		endSeq = currentSeq
	}

	segments = append(segments, Segment{
		Start: startSeq,
		End:   endSeq,
	})

	return segments
}

// DistributeSequenceRequests evenly creates segments amongst [numNodes] over
// the range [start, end].
func DistributeSequenceRequests(start, end uint64, numNodes int) []Segment {
	var segments []Segment

	if numNodes <= 0 || start > end {
		return segments
	}

	numSeqs := end + 1 - start
	seqsPerNode := numSeqs / uint64(numNodes)
	remainder := numSeqs % uint64(numNodes)

	if seqsPerNode == 0 {
		seqsPerNode = 1
	}

	nodeStart := start

	for i := 0; i < numNodes && nodeStart <= end; i++ {
		segmentLength := seqsPerNode
		if remainder > 0 {
			segmentLength++
			remainder--
		}

		nodeEnd := min(nodeStart+segmentLength-1, end)

		segments = append(segments, Segment{
			Start: nodeStart,
			End:   nodeEnd,
		})

		nodeStart = nodeEnd + 1
	}

	return segments
}

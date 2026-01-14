// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// RetrieveLastIndexFromStorage retrieves the latest block and finalization from storage.
// Returns an error if it cannot be retrieved but the storage has some block.
// Returns (nil, nil) if the storage is empty.
func RetrieveLastIndexFromStorage(s Storage) (*VerifiedFinalizedBlock, error) {
	numBlocks := s.NumBlocks()
	if numBlocks == 0 {
		return nil, nil
	}
	lastBlock, finalization, err := s.Retrieve(numBlocks - 1)
	if err != nil {
		return nil, fmt.Errorf("failed retrieving last block from storage with seq %d: %w", numBlocks-1, err)
	}
	return &VerifiedFinalizedBlock{
		VerifiedBlock: lastBlock,
		Finalization:  finalization,
	}, nil
}

func hasSomeNodeSignedTwice(nodeIDs []NodeID, logger Logger) (NodeID, bool) {
	seen := make(map[string]struct{}, len(nodeIDs))

	for _, nodeID := range nodeIDs {
		if _, alreadySeen := seen[string(nodeID)]; alreadySeen {
			logger.Debug("Observed a signature originating at least twice from the same node")
			return nodeID, true
		}
		seen[string(nodeID)] = struct{}{}
	}

	return NodeID{}, false
}

func VerifyQC(qc QuorumCertificate, logger Logger, messageType string, isQuorum func(signers []NodeID) bool, eligibleSigners map[string]struct{}, messageToVerify verifiableMessage, from NodeID) error {
	if qc == nil {
		logger.Debug("Received nil QuorumCertificate")
		return fmt.Errorf("nil QuorumCertificate")
	}
	msgTypeLowerCase := strings.ToLower(messageType)
	// Ensure no node signed the QuorumCertificate twice
	doubleSigner, signedTwice := hasSomeNodeSignedTwice(qc.Signers(), logger)
	if signedTwice {
		logger.Debug(fmt.Sprintf("%s is signed by the same node more than once", messageType), zap.Stringer("signer", doubleSigner))
		return fmt.Errorf("%s is signed by the same node (%s) more than once", msgTypeLowerCase, doubleSigner)
	}

	// Check enough signers signed the QuorumCertificate
	if !isQuorum(qc.Signers()) {
		logger.Debug(fmt.Sprintf("%s certificate signed by insufficient nodes", messageType),
			zap.Int("count", len(qc.Signers())))
		return fmt.Errorf("%s certificate signed by insufficient (%d) nodes", msgTypeLowerCase, len(qc.Signers()))
	}

	// Check QuorumCertificate was signed by only eligible nodes
	for _, signer := range qc.Signers() {
		if _, exists := eligibleSigners[string(signer)]; !exists {
			logger.Debug(fmt.Sprintf("%s quorum certificate contains an unknown signer", messageType), zap.Stringer("signer", signer))
			return fmt.Errorf("%s quorum certificate contains an unknown signer (%s)", msgTypeLowerCase, signer)
		}
	}

	if err := messageToVerify.Verify(); err != nil {
		if len(from) > 0 {
			logger.Debug(fmt.Sprintf("%s quorum certificate is invalid", messageType), zap.Stringer("NodeID", from), zap.Error(err))
		} else {
			logger.Debug(fmt.Sprintf("%s quorum certificate is invalid", messageType), zap.Error(err))
		}
		return err
	}
	return nil
}

// GetLatestVerifiedQuorumRound returns the latest verified quorum round given
// a round and empty notarization. If both are nil, it returns nil.
func GetLatestVerifiedQuorumRound(round *Round, emptyNotarization *EmptyNotarization) *VerifiedQuorumRound {
	var verifiedQuorumRound *VerifiedQuorumRound
	var highestRound uint64
	var exists bool

	if round != nil && (round.finalization != nil || round.notarization != nil) {
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
		block.otv.logger.Debug("Attempted to verify an already verified block", zap.Uint64("round", header.Round))
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

// compressSequences takes a slice of uint64 values representing
// missing sequence numbers and compresses consecutive numbers into segments.
// Each segment represents a continuous block of missing sequence numbers.
func CompressSequences(missingSeqs []uint64) []Segment {
	slices.Sort(missingSeqs)
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

type NotarizationTime struct {
	// config
	getRound                       func() uint64
	haveUnFinalizedNotarization    func() (uint64, bool)
	rebroadcastFinalizationVotes   func()
	checkInterval                  time.Duration
	finalizeVoteRebroadcastTimeout time.Duration
	// state
	lastSampleTime          time.Time
	latestRound             uint64
	lastRebroadcastTime     time.Time
	oldestNotFinalizedRound uint64
}

func NewNotarizationTime(
	finalizeVoteRebroadcastTimeout time.Duration,
	haveUnFinalizedNotarization func() (uint64, bool),
	rebroadcastFinalizationVotes func(),
	getRound func() uint64,
) NotarizationTime {
	return NotarizationTime{
		finalizeVoteRebroadcastTimeout: finalizeVoteRebroadcastTimeout,
		haveUnFinalizedNotarization:    haveUnFinalizedNotarization,
		rebroadcastFinalizationVotes:   rebroadcastFinalizationVotes,
		getRound:                       getRound,
		checkInterval:                  finalizeVoteRebroadcastTimeout / 3,
	}
}

func (nt *NotarizationTime) CheckForNotFinalizedNotarizedBlocks(now time.Time) {
	// If we have recently checked, don't check again
	if !nt.lastSampleTime.IsZero() && nt.lastSampleTime.Add(nt.checkInterval).After(now) {
		return
	}

	nt.lastSampleTime = now

	round := nt.getRound()

	// As long as we make some progress, we don't check for a round not finalized.
	if round > nt.latestRound {
		nt.latestRound = round
		return
	}

	// It is only if we didn't advance any round, that we check if we have made some progress in finalizing rounds.

	oldestNotFinalizedRound, haveNotFinalizedRound := nt.haveUnFinalizedNotarization()
	if !haveNotFinalizedRound {
		nt.lastRebroadcastTime = time.Time{}
		nt.oldestNotFinalizedRound = 0
		return
	}

	lastRebroadcastTime := nt.lastRebroadcastTime
	if lastRebroadcastTime.IsZero() {
		nt.lastRebroadcastTime = now
		nt.oldestNotFinalizedRound = oldestNotFinalizedRound
		return
	}

	if lastRebroadcastTime.Add(nt.finalizeVoteRebroadcastTimeout).Before(now) &&
		nt.oldestNotFinalizedRound == oldestNotFinalizedRound {
		nt.rebroadcastFinalizationVotes()
		nt.lastRebroadcastTime = now
	}
}

type voteSigner interface {
	Signer() NodeID
}

func NodeIDsFromVotes[VS voteSigner](votes []VS) []NodeID {
	nodeIDs := make([]NodeID, 0, len(votes))
	for _, vote := range votes {
		nodeIDs = append(nodeIDs, vote.Signer())
	}
	return nodeIDs
}

func emptyVotesToSignatures(votes []*EmptyVote) []Signature {
	sigs := make([]Signature, 0, len(votes))
	for _, vote := range votes {
		sigs = append(sigs, vote.Signature)
	}
	return sigs
}

type walRound struct {
	round             uint64
	emptyNotarization *EmptyNotarization
	emptyVote         *ToBeSignedEmptyVote
	notarization      *Notarization
	finalization      *Finalization
	finalizeVote      *FinalizeVote
	block             Block
}

func (t *walRound) String() string {
	hasEmptyNotarization := t.emptyNotarization != nil
	hasEmptyVote := t.emptyVote != nil
	hasNotarization := t.notarization != nil
	hasFinalization := t.finalization != nil
	hasBlock := t.block != nil
	return fmt.Sprintf("walRound{round: %d, hasEmptyNotarization: %t, hasEmptyVote: %t, hasNotarization: %t, hasFinalization: %t, hasBlock: %t}", t.round, hasEmptyNotarization, hasEmptyVote, hasNotarization, hasFinalization, hasBlock)
}

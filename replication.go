// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"fmt"
	"math"

	"go.uber.org/zap"
)

// signedSequence is a sequence that has been signed by a qourum certificate.
// it essentially is a quorum round without the enforcement of needing a block with a
// finalization certificate or notarization.
type signedSequence struct {
	seq     uint64
	signers NodeIDs
}

func newSignedSequenceFromRound(round QuorumRound) (*signedSequence, error) {
	ss := &signedSequence{}
	switch {
	case round.FCert != nil:
		ss.signers = round.FCert.QC.Signers()
		ss.seq = round.FCert.Finalization.Seq
	case round.EmptyNotarization != nil:
		ss.signers = round.EmptyNotarization.QC.Signers()
		ss.seq = round.EmptyNotarization.Vote.Seq
	case round.Notarization != nil:
		ss.signers = round.Notarization.QC.Signers()
		ss.seq = round.Notarization.Vote.Seq
	default:
		return nil, fmt.Errorf("round does not contain a finalization certificate, empty notarization, or notarization")
	}

	return ss, nil
}

type ReplicationState struct {
	logger         Logger
	enabled        bool
	maxRoundWindow uint64
	comm           Communication
	id             NodeID

	// latest seq requested
	lastSequenceRequested uint64

	// highest sequence we have received
	highestSequenceObserved *signedSequence

	// receivedQuorumRounds maps rounds to quorum rounds
	receivedQuorumRounds map[uint64]QuorumRound

	// request iterator
	requestIterator int
}

func NewReplicationState(logger Logger, comm Communication, id NodeID, maxRoundWindow uint64, enabled bool) *ReplicationState {
	return &ReplicationState{
		logger:               logger,
		enabled:              enabled,
		comm:                 comm,
		id:                   id,
		maxRoundWindow:       maxRoundWindow,
		receivedQuorumRounds: make(map[uint64]QuorumRound),
	}
}

// isReplicationComplete returns true if we have finished the replication process.
// The process is considered finished once [currentRound] has caught up to the highest round received.
func (r *ReplicationState) isReplicationComplete(nextSeqToCommit uint64, currentRound uint64) bool {
	if r.highestSequenceObserved == nil {
		return true
	}

	return nextSeqToCommit > r.highestSequenceObserved.seq && currentRound > r.highestKnownRound()
}

func (r *ReplicationState) collectMissingSequences(observedSignedSeq *signedSequence, nextSeqToCommit uint64) {
	observedSeq := observedSignedSeq.seq
	// Node is behind, but we've already sent messages to collect future fCerts
	if r.lastSequenceRequested >= observedSeq && r.highestSequenceObserved != nil {
		return
	}

	if r.highestSequenceObserved == nil || observedSeq > r.highestSequenceObserved.seq {
		r.highestSequenceObserved = observedSignedSeq
	}

	startSeq := math.Max(float64(nextSeqToCommit), float64(r.lastSequenceRequested))
	// Don't exceed the max round window
	endSeq := math.Min(float64(observedSeq), float64(r.maxRoundWindow+nextSeqToCommit))

	r.logger.Debug("Node is behind, requesting missing finalization certificates", zap.Uint64("seq", observedSeq), zap.Uint64("startSeq", uint64(startSeq)), zap.Uint64("endSeq", uint64(endSeq)))
	r.sendReplicationRequests(uint64(startSeq), uint64(endSeq))
}

// sendReplicationRequests sends requests for missing sequences for the
// range of sequences [start, end] <- inclusive. It does so by splitting the
// range of sequences equally amount the nodes that have signed [highestSequenceObserved].
func (r *ReplicationState) sendReplicationRequests(start uint64, end uint64) {
	// it's possible our node has signed [highestSequenceObserved].
	// For example this may happen if our node has sent a finalization
	// for [highestSequenceObserved] and has not received the
	// finalization certificate from the network.
	nodes := r.highestSequenceObserved.signers.Remove(r.id)
	numNodes := len(nodes)

	numSeqs := end + 1 - start
	seqsPerNode := numSeqs / uint64(numNodes)

	// this way we don't send unecessary requests
	if seqsPerNode == 0 {
		seqsPerNode = 1 
	}

	// Distribute sequences evenly among nodes in round-robin fashion
	for i := range numNodes {
		nodeIndex := (r.requestIterator + i) % numNodes
		nodeStart := start + (uint64(i) * seqsPerNode)

		// Last node gets any remainder sequences
		nodeEnd := nodeStart + seqsPerNode
		if i == numNodes-1 {
			nodeEnd = end
		}

		if nodeEnd > end {
			break
		}
		r.sendRequestToNode(nodeStart, nodeEnd, nodes[nodeIndex])
	}

	// next time we send requests, we start with a different permutation
	r.requestIterator++
}

func (r *ReplicationState) sendRequestToNode(start uint64, end uint64, node NodeID) {
	r.logger.Debug("Requesting missing finalization certificates ",
		zap.Stringer("from", node),
		zap.Uint64("start", start),
		zap.Uint64("end", end))
	seqs := make([]uint64, (end+1)-start)
	for i := start; i <= end; i++ {
		seqs[i-start] = i
	}
	request := &ReplicationRequest{
		Seqs:        seqs,
		LatestRound: r.highestSequenceObserved.seq,
	}
	msg := &Message{ReplicationRequest: request}

	r.lastSequenceRequested = end
	r.comm.SendMessage(msg, node)
}

func (r *ReplicationState) replicateBlocks(fCert *FinalizationCertificate, nextSeqToCommit uint64) {
	if !r.enabled {
		return
	}

	signedSequence := &signedSequence{
		seq:     fCert.Finalization.Seq,
		signers: fCert.QC.Signers(),
	}

	r.collectMissingSequences(signedSequence, nextSeqToCommit)
}

// maybeCollectFutureSequences attempts to collect future sequences if
// there are more to be collected and the round has caught up for us to send the request.
func (r *ReplicationState) maybeCollectFutureSequences(nextSequenceToCommit uint64) {
	if !r.enabled {
		return
	}

	if r.lastSequenceRequested >= r.highestSequenceObserved.seq {
		return
	}

	// we send out more requests once our seq has caught up to 1/2 of the maxRoundWindow
	if nextSequenceToCommit+r.maxRoundWindow/2 > r.lastSequenceRequested {
		r.collectMissingSequences(r.highestSequenceObserved, nextSequenceToCommit)
	}
}

func (r *ReplicationState) StoreQuorumRound(round QuorumRound) {
	if _, ok := r.receivedQuorumRounds[round.GetRound()]; ok {
		// maybe this quorum round was behind
		if r.receivedQuorumRounds[round.GetRound()].FCert == nil && round.FCert != nil {
			r.receivedQuorumRounds[round.GetRound()] = round
		}
		return
	}

	if round.GetSequence() > r.highestSequenceObserved.seq {
		signedSeq, err := newSignedSequenceFromRound(round)
		if err != nil {
			// should never be here since we already checked the QuorumRound was valid
			r.logger.Error("Error creating signed sequence from round", zap.Error(err))
			return
		}

		r.highestSequenceObserved = signedSeq
	}
	r.receivedQuorumRounds[round.GetRound()] = round
}

func (r *ReplicationState) GetFinalizedBlockForSequence(seq uint64) (Block, FinalizationCertificate, bool) {
	for _, round := range r.receivedQuorumRounds {
		if round.GetSequence() == seq {
			if round.Block == nil || round.FCert == nil {
				// this could be an empty notarization
				continue
			}
			return round.Block, *round.FCert, true
		}
	}
	return nil, FinalizationCertificate{}, false
}

func (r *ReplicationState) highestKnownRound() uint64 {
	var highestRound uint64
	for round := range r.receivedQuorumRounds {
		if round > highestRound {
			highestRound = round
		}
	}
	return highestRound
}

func (r *ReplicationState) GetQuroumRoundWithSeq(seq uint64) *QuorumRound {
	for _, round := range r.receivedQuorumRounds {
		if round.GetSequence() == seq {
			return &round
		}
	}
	return nil
}

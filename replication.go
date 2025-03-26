// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"math"

	"go.uber.org/zap"
)

// signedSequence is a sequence that has been signed by a qourum certificate.
// it essentially is a quorum round without the enforcement of needing a block with a
// finalization certificate or notarization.
type signedSequence struct {
	fcert             *FinalizationCertificate
	notarization      *Notarization
	emptyNotarization *EmptyNotarization
}

func newSignedSequenceFromRound(round QuorumRound) *signedSequence {
	return &signedSequence{
		fcert:             round.FCert,
		notarization:      round.Notarization,
		emptyNotarization: round.EmptyNotarization,
	}
}

func (s *signedSequence) getSequence() uint64 {
	if s.fcert != nil {
		return s.fcert.Finalization.Seq
	}
	if s.notarization != nil {
		return s.notarization.Vote.Seq
	}
	if s.emptyNotarization != nil {
		return s.emptyNotarization.Vote.Seq
	}
	return 0
}

func (s *signedSequence) getSigners() []NodeID {
	if s.fcert != nil {
		return s.fcert.QC.Signers()
	}
	if s.notarization != nil {
		return s.notarization.QC.Signers()
	}
	if s.emptyNotarization != nil {
		return s.emptyNotarization.QC.Signers()
	}
	return nil
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

	// outgoing requests
	outgoingRequests map[uint64]struct{}

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

	return nextSeqToCommit > r.highestSequenceObserved.getSequence() && currentRound > r.highestKnownRound()
}

func (r *ReplicationState) collectMissingSequences(observedSignedSeq *signedSequence, nextSeqToCommit uint64) {
	observedSeq := observedSignedSeq.getSequence()
	// Node is behind, but we've already sent messages to collect future fCerts
	if r.lastSequenceRequested >= observedSeq && r.highestSequenceObserved != nil {
		return
	}

	if r.highestSequenceObserved == nil || observedSeq > r.highestSequenceObserved.getSequence() {
		r.highestSequenceObserved = observedSignedSeq
	}

	startSeq := math.Max(float64(nextSeqToCommit), float64(r.lastSequenceRequested))
	// Don't exceed the max round window
	endSeq := math.Min(float64(observedSeq), float64(r.maxRoundWindow+nextSeqToCommit))

	r.logger.Debug("Node is behind, requesting missing finalization certificates", zap.Uint64("seq", observedSeq), zap.Uint64("startSeq", uint64(startSeq)), zap.Uint64("endSeq", uint64(endSeq)))
	r.sendReplicationRequests(uint64(startSeq), uint64(endSeq))
}

func (r *ReplicationState) replicateBlocks(fCert *FinalizationCertificate, nextSeqToCommit uint64) {
	if !r.enabled {
		return
	}

	r.collectMissingSequences(&signedSequence{
		fcert: fCert,
	}, nextSeqToCommit)
}

// maybeCollectFutureSequences attempts to collect future sequences if
// there are more to be collected and the round has caught up for us to send the request.
func (r *ReplicationState) maybeCollectFutureSequences(nextSequenceToCommit uint64) {
	if !r.enabled {
		return
	}

	if r.lastSequenceRequested >= r.highestSequenceObserved.getSequence() {
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

	if round.GetSequence() > r.highestSequenceObserved.getSequence() {
		r.highestSequenceObserved = newSignedSequenceFromRound(round)
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

// sendReplicationRequests sends requests for missing sequences for the
// range of sequences [start, end] <- inclusive. It does so by splitting the
// range of sequences equally amount the nodes that have signed the [highestSequenceObserved].
func (r *ReplicationState) sendReplicationRequests(start uint64, end uint64) {
	nodes := r.highestSequenceObserved.getSigners()

	// round up to ensure we get all the sequences
	reqPerNode := uint64(math.Ceil(float64(end+1-start) / float64(len(nodes))))

	nodeIndex := r.requestIterator

	// <= because we are inclusive
	for curSeq := uint64(0); curSeq <= end+1-start; curSeq += reqPerNode {
		endSeq := uint64(math.Min(float64(end), float64(start+curSeq+reqPerNode)))
		index := nodeIndex % len(nodes)
		// it's possible our node has signed the highest sequence observed.
		// this may happen if our node has sent a finalization for the highest sequence observed,
		// however has not received the finalization certificate from the network.
		if nodes[index].Equals(r.id) {
			// in this case we shouldn't send a request to ourselves.
			index = (index + 1) % len(nodes)
		}
		r.sendRequestToNode(start+curSeq, endSeq, nodes[index])
		nodeIndex++
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
		LatestRound: r.highestSequenceObserved.getSequence(),
	}
	msg := &Message{ReplicationRequest: request}

	r.lastSequenceRequested = end
	r.comm.SendMessage(msg, node)
}

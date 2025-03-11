// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"fmt"
	"math"

	"go.uber.org/zap"
)

type ReplicationState struct {
	logger         Logger
	enabled        bool
	maxRoundWindow uint64
	comm           Communication
	id             NodeID

	// latest seq requested
	lastSequenceRequested uint64

	// highest sequence we have received
	highestSeqReceived uint64

	// receivedQuorumRounds maps rounds to quorum rounds
	receivedQuorumRounds map[uint64]QuorumRound
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
	if nextSeqToCommit == 0 {
		return false
	}

	return nextSeqToCommit >= r.highestSeqReceived
}

func (r *ReplicationState) collectMissingSequences(receivedSeq uint64, currentRound uint64, nextSeqToCommit uint64) {
	// Node is behind, but we've already sent messages to collect future fCerts
	if r.lastSequenceRequested >= uint64(receivedSeq) {
		return
	}

	if receivedSeq > r.highestSeqReceived {
		r.highestSeqReceived = receivedSeq
	}

	startSeq := math.Max(float64(nextSeqToCommit), float64(r.lastSequenceRequested))

	// Don't exceed the max round window
	endSeq := math.Min(float64(receivedSeq), float64(r.maxRoundWindow+nextSeqToCommit))

	r.logger.Debug("Node is behind, requesting missing finalization certificates", zap.Uint64("seq", receivedSeq), zap.Uint64("startSeq", uint64(startSeq)), zap.Uint64("endSeq", uint64(endSeq)))
	r.sendReplicationRequests(uint64(startSeq), uint64(endSeq))
}

// sendReplicationRequests sends requests for missing sequences for the
// range of sequences [start, end] <- inclusive
func (r *ReplicationState) sendReplicationRequests(start uint64, end uint64) {
	seqs := make([]uint64, (end+1)-start)
	for i := start; i <= end; i++ {
		seqs[i-start] = i
	}

	request := &ReplicationRequest{
		Seqs:        seqs,
		LatestRound: r.highestSeqReceived,
	}
	msg := &Message{ReplicationRequest: request}

	requestFrom := r.requestFrom()

	r.lastSequenceRequested = end
	r.comm.SendMessage(msg, requestFrom)
}

// requestFrom returns a node to send a message request to
// this is used to ensure that we are not sending a message to ourselves
func (r *ReplicationState) requestFrom() NodeID {
	nodes := r.comm.ListNodes()
	for _, node := range nodes {
		if !node.Equals(r.id) {
			return node
		}
	}

	return NodeID{}
}

func (r *ReplicationState) replicateBlocks(fCert *FinalizationCertificate, currentRound uint64, nextSeqToCommit uint64) {
	if !r.enabled {
		return
	}

	r.collectMissingSequences(fCert.Finalization.Seq, currentRound, nextSeqToCommit)
}

// maybeCollectFutureSequences attempts to collect future sequences if
// there are more to be collected and the round has caught up for us to send the request.
func (r *ReplicationState) maybeCollectFutureSequences(round uint64, nextSequenceToCommit uint64) {
	if r.lastSequenceRequested >= r.highestSeqReceived {
		return
	}

	// we send out more requests once our seq has caught up to 1/2 of the maxRoundWindow
	if round+r.maxRoundWindow/2 > r.lastSequenceRequested {
		r.collectMissingSequences(r.highestSeqReceived, round, nextSequenceToCommit)
	}
}

func (r *ReplicationState) StoreQuorumRound(round QuorumRound) {
	if _, ok := r.receivedQuorumRounds[round.GetRound()]; ok {
		return
	}

	r.receivedQuorumRounds[round.GetRound()] = round
}

func (r *ReplicationState) GetFinalizedBlockForSequence(seq uint64) *FinalizedBlock {
	fmt.Println("len of receivedQuorumRounds: ", len(r.receivedQuorumRounds))
	for _, round := range r.receivedQuorumRounds {
		fmt.Println("round.GetSequence(): ", round.GetSequence())
		if round.GetSequence() == seq {
			if round.Block == nil || round.FCert == nil {
				return nil
			}
			return &FinalizedBlock{
				Block: round.Block,
				FCert: *round.FCert,
			}
		}
	}

	return nil
}

type NotarizedBlock struct {
	notarization Notarization
	block        Block
}

func (r *ReplicationState) GetNotarizedBlockForRound(round uint64) *NotarizedBlock {
	qRound, ok := r.receivedQuorumRounds[round]
	if !ok {
		return nil
	}

	if qRound.Block == nil || qRound.FCert == nil {
		return nil
	}

	return &NotarizedBlock{
		notarization: *qRound.Notarization,
		block:        qRound.Block,
	}
}

func (r *ReplicationState) highestNotarizedRound() uint64 {
	var highestRound uint64
	for round := range r.receivedQuorumRounds {
		if round > highestRound {
			highestRound = round
		}
	}
	return highestRound
}

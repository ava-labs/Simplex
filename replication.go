// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"fmt"
	"math"

	"go.uber.org/zap"
)

type ReplicationState struct {
	logger         Logger
	maxRoundWindow uint64
	comm           Communication
	id             NodeID
	quorumSize     int

	// latest seq requested
	lastSequenceRequested uint64

	// highest sequence we have received a finalization certificate for
	highestFCertReceived *FinalizationCertificate

	// received
	receivedFinalizationCertificates map[uint64]FinalizedBlock
}

func NewReplicationState(logger Logger, comm Communication, id NodeID, maxRoundWindow uint64) *ReplicationState {
	return &ReplicationState{
		quorumSize:                       Quorum(len(comm.ListNodes())),
		logger:                           logger,
		comm:                             comm,
		id:                               id,
		maxRoundWindow:                   maxRoundWindow,
		receivedFinalizationCertificates: make(map[uint64]FinalizedBlock),
	}
}

// sendFutureCertficatesRequests sends requests for future finalization certificates for the
// range of sequences [start, end] <- inclusive
func (r *ReplicationState) sendFutureCertficatesRequests(start uint64, end uint64) {
	if r.lastSequenceRequested >= end {
		// no need to resend
		return
	}
	seqs := make([]uint64, (end+1)-start)
	for i := start; i <= end; i++ {
		seqs[i-start] = i
	}

	roundRequest := &Request{
		FinalizationCertificateRequest: &FinalizationCertificateRequest{
			Sequences: seqs,
		},
	}
	msg := &Message{Request: roundRequest}

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

func (r *ReplicationState) collectFutureFinalizationCertificates(fCert *FinalizationCertificate, currentRound uint64, nextSeqToCommit uint64) {
	fCertRound := fCert.Finalization.Round
	// Don't exceed the max round window
	endSeq := math.Min(float64(fCertRound), float64(r.maxRoundWindow+currentRound))
	if r.highestFCertReceived == nil || fCertRound > r.highestFCertReceived.Finalization.Seq {
		r.highestFCertReceived = fCert
	}
	// Node is behind, but we've already sent messages to collect future fCerts
	if r.lastSequenceRequested >= uint64(endSeq) {
		return
	}

	startSeq := math.Max(float64(nextSeqToCommit), float64(r.lastSequenceRequested))
	r.logger.Debug("Node is behind, requesting missing finalization certificates", zap.Uint64("round", fCertRound), zap.Uint64("startSeq", uint64(startSeq)), zap.Uint64("endSeq", uint64(endSeq)))
	r.sendFutureCertficatesRequests(uint64(startSeq), uint64(endSeq))
}

// shouldCollectFutureFinalizationCertificates returns true if the node should collect future finalization certificates.
// This is the case when the node knows future sequences and [round] has caught up to 1/2 of the maxRoundWindow.
func (r *ReplicationState) maybeCollectFutureFinalizationCertificates(round uint64, nextSequenceToCommit uint64) {
	if r.highestFCertReceived == nil {
		return
	}

	// we send out more request once our round has caught up to 1/2 of the maxRoundWindow
	if r.lastSequenceRequested >= r.highestFCertReceived.Finalization.Round {
		return
	}
	if round+r.maxRoundWindow/2 > r.lastSequenceRequested {
		r.collectFutureFinalizationCertificates(r.highestFCertReceived, round, nextSequenceToCommit)
	}
}

// ShouldReplicate returns true if [round] still needs to be replicated
func (r *ReplicationState) ShouldReplicate(round uint64) bool {
	if r.highestFCertReceived == nil {
		return false
	}

	return r.highestFCertReceived.Finalization.BlockHeader.Round >= round
}

func (r *ReplicationState) StoreFinalizedBlock(data FinalizedBlock) error {
	// ensure the finalization certificate we get relates to the block
	blockDigest := data.Block.BlockHeader().Digest
	if !bytes.Equal(blockDigest[:], data.FCert.Finalization.BlockHeader.Digest[:]) {
		return fmt.Errorf("Finalization certificate does not match the block")
	}

	valid, err := isFinalizationCertificateValid(&data.FCert, r.quorumSize, r.logger)
	// verify the finalization certificate
	if err != nil || !valid {
		return fmt.Errorf("Finalization certificate failed verification")
	}

	// we should never receive two valid fCerts for the same round, check anyways
	if _, ok := r.receivedFinalizationCertificates[data.FCert.Finalization.Seq]; ok {
		return fmt.Errorf("finalization certificate for sequence %d already exists", data.FCert.Finalization.Seq)
	}
	r.receivedFinalizationCertificates[data.FCert.Finalization.Seq] = data
	return nil
}

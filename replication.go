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
	enabled        bool
	maxRoundWindow uint64
	comm           Communication
	id             NodeID

	// latest seq requested
	lastSequenceRequested uint64

	// highest sequence we have received a finalization certificate for
	highestFCertReceived *FinalizationCertificate

	// received
	receivedFinalizationCertificates map[uint64]FinalizedBlock

	receivedNotarizations map[uint64]NotarizedBlock
}

func NewReplicationState(logger Logger, comm Communication, id NodeID, maxRoundWindow uint64, enabled bool) *ReplicationState {
	return &ReplicationState{
		logger:                           logger,
		enabled:                          enabled,
		comm:                             comm,
		id:                               id,
		maxRoundWindow:                   maxRoundWindow,
		receivedFinalizationCertificates: make(map[uint64]FinalizedBlock),
		receivedNotarizations: make(map[uint64]NotarizedBlock),
	}
}

// isReplicationComplete returns true if the replication state has caught up to the highest finalization certificate.
// TODO: when we add notarization requests, this function should also make sure we have caught up to the highest notarization.
func (r *ReplicationState) isReplicationComplete(nextSeqToCommit uint64) bool {
	return nextSeqToCommit > r.highestFCertReceived.Finalization.Seq
}

func (r *ReplicationState) collectFutureFinalizationCertificates(fCert *FinalizationCertificate, currentRound uint64, nextSeqToCommit uint64) {
	fCertSeq := fCert.Finalization.Seq
	// Don't exceed the max round window
	endSeq := math.Min(float64(fCertSeq), float64(r.maxRoundWindow+currentRound))
	if r.highestFCertReceived == nil || fCertSeq > r.highestFCertReceived.Finalization.Seq {
		r.highestFCertReceived = fCert
	}
	// Node is behind, but we've already sent messages to collect future fCerts
	if r.lastSequenceRequested >= uint64(endSeq) {
		return
	}

	startSeq := math.Max(float64(nextSeqToCommit), float64(r.lastSequenceRequested))
	r.logger.Debug("Node is behind, requesting missing finalization certificates", zap.Uint64("seq", fCertSeq), zap.Uint64("startSeq", uint64(startSeq)), zap.Uint64("endSeq", uint64(endSeq)))
	r.sendFutureCertficatesRequests(uint64(startSeq), uint64(endSeq))
}

// sendFutureCertficatesRequests sends requests for future finalization certificates for the
// range of sequences [start, end] <- inclusive
func (r *ReplicationState) sendFutureCertficatesRequests(start uint64, end uint64) {
	seqs := make([]uint64, (end+1)-start)
	for i := start; i <= end; i++ {
		seqs[i-start] = i
	}

	roundRequest := &ReplicationRequest{
		FinalizationCertificateRequest: &FinalizationCertificateRequest{
			Sequences: seqs,
		},
	}
	msg := &Message{ReplicationRequest: roundRequest}

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
	r.collectFutureFinalizationCertificates(fCert, currentRound, nextSeqToCommit)
	r.collectFutureNotarizations(currentRound)
}

// maybeCollectFutureFinalizationCertificates attempts to collect future finalization certificates if
// there are more fCerts to be collected and the round has caught up.
func (r *ReplicationState) maybeCollectFutureFinalizationCertificates(round uint64, nextSequenceToCommit uint64) {
	if r.highestFCertReceived == nil {
		return
	}

	if r.lastSequenceRequested >= r.highestFCertReceived.Finalization.Seq {
		return
	}

	// we send out more requests once our seq has caught up to 1/2 of the maxRoundWindow
	if round+r.maxRoundWindow/2 > r.lastSequenceRequested {
		r.collectFutureFinalizationCertificates(r.highestFCertReceived, round, nextSequenceToCommit)
	}
}

func (r *ReplicationState) StoreFinalizedBlock(data FinalizedBlock) error {
	// ensure the finalization certificate we get relates to the block
	blockDigest := data.Block.BlockHeader().Digest
	if !bytes.Equal(blockDigest[:], data.FCert.Finalization.Digest[:]) {
		return fmt.Errorf("finalization certificate does not match the block")
	}

	// don't store the same finalization certificate twice
	if _, ok := r.receivedFinalizationCertificates[data.FCert.Finalization.Seq]; ok {
		return nil
	}

	r.receivedFinalizationCertificates[data.FCert.Finalization.Seq] = data
	return nil
}

func (r *ReplicationState) storeNotarizedBlock(data NotarizedBlock) {
	if _, ok := r.receivedNotarizations[data.GetRound()]; ok {
		return
	}

	r.receivedNotarizations[data.GetRound()] = data
}

func (r *ReplicationState) collectFutureNotarizations(currentRound uint64) {
	// round to start collecting notarizations
	start := max(r.highestNotarizedRound(), currentRound)

	msg := &Message{
		ReplicationRequest: &ReplicationRequest{
			NotarizationRequest: &NotarizationRequest{
				StartRound: start,
			},
		},
	}

	requestFrom := r.requestFrom()
	r.comm.SendMessage(msg, requestFrom)
}

func (r *ReplicationState) highestNotarizedRound() uint64 {
	var highestRound uint64
	for round := range r.receivedNotarizations {
		if round > highestRound {
			highestRound = round
		}
	}
	return highestRound
}
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
}

func NewReplicationState(logger Logger, comm Communication, id NodeID, maxRoundWindow uint64, enabled bool) *ReplicationState {
	return &ReplicationState{
		logger:                           logger,
		enabled:                          enabled,
		comm:                             comm,
		id:                               id,
		maxRoundWindow:                   maxRoundWindow,
		receivedFinalizationCertificates: make(map[uint64]FinalizedBlock),
	}
}

func (r *ReplicationState) collectFutureFinalizationCertificates(fCert *FinalizationCertificate, currentRound uint64, nextSeqToCommit uint64) {
	if !r.enabled {
		return
	}
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

// maybeCollectFutureFinalizationCertificates attempts to collect future finalization certificates if
// there are more fCerts to be collected and the round has caught up.
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

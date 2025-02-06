// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"math"
	"slices"

	"go.uber.org/zap"
)

const MaxSeqToSend = 1024

type Request struct {
	FinalizationCertificateRequest *FinalizationCertificateRequest
}

type Response struct {
	FinalizationCertificateResponse *FinalizationCertificateResponse
}

type ReplicationState struct {
	logger Logger
	maxRoundWindow uint64
	comm Communication
	id NodeID

	// latest seq requested
	lastSequenceRequested uint64

	// highest sequence we have received a finalization certificate for
	highestFCertReceived *FinalizationCertificate
}

func NewReplicationState(logger Logger, comm Communication, id NodeID, maxRoundWindow uint64) *ReplicationState {
	return &ReplicationState{
		logger: logger,
		comm: comm,
		id: id,
		maxRoundWindow: maxRoundWindow,
	}
}

// request a finalization certificate for the given sequence number
type FinalizationCertificateRequest struct {
	Sequences []uint64
}

type SequenceData struct {
	Block Block
	FCert FinalizationCertificate
}

type FinalizationCertificateResponse struct {
	Data []SequenceData
}

// HandleRequest processes a request and returns a response. It also sends a response to the sender.
func (e *Epoch) HandleRequest(req *Request, from NodeID) *Response {
	// TODO: should I update requests to be async? and have the epoch respond with e.Comm.Send(msg, node)
	response := &Response{}
	if req.FinalizationCertificateRequest != nil {
		response.FinalizationCertificateResponse = e.handleFinalizationCertificateRequest(req.FinalizationCertificateRequest)
	}

	msg := &Message{Response: response}
	e.Comm.SendMessage(msg, from)
	return response
}

func (e *Epoch) handleFinalizationCertificateRequest(req *FinalizationCertificateRequest) *FinalizationCertificateResponse {
	e.Logger.Debug("Received finalization certificate request", zap.Int("num seqs", len(req.Sequences)))
	seqs := req.Sequences
	slices.Sort(seqs)
	data := make([]SequenceData, len(seqs))
	for i, seq := range seqs {
		block, fCert, exists := e.Storage.Retrieve(seq)
		if !exists {
			// since we are sorted, we can break early
			data = data[:i]
			break
		}
		data[i] = SequenceData{
			Block: block,
			FCert: fCert,
		}
	}
	return &FinalizationCertificateResponse{
		Data: data,
	}
}

func (e *Epoch) handleResponse(resp *Response, from NodeID) error {
	var err error
	if resp.FinalizationCertificateResponse != nil {
		err = e.handleFinalizationCertificateResponse(resp.FinalizationCertificateResponse, from)
	}
	return err
}

func (e *Epoch) handleFinalizationCertificateResponse(resp *FinalizationCertificateResponse, from NodeID) error {
	e.Logger.Debug("Received finalization certificate response", zap.String("from", from.String()), zap.Int("num seqs", len(resp.Data)))
	for _, data := range resp.Data {
		if e.round+e.maxRoundWindow < data.FCert.Finalization.Seq {
			e.Logger.Debug("Received finalization certificate for a round that is too far ahead", zap.Uint64("seq", data.FCert.Finalization.Seq))
			// we are too far behind, we should ignore this message
			continue
		}

		// ensure the finalization certificate we get relates to the block
		blockDigest := data.Block.BlockHeader().Digest
		if !bytes.Equal(blockDigest[:], data.FCert.Finalization.BlockHeader.Digest[:]) {
			e.Logger.Error("Finalization certificate does not match the block", zap.String("from", from.String()), zap.Uint64("seq", data.FCert.Finalization.Seq))
			continue
		}

		// if we already have a round object for this round, we should persist the fCert
		round, ok := e.rounds[data.Block.BlockHeader().Round]
		if !ok {
			e.storeFutureFinalizationResponse(data.FCert, data.Block, from)
			continue
		}
		if round.fCert != nil {
			// we should never be here because the round would have been deleted
			e.Logger.Error("Received finalization certificate for a round that already has a finalization certificate", zap.Uint64("round", data.FCert.Finalization.Seq))
			continue
		}
		err := e.persistFinalizationCertificate(data.FCert)
		if err != nil {
			e.Logger.Error("Failed to persist finalization certificate", zap.Error(err))
			continue
		}
	}

	// handle future messages in case we need to persist more fCerts from the future
	return e.maybeLoadFutureMessages(e.round)
}

func (e *Epoch) storeFutureFinalizationResponse(fCert FinalizationCertificate, block Block, from NodeID) {
	msg, ok := e.futureMessages[string(from)][block.BlockHeader().Round]
	if !ok {
		msgsForRound := &messagesForRound{}
		msgsForRound.proposal = &BlockMessage{
			Block: block,
		}
		msgsForRound.fCert = &fCert
		e.futureMessages[string(from)][block.BlockHeader().Round] = msgsForRound
		return
	}

	if msg.proposal != nil && !bytes.Equal(msg.proposal.Block.Bytes(), block.Bytes()) {
		e.Logger.Error("Proposal does not match the block in the finalization certificate response", zap.String("from", from.String()))
		return
	} else if msg.proposal == nil {
		msg.proposal = &BlockMessage{
			Block: block,
		}
	} else {
		msg.proposal.Block = block
	}
	msg.fCert = &fCert
}

// sendFutureCertficatesRequests sends requests for future finalization certificates for the
// range of sequences [start, end)
func (r *ReplicationState) sendFutureCertficatesRequests(start uint64, end uint64) {
	if r.lastSequenceRequested >= end {
		// no need to resend
		return
	}
	seqs := make([]uint64, end-start)
	for i := start; i < end; i++ {
		seqs[i-start] = i
	}
	// also request latest round in case this fCert is also behind
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
	r.sendFutureCertficatesRequests(uint64(startSeq), uint64(endSeq+1))
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
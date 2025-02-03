// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"slices"

	"go.uber.org/zap"
)

const MaxSeqToSend = 1024

type Request struct {
	FinalizationCertificateRequest *FinalizationCertificateRequest
	LatestRoundRequest             *LatestRoundRequest
}

type Response struct {
	FinalizationCertificateResponse *FinalizationCertificateResponse
	LatestRoundResponse             *LatestRoundResponse
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

type LatestRoundRequest struct {
}

// if block has not been proposed yet, we should return the last round's block, notarization, and fCert
type LatestRoundResponse struct {
	// last finalizaed block
	last SequenceData

	// current block
	Block        Block
	Notarization *Notarization
}

func (e *Epoch) HandleRequest(req *Request, from NodeID) *Response {
	// TODO: should I update requests to be async? and have the epoch respond with e.Comm.Send(msg, node)
	response := &Response{}
	if req.LatestRoundRequest != nil {
		response.LatestRoundResponse = e.handleLatestBlockRequest()
	}
	if req.FinalizationCertificateRequest != nil {
		response.FinalizationCertificateResponse = e.handleFinalizationCertificateRequest(req.FinalizationCertificateRequest)
	}

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

func (e *Epoch) handleLatestBlockRequest() *LatestRoundResponse {
	e.Logger.Debug("Received latest round request")
	resp := &LatestRoundResponse{}
	block, fCert, err := RetrieveLastFromStorage(e.Storage)
	if err != nil {
		return &LatestRoundResponse{}
	}
	resp.last = SequenceData{
		Block: block,
		FCert: fCert,
	}

	// grab the current round
	round, ok := e.rounds[e.round]
	if ok {
		resp.Block = round.block
		resp.Notarization = round.notarization
		return resp
	}

	// we have not proposed a block yet
	if e.round == 0 {
		return resp
	}

	// get the previous block and notarization
	round, ok = e.rounds[e.round-1]
	if ok {
		return resp
	}
	return &LatestRoundResponse{
		Block:        round.block,
		Notarization: round.notarization,
	}
}

func (e *Epoch) handleResponse(resp *Response, from NodeID) {
	if resp.LatestRoundResponse != nil {
		e.handleLatestRoundResponse(resp.LatestRoundResponse)
	}
	if resp.FinalizationCertificateResponse != nil {
		e.handleFinalizationCertificateResponse(resp.FinalizationCertificateResponse, from)
	}
}

func (e *Epoch) handleFinalizationCertificateResponse(resp *FinalizationCertificateResponse, from NodeID) error {
	e.Logger.Debug("Received finalization certificate response", zap.String("from", from.String()), zap.Int("num seqs", len(resp.Data)))
	// fmt.Printf("repsonse %+v\n", resp)
	for _, data := range resp.Data {
		if e.round+e.maxRoundWindow < data.FCert.Finalization.Seq {
			// we are too far behind, we should ignore this message
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
			continue
		}
		err := e.persistFinalizationCertificate(data.FCert)
		if err != nil {
			e.Logger.Error("Failed to persist finalization certificate", zap.Error(err))
			continue
		}
		if e.round == data.FCert.Finalization.Seq {
			e.Logger.Debug("Received finalization certificate for current round", zap.Uint64("round", e.round))
			e.increaseRound()
		}
	}

	// handle future messages in case we need to persist more fCerts from the future
	return e.maybeLoadFutureMessages(e.round)
}

func (e *Epoch) handleLatestRoundResponse(r *LatestRoundResponse) {
	
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

func (e *Epoch) sendFutureCertficatesRequests(start uint64, end uint64) {
	if e.lastSequenceRequested >= end {
		// no need to resend
		return
	}
	seqs := make([]uint64, end-start)
	for i := start; i < end; i++ {
		seqs[i-start] = i
	}

	// also request latest round in case this fCert is also behind
	roundRequest := &Request{
		LatestRoundRequest: &LatestRoundRequest{},
		FinalizationCertificateRequest: &FinalizationCertificateRequest{
			Sequences: seqs,
		},
	}
	msg := &Message{Request: roundRequest}
	
	// todo: find a better way to determine who to send to
	requestFrom := e.nodes[0]
	if e.lastBlock != nil {
		hash := sha256.Sum256(e.lastBlock.Bytes())
		num := binary.BigEndian.Uint64(hash[:8])
		requestFrom = e.nodes[num % uint64(len(e.nodes))]
	}
	e.lastSequenceRequested = end
	e.Comm.SendMessage(msg, requestFrom)
}


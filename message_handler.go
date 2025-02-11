// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"slices"

	"go.uber.org/zap"
)

type Request struct {
	FinalizationCertificateRequest *FinalizationCertificateRequest
}

type Response struct {
	FinalizationCertificateResponse *FinalizationCertificateResponse
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

		err := e.replicationState.StoreSequenceData(data)
		if err != nil {
			e.Logger.Error("Failed to store sequence data", zap.Error(err), zap.Uint64("seq", data.FCert.Finalization.Seq), zap.String("from", from.String()))
			continue
		}
	}

	e.processReplicationState()
	return nil
}

func (e *Epoch) processReplicationState() {
	// next sequence to commit
	nextSeqToCommit := e.Storage.Height()
	sequenceData, ok := e.replicationState.receivedFinalizationCertificates[nextSeqToCommit]
	if !ok {
		// we are missing the finalization certificate for the next sequence to commit
		return
	}

	// process block should not verify the block if its in e.round map
	e.processFinalizedBlock(&sequenceData)
}

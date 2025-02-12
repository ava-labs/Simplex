// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"slices"

	"go.uber.org/zap"
)

type ReplicationRequest struct {
	FinalizationCertificateRequest *FinalizationCertificateRequest
}

type ReplicationResponse struct {
	FinalizationCertificateResponse *FinalizationCertificateResponse
}

// request a finalization certificate for the given sequence number
type FinalizationCertificateRequest struct {
	Sequences []uint64
}

type FinalizedBlock struct {
	Block Block
	FCert FinalizationCertificate
}

type FinalizationCertificateResponse struct {
	Data []FinalizedBlock
}

// HandleRequest processes a request and returns a response. It also sends a response to the sender.
func (e *Epoch) HandleReplicationRequest(req *ReplicationRequest, from NodeID) *ReplicationResponse {
	// TODO: should I update requests to be async? and have the epoch respond with e.Comm.Send(msg, node)
	response := &ReplicationResponse{}
	if req.FinalizationCertificateRequest != nil {
		response.FinalizationCertificateResponse = e.handleFinalizationCertificateRequest(req.FinalizationCertificateRequest)
	}

	msg := &Message{ReplicationResponse: response}
	e.Comm.SendMessage(msg, from)
	return response
}

func (e *Epoch) handleFinalizationCertificateRequest(req *FinalizationCertificateRequest) *FinalizationCertificateResponse {
	e.Logger.Debug("Received finalization certificate request", zap.Int("num seqs", len(req.Sequences)))
	seqs := req.Sequences
	slices.Sort(seqs)
	data := make([]FinalizedBlock, len(seqs))
	for i, seq := range seqs {
		block, fCert, exists := e.Storage.Retrieve(seq)
		if !exists {
			// since we are sorted, we can break early
			data = data[:i]
			break
		}
		data[i] = FinalizedBlock{
			Block: block,
			FCert: fCert,
		}
	}
	return &FinalizationCertificateResponse{
		Data: data,
	}
}

func (e *Epoch) handleReplicationResponse(resp *ReplicationResponse, from NodeID) error {
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

		err := e.replicationState.StoreFinalizedBlock(data)
		if err != nil {
			e.Logger.Info("Failed to store sequence data", zap.Error(err), zap.Uint64("seq", data.FCert.Finalization.Seq), zap.String("from", from.String()))
			continue
		}
	}

	e.processReplicationState()
	return nil
}

func (e *Epoch) processReplicationState() {
	nextSeqToCommit := e.Storage.Height()
	finalizedBlock, ok := e.replicationState.receivedFinalizationCertificates[nextSeqToCommit]
	if !ok {
		return
	}

	e.replicationState.maybeCollectFutureFinalizationCertificates(e.round, e.Storage.Height())
	e.processFinalizedBlock(&finalizedBlock)
}

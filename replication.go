// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import "go.uber.org/zap"

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
	Seq uint64
}

type FinalizationCertificateResponse struct {
	FCert FinalizationCertificate
	Block Block
}

type LatestRoundRequest struct {
}

// if block has not been proposed yet, we should return the last round's block, notarization, and fCert
type LatestRoundResponse struct {
	Block        Block
	Notarization *Notarization
	FCert        *FinalizationCertificate
}

func (e *Epoch) HandleRequest(req Request) *Response {
	// TODO: should I update requests to be async? and have the epoch respond with e.Comm.Send(msg, node)
	response := &Response{}

	if req.FinalizationCertificateRequest != nil {
		response.FinalizationCertificateResponse = e.HandleFinalizationCertificateRequest(req.FinalizationCertificateRequest)
	}
	if req.LatestRoundRequest != nil {
		response.LatestRoundResponse = e.handleLatestBlockRequest()
	}

	return response
}

func (e *Epoch) HandleFinalizationCertificateRequest(req *FinalizationCertificateRequest) *FinalizationCertificateResponse {
	block, fCert, exists := e.Storage.Retrieve(req.Seq)
	if !exists {
		return nil
	}
	return &FinalizationCertificateResponse{
		FCert: fCert,
		Block: block,
	}
}

func (e *Epoch) handleLatestBlockRequest() *LatestRoundResponse {
	round, ok := e.rounds[e.round]
	// this means we have not proposed a block yet for this round
	if !ok {
		if e.round == 0 {
			return &LatestRoundResponse{}
		}
		// get the previous round from memory or storage
		round, ok := e.rounds[e.round-1]
		if ok {
			return &LatestRoundResponse{
				Block:        round.block,
				Notarization: round.notarization,
				FCert:        round.fCert,
			}
		}

		// get latest block
		block, fCert, err := RetrieveLastFromStorage(e.Storage)
		if err != nil {
			return &LatestRoundResponse{}
		}
		return &LatestRoundResponse{
			Block:        block,
			FCert:        &fCert,
			Notarization: nil,
		}
	}

	latest := &LatestRoundResponse{
		Block:        round.block,
		Notarization: round.notarization,
		FCert:        round.fCert,
	}
	return latest
}

func (e *Epoch) handleResponse(resp *Response, from NodeID) {
	if resp.FinalizationCertificateResponse != nil {
		e.HandleFinalizationCertificateResponse(resp.FinalizationCertificateResponse, from)
	}
	if resp.LatestRoundResponse != nil {
		e.HandleLatestRoundResponse(resp.LatestRoundResponse)
	}
}

func (e *Epoch) HandleFinalizationCertificateResponse(resp *FinalizationCertificateResponse, from NodeID) error {
	e.Logger.Debug("Received finalization certificate response", zap.String("from", from.String()), zap.Uint64("seq", resp.FCert.Finalization.Seq))
	// if its the next sequence to commit, we should commit it. continue commiting from fCertsMap if we have the next sequenece
	// otherwise we will add to the finalization certificate state the finalization message
	// we may have received a finalization certificate round in the past
	round, ok := e.rounds[resp.Block.BlockHeader().Round]
	if !ok {
		e.StoreFutureFinalizationResponse(resp, from)
		return nil
	}

	// we have already committed this round
	if round.fCert != nil {
		return nil
	}

	return e.persistFinalizationCertificate(resp.FCert)
}

func (e *Epoch) HandleLatestRoundResponse(resp *LatestRoundResponse) {
	// if we get a round later than ours, save it fCertReplicationState
	e.futureFCerts.ProcessLatestRoundResponse(resp)
}

func (e *Epoch) StoreFutureFinalizationResponse(resp *FinalizationCertificateResponse, from NodeID) {
	msg, ok := e.futureMessages[string(from)][resp.Block.BlockHeader().Round]
	if !ok {
		msgsForRound := &messagesForRound{}
		msgsForRound.proposal = &BlockMessage{
			Block: resp.Block,
			// TODO: do we need to add the vote as well?
		}
		e.futureMessages[string(from)][resp.Block.BlockHeader().Round] = msgsForRound
		return
	}

	msg.fCert = &resp.FCert
	// TODO: maybe good to sanity check rather than setting blindly
	msg.proposal.Block = resp.Block
}

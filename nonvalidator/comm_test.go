// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"bytes"
	"sync"
	"testing"

	"github.com/ava-labs/simplex/common"
)

// nonValidatorResponderComm implements common.Communication and is used during tests
// to create responses to any requests a node sends or broadcasts.
type nonValidatorResponderComm struct {
	t *testing.T

	// storage is used to create the responses
	storage common.Storage

	// nodes should contain the validator set of the highest epoch
	nodes common.Nodes

	// ID is the NodeID of the non-validator using this comm. Broadcasts
	// pick the first node in `nodes` that is not equal to ID as the
	// simulated responder.
	ID common.NodeID

	// responses is the queue of synthesized responses. Tests will pop messages
	// and feed entries into NonValidator.HandleMessage.
	responsesLock sync.Mutex
	responses     []*messageInfo
}

func newTestResponder(t *testing.T, myNodeID common.NodeID, tc *testChain) *nonValidatorResponderComm {
	return &nonValidatorResponderComm{
		nodes:   tc.nodes(),
		t:       t,
		ID:      myNodeID,
		storage: tc,
	}
}

func (r *nonValidatorResponderComm) Nodes() common.Nodes { return r.nodes }

// Enqueues a response coming from `destination`.
func (r *nonValidatorResponderComm) Send(msg *common.Message, destination common.NodeID) {
	r.handle(msg, destination)
}

// Enqueues responses coming from all other nodes in the network.
func (r *nonValidatorResponderComm) Broadcast(msg *common.Message) {

	for _, n := range r.nodes {
		if bytes.Equal(n.Id, r.ID) {
			continue
		}

		r.handle(msg, n.Id)
	}
}

func (r *nonValidatorResponderComm) handle(msg *common.Message, from common.NodeID) {
	switch {
	case msg.BlockDigestRequest != nil:
		r.respondToDigestRequest(msg.BlockDigestRequest, from)
	case msg.ReplicationRequest != nil:
		r.respondToReplicationRequest(msg.ReplicationRequest, from)
	}
}

func (r *nonValidatorResponderComm) clearResponses() {
	r.responsesLock.Lock()
	defer r.responsesLock.Unlock()
	r.responses = []*messageInfo{}
}

func (r *nonValidatorResponderComm) respondToDigestRequest(req *common.BlockDigestRequest, from common.NodeID) {
	block, fin, err := r.storage.Retrieve(req.Seq)
	if err != nil {
		return
	}

	// Empty digest should mean "send any block at this seq"
	if req.Digest != (common.Digest{}) && block.BlockHeader().Digest != req.Digest {
		return
	}

	resp := &common.ReplicationResponse{
		Data: []common.QuorumRound{{Block: block.(common.Block), Finalization: &fin}},
	}

	r.enqueue(&messageInfo{msg: &common.Message{ReplicationResponse: resp}, from: from})
}

func (r *nonValidatorResponderComm) respondToReplicationRequest(req *common.ReplicationRequest, from common.NodeID) {

	resp := &common.ReplicationResponse{}

	for _, seq := range req.Seqs {
		block, fin, err := r.storage.Retrieve(seq)
		if err == nil {
			resp.Data = append(resp.Data, common.QuorumRound{Block: block.(common.Block), Finalization: &fin})
		}
	}

	if req.LatestFinalizedSeq > 0 && r.storage.NumBlocks() > 0 {
		numBlocks := r.storage.NumBlocks()
		if req.LatestFinalizedSeq < numBlocks-1 {
			block, fin, err := r.storage.Retrieve(numBlocks - 1)
			if err == nil {
				resp.LatestSeq = &common.QuorumRound{Block: block.(common.Block), Finalization: &fin}
			}
		}
	}

	if len(resp.Data) == 0 && resp.LatestSeq == nil {
		return
	}

	r.enqueue(&messageInfo{msg: &common.Message{ReplicationResponse: resp}, from: from})
}

func (r *nonValidatorResponderComm) enqueue(m *messageInfo) {
	r.responsesLock.Lock()
	defer r.responsesLock.Unlock()
	r.responses = append(r.responses, m)
}

func (r *nonValidatorResponderComm) popResponse() (*messageInfo, bool) {
	r.responsesLock.Lock()
	defer r.responsesLock.Unlock()
	if len(r.responses) == 0 {
		return nil, false
	}
	m := r.responses[0]
	r.responses = r.responses[1:]
	return m, true
}

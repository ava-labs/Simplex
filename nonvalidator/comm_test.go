package nonvalidator

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/stretchr/testify/require"
)

type nonValidatorResponderComm struct {
	t       *testing.T
	storage *testChain
	nodes   simplex.Nodes

	// ID is the NodeID of the non-validator using this comm. Broadcasts
	// pick the first node in `nodes` that is not equal to ID as the
	// simulated responder.
	ID simplex.NodeID

	// responses is the queue of synthesized responses. Tests pop from
	// here and feed entries into NonValidator.HandleMessage.
	responses []*messageInfo
}

func newTestResponder(t *testing.T, nodes simplex.Nodes, myNodeID simplex.NodeID) *nonValidatorResponderComm {
	return &nonValidatorResponderComm{
		nodes: nodes,
		t:     t,
		ID:    myNodeID,
	}
}

func (r *nonValidatorResponderComm) Nodes() simplex.Nodes { return r.nodes }

// Send simulates `destination` answering the request — the enqueued response
// is tagged as coming from `destination`.
func (r *nonValidatorResponderComm) Send(msg *simplex.Message, destination simplex.NodeID) {
	r.handle(msg, destination)
}

// Broadcast simulates the first non-self node answering — the enqueued
// response is tagged as coming from that node.
func (r *nonValidatorResponderComm) Broadcast(msg *simplex.Message) {
	for _, n := range r.nodes {
		if !bytes.Equal(n.Node, r.ID) {
			r.handle(msg, n.Node)
		}
	}
}

func (r *nonValidatorResponderComm) handle(msg *simplex.Message, from simplex.NodeID) {
	switch {
	case msg.BlockDigestRequest != nil:
		r.respondToDigestRequest(msg.BlockDigestRequest, from)
	case msg.ReplicationRequest != nil:
		r.respondToReplicationRequest(msg.ReplicationRequest, from)
	}
}

func (r *nonValidatorResponderComm) respondToDigestRequest(req *simplex.BlockDigestRequest, from simplex.NodeID) {
	block, fin, err := r.storage.Retrieve(req.Seq)
	if err != nil {
		return
	}
	// Empty digest means "send any block at this seq" — see epoch_replicator.go.
	if req.Digest != (simplex.Digest{}) && block.BlockHeader().Digest != req.Digest {
		return
	}

	resp := &simplex.ReplicationResponse{
		Data: []simplex.QuorumRound{{Block: block.(simplex.Block), Finalization: &fin}},
	}
	r.enqueue(&messageInfo{msg: &simplex.Message{ReplicationResponse: resp}, from: from})
}

func (r *nonValidatorResponderComm) respondToReplicationRequest(req *simplex.ReplicationRequest, from simplex.NodeID) {
	resp := &simplex.ReplicationResponse{}

	for _, seq := range req.Seqs {
		block, fin, err := r.storage.Retrieve(seq)
		if err == nil {
			resp.Data = append(resp.Data, simplex.QuorumRound{Block: block.(simplex.Block), Finalization: &fin})
		}
	}

	if req.LatestFinalizedSeq > 0 && r.storage.NumBlocks() > 0 {
		numBlocks := r.storage.NumBlocks()

		if req.LatestFinalizedSeq < numBlocks-1 {
			block, fin, err := r.storage.Retrieve(numBlocks - 1)
			if err == nil {
				resp.LatestSeq = &simplex.QuorumRound{Block: block.(simplex.Block), Finalization: &fin}
			}
		}
	}

	if len(resp.Data) == 0 && resp.LatestSeq == nil {
		return
	}

	r.enqueue(&messageInfo{msg: &simplex.Message{ReplicationResponse: resp}, from: from})
}

func (r *nonValidatorResponderComm) enqueue(m *messageInfo) {
	r.responses = append(r.responses, m)
}

func (r *nonValidatorResponderComm) popResponse() (*messageInfo, bool) {
	if len(r.responses) == 0 {
		return nil, false
	}
	m := r.responses[0]
	r.responses = r.responses[1:]
	return m, true
}

// initializes the storage with sealing blocks at epochs, and normal blocks in between
func (r *nonValidatorResponderComm) initializeStorage(epochs ...uint64) {
	if len(epochs) < 1 || epochs[0] != 1 {
		r.t.Fatalf("Need to have the first epoch")
	}
	chain := newSeededChain(r.t, r.nodes, epochs[0])

	for _, epoch := range epochs[1:] {
		for chain.seq < epoch-1 {
			b := chain.appendBlock()
			finalization := simplex.NewFinalization()
			require.NoError(r.t, chain.Index(context.Background(), b, simplex.Finalization{}))
		}
		newNodes := append(r.nodes, simplex.Node{
			Node:   simplex.NodeID{byte(epoch)},
			Weight: 1,
		})
		sealing := chain.appendSealing(newNodes)
		require.NoError(r.t, chain.Index(context.Background(), sealing, simplex.Finalization{}))
		fmt.Println(chain.String())
	}

	r.storage = chain
}

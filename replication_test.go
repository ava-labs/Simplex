package simplex_test

import (
	"context"
	"simplex"
	"simplex/testutil"
	"simplex/wal"
	"time"

	"testing"

	"github.com/stretchr/testify/require"
)

// TestHandleLatestBlockRequest tests block requests return
// the latest block, notarization, and fCert.
func TestHandleLatestRoundRequest(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	quorum := simplex.Quorum(len(nodes))
	signatureAggregator := &testSignatureAggregator{}
	conf := simplex.EpochConfig{
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal.NewMemWAL(t),
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: signatureAggregator,
	}

	e, err := simplex.NewEpoch(conf)
	require.NoError(t, err)
	require.NoError(t, e.Start())
	block := <-bb.out
	time.Sleep(50 * time.Millisecond)
	req := &simplex.Request{LatestRoundRequest: &simplex.LatestRoundRequest{}}
	resp := e.HandleRequest(req, nodes[1])
	require.NotNil(t, resp.LatestRoundResponse)
	require.Equal(t, block, resp.LatestRoundResponse.Block)
	require.Nil(t, resp.LatestRoundResponse.Notarization)
	require.Equal(t, simplex.SequenceData{}, resp.LatestRoundResponse.LastIndex)

	// start at one since our node has already voted
	for i := 1; i < quorum; i++ {
		injectTestVote(t, e, block, nodes[i])
	}
	expectedNotarization, err := newNotarization(l, conf.SignatureAggregator, block, nodes[:quorum])
	require.NoError(t, err)

	// we will advance to a new round, however since no block proposed the request should
	// return the previous round
	req = &simplex.Request{LatestRoundRequest: &simplex.LatestRoundRequest{}}
	resp = e.HandleRequest(req, nodes[1])
	require.NotNil(t, resp.LatestRoundResponse)
	require.Equal(t, block, resp.LatestRoundResponse.Block)
	require.Equal(t, expectedNotarization, *resp.LatestRoundResponse.Notarization)
	require.Equal(t, simplex.SequenceData{}, resp.LatestRoundResponse.LastIndex)

	for i := 1; i < quorum; i++ {
		injectTestFinalization(t, e, block, nodes[i])
	}
	expectedFCert, _ := newFinalizationRecord(t, l, conf.SignatureAggregator, block, nodes[:quorum])

	req = &simplex.Request{LatestRoundRequest: &simplex.LatestRoundRequest{}}
	resp = e.HandleRequest(req, nodes[1])
	require.NotNil(t, resp.LatestRoundResponse)
	require.Equal(t, block, resp.LatestRoundResponse.Block)
	require.Equal(t, expectedNotarization, *resp.LatestRoundResponse.Notarization)
	require.Equal(t, expectedFCert, resp.LatestRoundResponse.LastIndex.FCert)
	require.Equal(t, block, resp.LatestRoundResponse.LastIndex.Block)

	// test that we can grab both requests
	finalizationRequest := &simplex.FinalizationCertificateRequest{
		Sequences: []uint64{1, 0},
	}
	// now propose a block and we should get a response with just a block for that round
	block2 := buildAndSendBlock(t, e, bb, nodes[1])
	req = &simplex.Request{
		LatestRoundRequest:             &simplex.LatestRoundRequest{},
		FinalizationCertificateRequest: finalizationRequest,
	}
	resp = e.HandleRequest(req, nodes[1])
	require.NotNil(t, resp.LatestRoundResponse)
	require.Equal(t, block2, resp.LatestRoundResponse.Block)
	require.Nil(t, resp.LatestRoundResponse.Notarization)
	require.Equal(t, simplex.SequenceData{}, resp.LatestRoundResponse.LastIndex)
	require.Equal(t, 1, len(resp.FinalizationCertificateResponse.Data))
	require.Equal(t, expectedFCert, resp.FinalizationCertificateResponse.Data[0].FCert)
	require.Equal(t, block, resp.FinalizationCertificateResponse.Data[0].Block)
}

func TestHandleFinalizationCertificateRequest(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	signatureAggregator := &testSignatureAggregator{}
	wal := wal.NewMemWAL(t)
	conf := simplex.EpochConfig{
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: signatureAggregator,
		BlockDeserializer:   &blockDeserializer{},
		QCDeserializer:      &testQCDeserializer{t: t},
	}

	seqs := createBlocks(t, nodes, bb, 10)
	for _, data := range seqs {
		conf.Storage.Index(data.Block, data.FCert)
	}
	e, err := simplex.NewEpoch(conf)
	require.NoError(t, err)
	require.NoError(t, e.Start())
	sequences := []uint64{0, 1, 2, 3}
	req := &simplex.Request{FinalizationCertificateRequest: &simplex.FinalizationCertificateRequest{
		Sequences: sequences,
	}}
	resp := e.HandleRequest(req, nodes[1])
	require.NotNil(t, resp.FinalizationCertificateResponse)
	require.Equal(t, len(sequences), len(resp.FinalizationCertificateResponse.Data))
	for i, data := range resp.FinalizationCertificateResponse.Data {
		require.Equal(t, seqs[i].FCert, data.FCert)
		require.Equal(t, seqs[i].Block, data.Block)
	}

	// request out of scope
	req = &simplex.Request{FinalizationCertificateRequest: &simplex.FinalizationCertificateRequest{
		Sequences: []uint64{11, 12, 13},
	}}
	resp = e.HandleRequest(req, nodes[1])
	require.Zero(t, len(resp.FinalizationCertificateResponse.Data))
}

func TestReplication(t *testing.T) {
	bb := newTestControlledBlockBuilder()
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	net := newInMemNetwork(t, nodes)
	startSeq := uint64(8)

	// initiate a network with 4 nodes. one node is behind by 8 blocks
	storageData := createBlocks(t, nodes, &bb.testBlockBuilder, startSeq)
	normalNode1 := newSimplexNodeWithStorage(t, nodes[0], net, bb, storageData)
	normalNode2 := newSimplexNodeWithStorage(t, nodes[1], net, bb, storageData)
	normalNode3 := newSimplexNodeWithStorage(t, nodes[2], net, bb, storageData)
	laggingNode := newSimplexNode(t, nodes[3], net, bb)

	require.Equal(t, startSeq, normalNode1.storage.Height())
	require.Equal(t, startSeq, normalNode2.storage.Height())
	require.Equal(t, startSeq, normalNode3.storage.Height())
	require.Equal(t, uint64(0), laggingNode.storage.Height())

	net.startInstances()
	bb.triggerNewBlock()

	// all blocks except the lagging node start at round 8, seq 8.
	// lagging node starts at round 0, seq 0.
	// this asserts that the lagging node catches up to the latest round
	for _, n := range net.instances {
		n.storage.waitForBlockCommit(uint64(startSeq))
	}
}

func TestReplicationExceedsMaxRoundWindow(t *testing.T) {
	bb := newTestControlledBlockBuilder()
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	net := newInMemNetwork(t, nodes)
	startSeq := uint64(simplex.DefaultMaxRoundWindow + 3)

	storageData := createBlocks(t, nodes, &bb.testBlockBuilder, startSeq)
	normalNode1 := newSimplexNodeWithStorage(t, nodes[0], net, bb, storageData)
	normalNode2 := newSimplexNodeWithStorage(t, nodes[1], net, bb, storageData)
	normalNode3 := newSimplexNodeWithStorage(t, nodes[2], net, bb, storageData)
	laggingNode := newSimplexNode(t, nodes[3], net, bb)
	require.Equal(t, startSeq, normalNode1.storage.Height())
	require.Equal(t, startSeq, normalNode2.storage.Height())
	require.Equal(t, startSeq, normalNode3.storage.Height())
	require.Equal(t, uint64(0), laggingNode.storage.Height())

	net.startInstances()
	bb.triggerNewBlock()
	for _, n := range net.instances {
		n.storage.waitForBlockCommit(uint64(startSeq))
	}
}

// our node has a couple of blocks and notarizations causing us to increase our round
// then when we receive a fCert in the future, we should ensure those rounds are persisted
func TestReplicationStartsBeforeCurrentRound(t *testing.T) {
	bb := newTestControlledBlockBuilder()
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	net := newInMemNetwork(t, nodes)
	startSeq := uint64(simplex.DefaultMaxRoundWindow + 3)

	storageData := createBlocks(t, nodes, &bb.testBlockBuilder, startSeq)
	// wal := wal.NewMemWAL(t)
	// write 2 blocks and 2 notarizations


	normalNode1 := newSimplexNodeWithStorage(t, nodes[0], net, bb, storageData)
	normalNode2 := newSimplexNodeWithStorage(t, nodes[1], net, bb, storageData)
	normalNode3 := newSimplexNodeWithStorage(t, nodes[2], net, bb, storageData)
	laggingNode := newSimplexNode(t, nodes[3], net, bb)

	require.Equal(t, startSeq, normalNode1.storage.Height())
	require.Equal(t, startSeq, normalNode2.storage.Height())
	require.Equal(t, startSeq, normalNode3.storage.Height())
	require.Equal(t, uint64(0), laggingNode.storage.Height())

	net.startInstances()
	bb.triggerNewBlock()
	for _, n := range net.instances {
		n.storage.waitForBlockCommit(uint64(startSeq))
	}



}

// // We replicate to the latest round(which doesn't have a fCert)
// // ensuring that we have the latest block and notarization
// func TestReplicationLatestRoundNotarized() {

// }

// // We get an FCert from the future, but it is actually not the global latest
// // so we should realize that during the replication process and replicate to the latest
// func TestReplicationMultipleBehind() {

// }

func buildAndSendBlock(t *testing.T, e *simplex.Epoch, bb *testBlockBuilder, from simplex.NodeID) *testBlock {
	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	block := <-bb.out
	vote, err := newTestVote(block, from)
	require.NoError(t, err)
	err = e.HandleMessage(&simplex.Message{
		BlockMessage: &simplex.BlockMessage{
			Vote:  vote,
			Block: block,
		},
	}, from)
	require.NoError(t, err)

	// wait for the block to be processed
	time.Sleep(50 * time.Millisecond)
	return block
}

func createBlocks(t *testing.T, nodes []simplex.NodeID, bb simplex.BlockBuilder, seqCount uint64) []simplex.SequenceData {
	logger := testutil.MakeLogger(t, int(0))
	ctx := context.Background()
	data := make([]simplex.SequenceData, 0, seqCount)
	var prev simplex.Digest
	for i := uint64(0); i < seqCount; i++ {
		protocolMetadata := simplex.ProtocolMetadata{
			Seq:   i,
			Round: i,
			Prev:  prev,
		}

		block, ok := bb.BuildBlock(ctx, protocolMetadata)
		require.True(t, ok)
		prev = block.BlockHeader().Digest
		fCert, _ := newFinalizationRecord(t, logger, &testSignatureAggregator{}, block, nodes)
		data = append(data, simplex.SequenceData{
			Block: block,
			FCert: fCert,
		})
	}
	return data
}

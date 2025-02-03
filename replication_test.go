package simplex_test

import (
	"context"
	"fmt"
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
	require.Nil(t, resp.LatestRoundResponse.FCert)

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
	require.Nil(t, resp.LatestRoundResponse.FCert)

	for i := 1; i < quorum; i++ {
		injectTestFinalization(t, e, block, nodes[i])
	}
	expectedFCert, _ := newFinalizationRecord(t, l, conf.SignatureAggregator, block, nodes[:quorum])

	req = &simplex.Request{LatestRoundRequest: &simplex.LatestRoundRequest{}}
	resp = e.HandleRequest(req, nodes[1])
	require.NotNil(t, resp.LatestRoundResponse)
	require.Equal(t, block, resp.LatestRoundResponse.Block)
	require.Equal(t, expectedNotarization, *resp.LatestRoundResponse.Notarization)
	require.Equal(t, expectedFCert, *resp.LatestRoundResponse.FCert)

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
	require.Nil(t, resp.LatestRoundResponse.FCert)
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

	storage, seqs := newStorage(t, l, nodes, bb, 10)
	conf.Storage = storage
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
	l := testutil.MakeLogger(t, int(0))
	bb := &testBlockBuilder{}
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	net := newInMemNetwork(nodes)
	startSeq := uint64(10)

	// initiate a network with 3 nodes. one node is behind by 10 blocks
	storage, _ := newStorage(t, l, nodes, bb, startSeq)
	normalNode1 := newSimplexNodeWithStorage(t, nodes[0], net, bb, storage)
	normalNode2 := newSimplexNodeWithStorage(t, nodes[1], net, bb, storage)
	normalNode3 := newSimplexNodeWithStorage(t, nodes[2], net, bb, storage)
	laggingNode := newSimplexNode(t, nodes[3], net, bb)

	net.addInstances(t, normalNode1, normalNode2, normalNode3, laggingNode)
	net.startInstances()
	require.Equal(t, uint64(10), normalNode1.storage.Height())
	require.Equal(t, uint64(10), normalNode2.storage.Height())
	require.Equal(t, uint64(0), laggingNode.storage.Height())
	
	
	// propose the block from the leader
	leader := net.getLeader(startSeq)
	block := leader.proposeBlock()
	time.Sleep(100 * time.Millisecond)
	normalNode1.voteOnBlock(block)
	normalNode2.voteOnBlock(block)
	time.Sleep(500 * time.Millisecond)
	// normal votes send finalization
	normalNode1.sendFinalization(block)
	normalNode2.sendFinalization(block)
	normalNode3.sendFinalization(block)
	time.Sleep(500 * time.Millisecond)

	fmt.Println("normalNode1.storage.Height()", normalNode1.storage.Height())
	fmt.Println("normalNode2.storage.Height()", normalNode2.storage.Height())
	fmt.Println("normalNode3.storage.Height()", normalNode3.storage.Height())
	fmt.Println("laggingNode.storage.Height()", laggingNode.storage.Height())
	
}

// func newReplicationResponses(b *testRoundBuilder, initialMetadata simplex.ProtocolMetadata, seqs uint64) map[uint64]*simplex.FinalizationCertificateResponse {
// 	responses := make(map[uint64]*simplex.FinalizationCertificateResponse, seqs)
// 	md := initialMetadata
// 	for seq := initialMetadata.Seq; seq < initialMetadata.Seq+seqs; seq++ {
// 		// generate the block, fCert, and response
// 		block := newTestBlock(md)
// 		fCert, _ := b.newFinalizationCertificateAndRecord(block)
// 		responses[seq] = &simplex.FinalizationCertificateResponse{
// 			Block: block,
// 			FCert: fCert,
// 		}

// 		// update the metadata
// 		md.Round++
// 		md.Seq++
// 		md.Prev = block.BlockHeader().Digest
// 	}
// 	return responses

// }

type testRoundBuilder struct {
	t *testing.T
	l simplex.Logger

	// for generation
	sigAgg simplex.SignatureAggregator
	nodes  []simplex.NodeID
	signer simplex.Signer
}

func newTestRoundBuilder(t *testing.T, eConf simplex.EpochConfig) *testRoundBuilder {
	return &testRoundBuilder{
		t:      t,
		l:      eConf.Logger,
		sigAgg: eConf.SignatureAggregator,
		nodes:  eConf.Comm.ListNodes(),
		signer: eConf.Signer,
	}
}

func (b *testRoundBuilder) newFinalizationCertificateAndRecord(block simplex.Block) (simplex.FinalizationCertificate, []byte) {
	return newFinalizationRecord(b.t, b.l, b.sigAgg, block, b.nodes)
}

// func TestReplicationExceedsMaxRoundWindow() {

// }

// // our node has a couple of blocks and notarizations causing us to increase our round
// // then when we receive a fCert in the future, we should ensure those rounds are persisted
// func TestReplicationStartsBeforeCurrentRound() {

// }

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
			Vote:  *vote,
			Block: block,
		},
	}, from)
	require.NoError(t, err)

	// wait for the block to be processed
	time.Sleep(50 * time.Millisecond)
	return block
}

func newStorage(t *testing.T, logger simplex.Logger, nodes []simplex.NodeID, bb *testBlockBuilder, seqs uint64) (*InMemStorage, []simplex.SequenceData) {	
	ctx := context.Background()
	protocolMetadata := simplex.ProtocolMetadata{}
	data := make([]simplex.SequenceData, 0, seqs)
	storage := newInMemStorage()
	for i := uint64(0); i < seqs; i++ {
		block, ok := bb.BuildBlock(ctx, protocolMetadata)
		require.True(t, ok)
		fCert, _ := newFinalizationRecord(t, logger, &testSignatureAggregator{}, block, nodes)
		storage.Index(block, fCert)

		data = append(data, simplex.SequenceData{
			Block: block,
			FCert: fCert,
		})
		protocolMetadata.Seq++
		protocolMetadata.Round++
		protocolMetadata.Prev = block.BlockHeader().Digest
		fmt.Printf("block digest %+v \n", block.BlockHeader().ProtocolMetadata)
	}

	return storage, data
}
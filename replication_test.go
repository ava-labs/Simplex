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
	req := simplex.Request{LatestRoundRequest: &simplex.LatestRoundRequest{}}
	resp := e.HandleRequest(req)
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
	req = simplex.Request{LatestRoundRequest: &simplex.LatestRoundRequest{}}
	resp = e.HandleRequest(req)
	require.NotNil(t, resp.LatestRoundResponse)
	require.Equal(t, block, resp.LatestRoundResponse.Block)
	require.Equal(t, expectedNotarization, *resp.LatestRoundResponse.Notarization)
	require.Nil(t, resp.LatestRoundResponse.FCert)

	for i := 1; i < quorum; i++ {
		injectTestFinalization(t, e, block, nodes[i])
	}
	expectedFCert, _ := newFinalizationRecord(t, l, conf.SignatureAggregator, block, nodes[:quorum])

	req = simplex.Request{LatestRoundRequest: &simplex.LatestRoundRequest{}}
	resp = e.HandleRequest(req)
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
	req = simplex.Request{
		LatestRoundRequest:             &simplex.LatestRoundRequest{},
		FinalizationCertificateRequest: finalizationRequest,
	}
	resp = e.HandleRequest(req)
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
	ctx := context.Background()
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

	storage, seqs := newStorage(t, ctx, conf, bb, 10)
	conf.Storage = storage
	e, err := simplex.NewEpoch(conf)
	require.NoError(t, err)
	require.NoError(t, e.Start())
	sequences := []uint64{0, 1, 2, 3}
	req := simplex.Request{FinalizationCertificateRequest: &simplex.FinalizationCertificateRequest{
		Sequences: sequences,
	}}
	resp := e.HandleRequest(req)
	require.NotNil(t, resp.FinalizationCertificateResponse)
	require.Equal(t, len(sequences), len(resp.FinalizationCertificateResponse.Data))
	for i, data := range resp.FinalizationCertificateResponse.Data {
		require.Equal(t, seqs[i].FCert, data.FCert)
		require.Equal(t, seqs[i].Block, data.Block)
	}

	// request out of scope
	req = simplex.Request{FinalizationCertificateRequest: &simplex.FinalizationCertificateRequest{
		Sequences: []uint64{11, 12, 13},
	}}
	resp = e.HandleRequest(req)
	require.Zero(t, len(resp.FinalizationCertificateResponse.Data))
}

func TestReplication(t *testing.T) {
	bb := &testBlockBuilder{}
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	net := newInMemNetwork(nodes)
	startSeq := uint64(10)
	normalNode := newSimplexNodeWithStorage(t, nodes[0], net, bb, startSeq)
	normalNode2 := newSimplexNodeWithStorage(t, nodes[1], net, bb, startSeq)
	normalNode3 := newSimplexNodeWithStorage(t, nodes[2], net, bb, startSeq)
	laggingNode := newSimplexNode(t, nodes[3], net, bb)

	net.addInstances(t, normalNode, normalNode2, normalNode3, laggingNode)
	net.startInstances()
	require.Equal(t, uint64(10), normalNode.storage.Height())
	require.Equal(t, uint64(10), normalNode2.storage.Height())
	require.Equal(t, uint64(0), laggingNode.storage.Height())
	
	fmt.Println("leader: ", string(simplex.LeaderForRound(nodes, 10)))
	fmt.Println("proposing block from normal node")
	block := normalNode3.proposeBlock()
	time.Sleep(500 * time.Millisecond)
	fmt.Println("voting on block from normal node")
	normalNode.voteOnBlock(block)
	normalNode2.voteOnBlock(block)
	time.Sleep(500 * time.Millisecond)

	// now the normal node sends fCert for round 10
	// and the lagging node should replicate up to round 10

	// fCert, _ := newFinalizationRecord(t, normalNode.e.EpochConfig.Logger, normalNode.e.EpochConfig.SignatureAggregator, <-bb.out, nodeIds)
	// err := normalNode.e.HandleMessage(&simplex.Message{
	// 	FinalizationCertificate: &fCert,
	// }, nodeIds[1])
	// require.NoError(t, err)





	// seqs := setStorage(t, ctx, conf, bb, 10)
	// normalNode := newSimplexNode(t, net.nodes[1], &net, bb)
	// laggingNode := newSimplexNode(t, net.nodes[0], &net, bb, nil)
	
	// normalNode.start()

	// e, err := simplex.NewEpoch(conf)
	// roundBuilder := newTestRoundBuilder(t, conf)
	// initialMetadata := e.Metadata()
	// responses := newReplicationResponses(roundBuilder, initialMetadata, 5)
	// comm.e = e
	// require.NoError(t, err)
	// require.NoError(t, e.Start())

	// // // broadcast the last finalization certificate
	// e.HandleMessage(&simplex.Message{
	// 	FinalizationCertificate: &responses[initialMetadata.Seq+4].FCert,
	// }, nodes[1])
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

func newStorage(t *testing.T, ctx context.Context, eConf simplex.EpochConfig, bb *testBlockBuilder, seqs uint64) (*InMemStorage, []simplex.SequenceData) {	
	protocolMetadata := simplex.ProtocolMetadata{}
	data := make([]simplex.SequenceData, 0, seqs)
	storage := newInMemStorage()
	for i := uint64(0); i < seqs; i++ {
		block, ok := bb.BuildBlock(ctx, protocolMetadata)
		require.True(t, ok)
		fCert, _ := newFinalizationRecord(t, eConf.Logger, eConf.SignatureAggregator, block, eConf.Comm.ListNodes())
		storage.Index(block, fCert)

		data = append(data, simplex.SequenceData{
			Block: block,
			FCert: fCert,
		})
		protocolMetadata.Seq++
		protocolMetadata.Round++
		protocolMetadata.Prev = block.BlockHeader().Digest
	}

	return storage, data
}
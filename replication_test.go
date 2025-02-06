package simplex_test

import (
	"context"
	"simplex"
	"simplex/testutil"
	"simplex/wal"

	"testing"

	"github.com/stretchr/testify/require"
)

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

// TestReplication tests the replication process of a node that
// is behind the rest of the network by less than maxRoundWindow.
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

// TestReplicationExceedsMaxRoundWindow tests the replication process of a node that
// is behind the rest of the network by more than maxRoundWindow.
func TestReplicationExceedsMaxRoundWindow(t *testing.T) {
	bb := newTestControlledBlockBuilder()
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	net := newInMemNetwork(t, nodes)
	startSeq := uint64(simplex.DefaultMaxRoundWindow * 3)

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

// TestReplicationStartsBeforeCurrentRound tests the replication process of a node that
// starts replicating in the middle of the current round.
func TestReplicationStartsBeforeCurrentRound(t *testing.T) {
	bb := newTestControlledBlockBuilder()
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	quorum := simplex.Quorum(len(nodes))
	net := newInMemNetwork(t, nodes)
	startSeq := uint64(simplex.DefaultMaxRoundWindow + 3)
	storageData := createBlocks(t, nodes, &bb.testBlockBuilder, startSeq)

	normalNode1 := newSimplexNodeWithStorage(t, nodes[0], net, bb, storageData)
	normalNode2 := newSimplexNodeWithStorage(t, nodes[1], net, bb, storageData)
	normalNode3 := newSimplexNodeWithStorage(t, nodes[2], net, bb, storageData)
	laggingNode := newSimplexNode(t, nodes[3], net, bb)

	firstBlock := storageData[0].Block
	record := simplex.BlockRecord(firstBlock.BlockHeader(), firstBlock.Bytes())
	laggingNode.wal.Append(record)

	firstNotarizationRecord, err := newNotarizationRecord(laggingNode.e.Logger, laggingNode.e.SignatureAggregator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	laggingNode.wal.Append(firstNotarizationRecord)

	secondBlock := storageData[1].Block
	record = simplex.BlockRecord(secondBlock.BlockHeader(), secondBlock.Bytes())
	laggingNode.wal.Append(record)

	secondNotarizationRecord, err := newNotarizationRecord(laggingNode.e.Logger, laggingNode.e.SignatureAggregator, secondBlock, nodes[0:quorum])
	require.NoError(t, err)
	laggingNode.wal.Append(secondNotarizationRecord)

	require.Equal(t, startSeq, normalNode1.storage.Height())
	require.Equal(t, startSeq, normalNode2.storage.Height())
	require.Equal(t, startSeq, normalNode3.storage.Height())
	require.Equal(t, uint64(0), laggingNode.storage.Height())

	net.startInstances()

	laggingNodeMd := laggingNode.e.Metadata()
	require.Equal(t, uint64(2), laggingNodeMd.Round)

	bb.triggerNewBlock()
	for _, n := range net.instances {
		n.storage.waitForBlockCommit(uint64(startSeq))
	}
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

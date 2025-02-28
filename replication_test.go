// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"bytes"
	"context"
	"fmt"
	"simplex"
	"simplex/testutil"
	"simplex/wal"
	"time"

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
		ReplicationEnabled:  true,
	}

	seqs := createBlocks(t, nodes, bb, 10)
	for _, data := range seqs {
		conf.Storage.Index(data.Block, data.FCert)
	}
	e, err := simplex.NewEpoch(conf)
	require.NoError(t, err)
	require.NoError(t, e.Start())
	sequences := []uint64{0, 1, 2, 3}
	req := &simplex.ReplicationRequest{FinalizationCertificateRequest: &simplex.FinalizationCertificateRequest{
		Sequences: sequences,
	}}
	resp, err := e.HandleReplicationRequest(req, nodes[1])
	require.NoError(t, err)
	require.NotNil(t, resp.FinalizationCertificateResponse)
	require.Equal(t, len(sequences), len(resp.FinalizationCertificateResponse.Data))
	for i, data := range resp.FinalizationCertificateResponse.Data {
		require.Equal(t, seqs[i].FCert, data.FCert)
		require.Equal(t, seqs[i].Block, data.Block)
	}

	// request out of scope
	req = &simplex.ReplicationRequest{FinalizationCertificateRequest: &simplex.FinalizationCertificateRequest{
		Sequences: []uint64{11, 12, 13},
	}}
	resp, err = e.HandleReplicationRequest(req, nodes[1])
	require.NoError(t, err)
	require.Zero(t, len(resp.FinalizationCertificateResponse.Data))
}

// TestNotarizationRequestBasic tests notarization requests for blocks and notarizations.
func TestNotarizationRequestBasic(t *testing.T) {
	// generate 5 blocks & notarizations
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	wal := wal.NewMemWAL(t)
	conf := defaultTestNodeEpochConfig(t, nodes[0], noopComm(nodes), wal, bb, true)

	e, err := simplex.NewEpoch(conf)
	require.NoError(t, err)
	require.NoError(t, e.Start())

	blocks := make(map[int]simplex.NotarizedBlock)
	for i := 0; i < 5; i++ {
		block, notarization := advanceRound(t, e, bb, true, false)

		blocks[i] = simplex.NotarizedBlock{
			Block: block,
			Notarization: notarization,
		}
	}

	require.Equal(t, uint64(5), e.Metadata().Round)

	req := &simplex.ReplicationRequest{
		NotarizationRequest: &simplex.NotarizationRequest{
			StartRound: 0,
		},
	}
	resp, err := e.HandleReplicationRequest(req, nodes[1])
	require.NoError(t, err)
	require.NotNil(t, resp.NotarizationResponse)
	require.Nil(t, resp.FinalizationCertificateResponse)

	for _, round := range resp.NotarizationResponse.Data {
		require.Nil(t, round.EmptyNotarization)
		notarizedBlock, ok := blocks[int(round.Block.BlockHeader().Round)]
		require.True(t, ok)
		require.Equal(t, notarizedBlock.Block, round.Block)
		require.Equal(t, notarizedBlock.Notarization, round.Notarization)
	}	
}

// TestNotarizationRequestMixed ensures the notarization response also includes empty notarizations
// No empty notarizations. Within the maxRoundLimit
func TestNotarizationRequestMixed(t *testing.T) {
	// generate 5 blocks & notarizations
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	wal := wal.NewMemWAL(t)
	conf := defaultTestNodeEpochConfig(t, nodes[0], noopComm(nodes), wal, bb, true)

	e, err := simplex.NewEpoch(conf)
	require.NoError(t, err)
	require.NoError(t, e.Start())

	blocks := make(map[int]simplex.NotarizedBlock)
	for i := 0; i < 8; i++ {
		leaderForRound := bytes.Equal(simplex.LeaderForRound(nodes, uint64(i)), e.ID)
		emptyBlock := !leaderForRound
		if emptyBlock {
			emptyNotarization := newEmptyNotarization(t, e, uint64(i))
			e.HandleMessage(&simplex.Message{
				EmptyNotarization: emptyNotarization,
			}, nodes[1])

			time.Sleep(50 * time.Millisecond)
			blocks[i] = simplex.NotarizedBlock{
				EmptyNotarization: emptyNotarization,
			}
			continue
		}
		block, notarization := advanceRound(t, e, bb, true, false)

		blocks[i] = simplex.NotarizedBlock{
			Block: block,
			Notarization: notarization,
		}
	}

	require.Equal(t, uint64(8), e.Metadata().Round)

	req := &simplex.ReplicationRequest{
		NotarizationRequest: &simplex.NotarizationRequest{
			StartRound: 0,
		},
	}
	resp, err := e.HandleReplicationRequest(req, nodes[1])
	require.NoError(t, err)
	require.NotNil(t, resp.NotarizationResponse)
	require.Nil(t, resp.FinalizationCertificateResponse)

	for _, round := range resp.NotarizationResponse.Data {
		notarizedBlock, ok := blocks[int(round.GetRound())]
		require.True(t, ok)
		require.Equal(t, notarizedBlock.Block, round.Block)
		require.Equal(t, notarizedBlock.Notarization, round.Notarization)
		require.Equal(t, notarizedBlock.EmptyNotarization, round.EmptyNotarization)
	}
}


// TestNotarizationRequestBehind tests notarization requests when the requested start round
// is behind the storage height.
func TestNotarizationRequestBehind(t *testing.T) {
	// generate 5 blocks & notarizations
	bb := &testBlockBuilder{}
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	wal := wal.NewMemWAL(t)
	finalizedBlocks := createBlocks(t, nodes, bb, 4)
	conf := defaultTestNodeEpochConfig(t, nodes[0], noopComm(nodes), wal, bb, true)
	for _, data := range finalizedBlocks {
		conf.Storage.Index(data.Block, data.FCert)
	}

	bb.out = make(chan *testBlock, 1)
	e, err := simplex.NewEpoch(conf)
	require.NoError(t, err)
	require.NoError(t, e.Start())

	require.Equal(t, uint64(4), e.Metadata().Round)
	blocks := make(map[int]simplex.NotarizedBlock)
	for range 5 {
		block, notarization := advanceRound(t, e, bb, true, false)

		blocks[int(block.BlockHeader().Round)] = simplex.NotarizedBlock{
			Block: block,
			Notarization: notarization,
		}
	}

	require.Equal(t, uint64(5 + 4), e.Metadata().Round)

	req := &simplex.ReplicationRequest{
		NotarizationRequest: &simplex.NotarizationRequest{
			StartRound: 0,
		},
	}
	resp, err := e.HandleReplicationRequest(req, nodes[1])
	require.NoError(t, err)
	require.NotNil(t, resp.NotarizationResponse)
	require.Nil(t, resp.FinalizationCertificateResponse)
	require.Equal(t, 5, len(resp.NotarizationResponse.Data))

	for _, round := range resp.NotarizationResponse.Data {
		require.Nil(t, round.EmptyNotarization)
		notarizedBlock, ok := blocks[int(round.Block.BlockHeader().Round)]
		require.True(t, ok)
		require.Equal(t, notarizedBlock.Block, round.Block)
		require.Equal(t, notarizedBlock.Notarization, round.Notarization)
	}	
}

// TestReplication tests the replication process of a node that
// is behind the rest of the network by less than maxRoundWindow.
func TestReplication(t *testing.T) {
	bb := newTestControlledBlockBuilder(t)
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	net := newInMemNetwork(t, nodes)
	startSeq := uint64(8)

	// initiate a network with 4 nodes. one node is behind by 8 blocks
	storageData := createBlocks(t, nodes, &bb.testBlockBuilder, startSeq)
	normalNode1 := newSimplexNodeWithStorage(t, nodes[0], net, bb, storageData)
	normalNode2 := newSimplexNodeWithStorage(t, nodes[1], net, bb, storageData)
	normalNode3 := newSimplexNodeWithStorage(t, nodes[2], net, bb, storageData)
	laggingNode := newSimplexNode(t, nodes[3], net, bb, true)

	require.Equal(t, startSeq, normalNode1.storage.Height())
	require.Equal(t, startSeq, normalNode2.storage.Height())
	require.Equal(t, startSeq, normalNode3.storage.Height())
	require.Equal(t, uint64(0), laggingNode.storage.Height())

	net.startInstances()
	bb.triggerNewBlock()

	// all blocks except the lagging node start at round 8, seq 8.
	// lagging node starts at round 0, seq 0.
	// this asserts that the lagging node catches up to the latest round
	for i := 0; i <= int(startSeq); i++ {
		for _, n := range net.instances {
			n.storage.waitForBlockCommit(uint64(startSeq))
		}
	}
}

// TestReplicationExceedsMaxRoundWindow tests the replication process of a node that
// is behind the rest of the network by more than maxRoundWindow.
func TestReplicationExceedsMaxRoundWindow(t *testing.T) {
	bb := newTestControlledBlockBuilder(t)
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	net := newInMemNetwork(t, nodes)
	startSeq := uint64(simplex.DefaultMaxRoundWindow * 3)

	storageData := createBlocks(t, nodes, &bb.testBlockBuilder, startSeq)
	normalNode1 := newSimplexNodeWithStorage(t, nodes[0], net, bb, storageData)
	normalNode2 := newSimplexNodeWithStorage(t, nodes[1], net, bb, storageData)
	normalNode3 := newSimplexNodeWithStorage(t, nodes[2], net, bb, storageData)
	laggingNode := newSimplexNode(t, nodes[3], net, bb, true)
	require.Equal(t, startSeq, normalNode1.storage.Height())
	require.Equal(t, startSeq, normalNode2.storage.Height())
	require.Equal(t, startSeq, normalNode3.storage.Height())
	require.Equal(t, uint64(0), laggingNode.storage.Height())

	net.startInstances()
	bb.triggerNewBlock()
	for i := 0; i <= int(startSeq); i++ {
		for _, n := range net.instances {
			n.storage.waitForBlockCommit(uint64(startSeq))
		}
	}
}

// TestReplicationStartsBeforeCurrentRound tests the replication process of a node that
// starts replicating in the middle of the current round.
func TestReplicationStartsBeforeCurrentRound(t *testing.T) {
	bb := newTestControlledBlockBuilder(t)
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}
	quorum := simplex.Quorum(len(nodes))
	net := newInMemNetwork(t, nodes)
	startSeq := uint64(simplex.DefaultMaxRoundWindow + 3)
	storageData := createBlocks(t, nodes, &bb.testBlockBuilder, startSeq)

	normalNode1 := newSimplexNodeWithStorage(t, nodes[0], net, bb, storageData)
	normalNode2 := newSimplexNodeWithStorage(t, nodes[1], net, bb, storageData)
	normalNode3 := newSimplexNodeWithStorage(t, nodes[2], net, bb, storageData)
	laggingNode := newSimplexNode(t, nodes[3], net, bb, true)

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
	for i := 0; i <= int(startSeq); i++ {
		for _, n := range net.instances {
			n.storage.waitForBlockCommit(uint64(startSeq))
		}
	}
}

func TestReplicationFutureFinalizationCertificate(t *testing.T) {
	// send a block, then simultaneously send a finalization certificate for the block
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	quorum := simplex.Quorum(len(nodes))
	signatureAggregator := &testSignatureAggregator{}
	conf := simplex.EpochConfig{
		MaxProposalWait:     simplex.DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[1],
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

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	require.Equal(t, md.Round, md.Seq)

	block := <-bb.out
	block.verificationDelay = make(chan struct{}) // add a delay to the block verification

	vote, err := newTestVote(block, nodes[0])
	require.NoError(t, err)

	err = e.HandleMessage(&simplex.Message{
		BlockMessage: &simplex.BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[0])
	require.NoError(t, err)

	fCert, _ := newFinalizationRecord(t, l, signatureAggregator, block, nodes[0:quorum])
	// send fcert
	err = e.HandleMessage(&simplex.Message{
		FinalizationCertificate: &fCert,
	}, nodes[0])
	require.NoError(t, err)

	block.verificationDelay <- struct{}{} // unblock the block verification

	storedBlock := storage.waitForBlockCommit(0)
	require.Equal(t, uint64(1), storage.Height())
	require.Equal(t, block, storedBlock)
}

// TestReplicationAfterNodeDisconnects tests the replication process of a node that
// disconnects from the network and reconnects after the rest of the network has made progress.
//
// All nodes make progress for `startDisconnect` blocks. The lagging node disconnects
// and the rest of the nodes continue to make progress for another `endDisconnect - startDisconnect` blocks.
// The lagging node reconnects and the after the next `fCert` is sent, the lagging node catches up to the latest height.
func TestReplicationAfterNodeDisconnects(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}

	for startDisconnect := uint64(0); startDisconnect <= 5; startDisconnect++ {
		for endDisconnect := uint64(10); endDisconnect <= 20; endDisconnect++ {
			// lagging node cannot be the leader after node disconnects
			isLaggingNodeLeader := bytes.Equal(simplex.LeaderForRound(nodes, endDisconnect), nodes[3])
			if isLaggingNodeLeader {
				continue
			}

			testName := fmt.Sprintf("Disconnect_%d_to_%d", startDisconnect, endDisconnect)

			t.Run(testName, func(t *testing.T) {
				t.Parallel()
				testReplicationAfterNodeDisconnects(t, nodes, startDisconnect, endDisconnect)
			})
		}
	}
}

func testReplicationAfterNodeDisconnects(t *testing.T, nodes []simplex.NodeID, startDisconnect, endDisconnect uint64) {
	bb := newTestControlledBlockBuilder(t)
	net := newInMemNetwork(t, nodes)
	normalNode1 := newSimplexNode(t, nodes[0], net, bb, true)
	normalNode2 := newSimplexNode(t, nodes[1], net, bb, true)
	normalNode3 := newSimplexNode(t, nodes[2], net, bb, true)
	laggingNode := newSimplexNode(t, nodes[3], net, bb, true)

	require.Equal(t, uint64(0), normalNode1.storage.Height())
	require.Equal(t, uint64(0), normalNode2.storage.Height())
	require.Equal(t, uint64(0), normalNode3.storage.Height())
	require.Equal(t, uint64(0), laggingNode.storage.Height())

	epochTimes := make([]time.Time, 0, 4)
	for _, n := range net.instances {
		epochTimes = append(epochTimes, n.e.StartTime)
	}

	net.startInstances()

	for i := uint64(0); i < startDisconnect; i++ {
		bb.triggerNewBlock()
		for _, n := range net.instances {
			n.storage.waitForBlockCommit(i)
		}
	}

	// all nodes have commited `startDisconnect` blocks
	for _, n := range net.instances {
		require.Equal(t, startDisconnect, n.storage.Height())
	}

	// lagging node disconnects
	net.Disconnect(nodes[3])

	isLaggingNodeLeader := bytes.Equal(simplex.LeaderForRound(nodes, startDisconnect), nodes[3])
	if isLaggingNodeLeader {
		bb.triggerNewBlock()
	}

	missedSeqs := uint64(0)
	// normal nodes continue to make progress
	for i := startDisconnect; i < endDisconnect; i++ {
		emptyRound := bytes.Equal(simplex.LeaderForRound(nodes, i), nodes[3])
		if emptyRound {
			advanceWithoutLeader(t, net, bb, epochTimes)
			missedSeqs++
		} else {
			bb.triggerNewBlock()
			for _, n := range net.instances[:3] {
				n.storage.waitForBlockCommit(i - missedSeqs)
			}
		}
	}
	// all nodes excpet for lagging node have progressed and commited [endDisconnect - missedSeqs] blocks
	for _, n := range net.instances[:3] {
		require.Equal(t, endDisconnect-missedSeqs, n.storage.Height())
	}
	require.Equal(t, startDisconnect, laggingNode.storage.Height())
	require.Equal(t, startDisconnect, laggingNode.e.Metadata().Round)
	// lagging node reconnects
	net.Connect(nodes[3])

	bb.triggerNewBlock()
	for _, n := range net.instances {
		n.storage.waitForBlockCommit(endDisconnect - missedSeqs)
	}

	for _, n := range net.instances {
		require.Equal(t, endDisconnect-missedSeqs, n.storage.Height()-1)
		require.Equal(t, endDisconnect+1, n.e.Metadata().Round)
	}

	bb.triggerNewBlock() // the lagging node should build a block when triggered
	for _, n := range net.instances {
		n.storage.waitForBlockCommit(endDisconnect - missedSeqs + 1)
	}
}

func advanceWithoutLeader(t *testing.T, net *inMemNetwork, bb *testControlledBlockBuilder, epochTimes []time.Time) {
	for range net.instances {
		bb.blockShouldBeBuilt <- struct{}{}
	}

	for i, n := range net.instances[:3] {
		waitForBlockProposerTimeout(t, n.e, epochTimes[i])
	}
}

func createBlocks(t *testing.T, nodes []simplex.NodeID, bb simplex.BlockBuilder, seqCount uint64) []simplex.FinalizedBlock {
	logger := testutil.MakeLogger(t, int(0))
	ctx := context.Background()
	data := make([]simplex.FinalizedBlock, 0, seqCount)
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
		data = append(data, simplex.FinalizedBlock{
			Block: block,
			FCert: fCert,
		})
	}
	return data
}

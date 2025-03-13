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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestReplication tests the replication process of a node that
// is behind the rest of the network by less than maxRoundWindow.
func TestBasicReplication(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, []byte("lagging")}

	// wihtih max round window for now
	for i := range 3 * simplex.DefaultMaxRoundWindow {
		testName := fmt.Sprintf("Basic replication_of_%d_blocks", i)

		// lagging node cannot be the leader after node disconnects
		isLaggingNodeLeader := bytes.Equal(simplex.LeaderForRound(nodes, uint64(i)), nodes[3])
		if isLaggingNodeLeader {
			continue
		}

		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			testReplication(t, uint64(i), nodes)
		})
	}

}

func testReplication(t *testing.T, startSeq uint64, nodes []simplex.NodeID) {
	bb := newTestControlledBlockBuilder(t)
	net := newInMemNetwork(t, nodes)

	// initiate a network with 4 nodes. one node is behind by 8 blocks
	storageData := createBlocks(t, nodes, &bb.testBlockBuilder, startSeq)
	testEpochConfig := &testNodeConfig{
		initialStorage:     storageData,
		replicationEnabled: true,
	}
	normalNode1 := newSimplexNode(t, nodes[0], net, bb, testEpochConfig)
	normalNode2 := newSimplexNode(t, nodes[1], net, bb, testEpochConfig)
	normalNode3 := newSimplexNode(t, nodes[2], net, bb, testEpochConfig)
	laggingNode := newSimplexNode(t, nodes[3], net, bb, &testNodeConfig{
		replicationEnabled: true,
	})

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

// TestReplicationNotarizations tests that a lagging node also replicates
// notarizations after lagging behind.
func TestReplicationNotarizations(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	bb := newTestControlledBlockBuilder(t)
	net := newInMemNetwork(t, nodes)

	newNodeConfig := func(from simplex.NodeID) *testNodeConfig {
		comm := newTestComm(from, net, denyFinalizationMessages)
		return &testNodeConfig{
			comm:               comm,
			replicationEnabled: true,
		}
	}

	newSimplexNode(t, nodes[0], net, bb, newNodeConfig(nodes[0]))
	newSimplexNode(t, nodes[1], net, bb, newNodeConfig(nodes[1]))
	newSimplexNode(t, nodes[2], net, bb, newNodeConfig(nodes[2]))
	laggingNode := newSimplexNode(t, nodes[3], net, bb, newNodeConfig(nodes[3]))

	for _, n := range net.instances {
		require.Equal(t, uint64(0), n.storage.Height())
	}

	epochTimes := make([]time.Time, 0, 4)
	for _, n := range net.instances {
		epochTimes = append(epochTimes, n.e.StartTime)
	}

	net.startInstances()

	net.Disconnect(nodes[3])
	numNotarizations := 9
	missedSeqs := uint64(0)
	blocks := []simplex.VerifiedBlock{}
	// normal nodes continue to make progress
	for i := uint64(0); i < uint64(numNotarizations); i++ {
		emptyRound := bytes.Equal(simplex.LeaderForRound(nodes, i), nodes[3])
		if emptyRound {
			advanceWithoutLeader(t, net, bb, epochTimes, i)
			missedSeqs++
		} else {
			bb.triggerNewBlock()
			block := <-bb.out
			blocks = append(blocks, block)
			for _, n := range net.instances[:3] {
				n.wal.assertNotarization(i)
			}
		}
	}

	for _, n := range net.instances[:3] {
		// assert metadata
		require.Equal(t, uint64(numNotarizations), n.e.Metadata().Round)
		require.Equal(t, uint64(0), n.e.Storage.Height())
	}

	net.Connect(nodes[3])
	net.setAllNodesMessageFilter(allowAllMessages)
	fCert, _ := newFinalizationRecord(t, laggingNode.e.Logger, laggingNode.e.EpochConfig.SignatureAggregator, blocks[0], nodes)

	// we broadcast from the second node so that node 1 will be able to respond
	// to the lagging nodes replication request in time
	net.instances[2].e.Comm.Broadcast(&simplex.Message{
		FinalizationCertificate: &fCert,
	})
	// all nodes should have replicated finalization certificates
	for _, n := range net.instances {
		n.storage.waitForBlockCommit(0)
	}

	for i := 1; i < numNotarizations; i++ {
		for _, n := range net.instances {
			// lagging node wont have a notarization record if it was the leader
			leader := simplex.LeaderForRound(nodes, uint64(i))
			if n.e.ID.Equals(leader) && n.e.ID.Equals(nodes[3]) {
				continue
			}
			n.wal.assertNotarization(uint64(i))
		}
	}
}

// TestReplicationEmptyNotarizations ensures a lagging node will properly replicate
// many empty notarizations in a row.
func TestReplicationEmptyNotarizations(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	bb := newTestControlledBlockBuilder(t)
	net := newInMemNetwork(t, nodes)

	newNodeConfig := func(from simplex.NodeID) *testNodeConfig {
		comm := newTestComm(from, net, denyFinalizationMessages)
		return &testNodeConfig{
			comm:               comm,
			replicationEnabled: true,
		}
	}

	normalNode1 := newSimplexNode(t, nodes[0], net, bb, newNodeConfig(nodes[0]))
	normalNode2 := newSimplexNode(t, nodes[1], net, bb, newNodeConfig(nodes[1]))
	newSimplexNode(t, nodes[2], net, bb, newNodeConfig(nodes[2]))
	laggingNode := newSimplexNode(t, nodes[3], net, bb, newNodeConfig(nodes[3]))

	for _, n := range net.instances {
		require.Equal(t, uint64(0), n.storage.Height())
	}

	net.startInstances()

	net.Disconnect(nodes[3])
	numNotarizations := uint64(9)

	bb.triggerNewBlock()
	block := <-bb.out
	for _, n := range net.instances[:3] {
		n.wal.assertNotarization(0)
	}

	net.setAllNodesMessageFilter(onlyAllowEmptyRoundMessages)

	// normal nodes continue to make progress
	for i := uint64(1); i < uint64(numNotarizations); i++ {
		bb.triggerNewBlock()
		emptyNotarization := newEmptyNotarization(nodes[:3], i, 1)
		msg := &simplex.Message{
			EmptyNotarization: emptyNotarization,
		}
		normalNode2.e.Comm.SendMessage(msg, normalNode1.e.ID)
		normalNode1.e.Comm.Broadcast(msg)

		for _, n := range net.instances[:3] {
			n.wal.assertNotarization(i)
		}
	}

	for _, n := range net.instances[:3] {
		// assert metadata
		require.Equal(t, uint64(numNotarizations), n.e.Metadata().Round)
		require.Equal(t, uint64(1), n.e.Metadata().Seq)
		require.Equal(t, uint64(0), n.e.Storage.Height())
	}

	net.setAllNodesMessageFilter(allowAllMessages)
	net.Connect(nodes[3])

	fCert, _ := newFinalizationRecord(t, laggingNode.e.Logger, laggingNode.e.SignatureAggregator, block, nodes)

	// we broadcast from the second node so that node 1 will be able to respond
	// to the lagging nodes request
	normalNode2.e.Comm.Broadcast(&simplex.Message{
		FinalizationCertificate: &fCert,
	})
	for _, n := range net.instances {
		n.storage.waitForBlockCommit(0)
	}

	laggingNode.wal.assertNotarization(uint64(numNotarizations) - 1)
	require.Equal(t, uint64(1), laggingNode.storage.Height())
	require.Equal(t, numNotarizations, laggingNode.e.Metadata().Round)
	require.Equal(t, uint64(1), laggingNode.e.Metadata().Seq)
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
	testEpochConfig := &testNodeConfig{
		initialStorage:     storageData,
		replicationEnabled: true,
	}
	normalNode1 := newSimplexNode(t, nodes[0], net, bb, testEpochConfig)
	normalNode2 := newSimplexNode(t, nodes[1], net, bb, testEpochConfig)
	normalNode3 := newSimplexNode(t, nodes[2], net, bb, testEpochConfig)
	laggingNode := newSimplexNode(t, nodes[3], net, bb, &testNodeConfig{
		replicationEnabled: true,
	})

	firstBlock := storageData[0].VerifiedBlock
	record := simplex.BlockRecord(firstBlock.BlockHeader(), firstBlock.Bytes())
	laggingNode.wal.Append(record)

	firstNotarizationRecord, err := newNotarizationRecord(laggingNode.e.Logger, laggingNode.e.SignatureAggregator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	laggingNode.wal.Append(firstNotarizationRecord)

	secondBlock := storageData[1].VerifiedBlock
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
	testConfig := &testNodeConfig{
		replicationEnabled: true,
	}
	normalNode1 := newSimplexNode(t, nodes[0], net, bb, testConfig)
	normalNode2 := newSimplexNode(t, nodes[1], net, bb, testConfig)
	normalNode3 := newSimplexNode(t, nodes[2], net, bb, testConfig)
	laggingNode := newSimplexNode(t, nodes[3], net, bb, testConfig)

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
			advanceWithoutLeader(t, net, bb, epochTimes, i)
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

func advanceWithoutLeader(t *testing.T, net *inMemNetwork, bb *testControlledBlockBuilder, epochTimes []time.Time, round uint64) {
	for range net.instances {
		time.Sleep(10 * time.Millisecond)
		bb.blockShouldBeBuilt <- struct{}{}
	}

	for i, n := range net.instances[:3] {
		waitForBlockProposerTimeout(t, n.e, epochTimes[i])
	}

	for _, n := range net.instances[:3] {
		n.wal.assertNotarization(round)
	}
}

type VerifiedFinalizedBlock struct {
	VerifiedBlock simplex.VerifiedBlock
	FCert         simplex.FinalizationCertificate
}

func createBlocks(t *testing.T, nodes []simplex.NodeID, bb simplex.BlockBuilder, seqCount uint64) []VerifiedFinalizedBlock {
	logger := testutil.MakeLogger(t, int(0))
	ctx := context.Background()
	data := make([]VerifiedFinalizedBlock, 0, seqCount)
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
		data = append(data, VerifiedFinalizedBlock{
			VerifiedBlock: block,
			FCert:         fCert,
		})
	}
	return data
}

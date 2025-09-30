// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	. "github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/record"
	"github.com/ava-labs/simplex/testutil"

	"github.com/stretchr/testify/require"
)

// TestRecoverFromWALProposed tests that the epoch can recover from
// a wal with a single block record written to it(that we have proposed).
func TestRecoverFromWALProposed(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	protocolMetadata := e.Metadata()
	firstBlock, ok := bb.BuildBlock(ctx, protocolMetadata, emptyBlacklist)
	require.True(t, ok)
	fBytes, err := firstBlock.Bytes()
	require.NoError(t, err)
	record := BlockRecord(firstBlock.BlockHeader(), fBytes)

	// write block record to wal
	require.NoError(t, wal.Append(record))

	records, err := wal.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, record, records[0])

	err = e.Start()
	require.NoError(t, err)

	rounds := uint64(100)
	for i := uint64(0); i < rounds; i++ {
		leader := LeaderForRound(nodes, uint64(i))
		isEpochNode := leader.Equals(e.ID)
		if !isEpochNode {
			md := e.Metadata()
			_, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
			require.True(t, ok)
			require.NotEqual(t, 0, rounds)
		}

		block := <-bb.Out
		if rounds == 0 {
			require.Equal(t, firstBlock, block)
		}

		if !isEpochNode {
			// send node a message from the leader
			vote, err := testutil.NewTestVote(block, leader)
			require.NoError(t, err)
			err = e.HandleMessage(&Message{
				BlockMessage: &BlockMessage{
					Vote:  *vote,
					Block: block,
				},
			}, leader)
			require.NoError(t, err)
		}

		// start at one since our node has already voted
		for i := 1; i < quorum; i++ {
			testutil.InjectTestVote(t, e, block, nodes[i])
		}

		for i := 1; i < quorum; i++ {
			testutil.InjectTestFinalizeVote(t, e, block, nodes[i])
		}

		block2 := storage.WaitForBlockCommit(i)

		require.Equal(t, block, block2)
	}

	require.Equal(t, rounds, e.Storage.NumBlocks())
}

// TestRecoverFromWALNotarized tests that the epoch can recover from a wal
// with a block record written to it, and a notarization record.
func TestRecoverFromNotarization(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	sigAggregrator := &testutil.TestSignatureAggregator{}
	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	protocolMetadata := e.Metadata()
	block, ok := bb.BuildBlock(ctx, protocolMetadata, emptyBlacklist)
	require.True(t, ok)
	bBytes, err := block.Bytes()
	require.NoError(t, err)
	blockRecord := BlockRecord(block.BlockHeader(), bBytes)

	// write block blockRecord to wal
	require.NoError(t, wal.Append(blockRecord))

	// lets add some notarizations
	notarizationRecord, err := testutil.NewNotarizationRecord(conf.Logger, sigAggregrator, block, nodes[0:quorum])
	require.NoError(t, err)

	// when we start this we should kickoff the finalization process by broadcasting a finalization message and then waiting for incoming finalization messages
	require.NoError(t, wal.Append(notarizationRecord))

	records, err := wal.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)
	require.Equal(t, blockRecord, records[0])
	require.Equal(t, notarizationRecord, records[1])

	require.Equal(t, uint64(0), e.Metadata().Round)
	err = e.Start()
	require.NoError(t, err)

	// require the round was incremented(notarization increases round)
	require.Equal(t, uint64(1), e.Metadata().Round)
	for i := 1; i < quorum; i++ {
		testutil.InjectTestFinalizeVote(t, e, block, nodes[i])
	}

	firstBlock, _, err := storage.Retrieve(0)
	require.NoError(t, err)
	committedData, err := firstBlock.Bytes()
	require.NoError(t, err)
	require.Equal(t, bBytes, committedData)
	require.Equal(t, uint64(1), e.Storage.NumBlocks())
}

// TestRecoverFromWALFinalized tests that the epoch can recover from a wal
// with a block already stored in the storage
func TestRecoverFromWalWithStorage(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	sigAggregrator := &testutil.TestSignatureAggregator{}
	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	err := storage.Index(ctx, testutil.NewTestBlock(ProtocolMetadata{Seq: 0, Round: 0, Epoch: 0}, emptyBlacklist), Finalization{})
	require.NoError(t, err)

	e, err := NewEpoch(conf)
	require.NoError(t, err)
	require.Equal(t, uint64(1), e.Metadata().Round)

	protocolMetadata := e.Metadata()
	block, ok := bb.BuildBlock(ctx, protocolMetadata, emptyBlacklist)
	require.True(t, ok)
	bBytes, err := block.Bytes()
	require.NoError(t, err)
	record := BlockRecord(block.BlockHeader(), bBytes)

	// write block record to wal
	require.NoError(t, wal.Append(record))

	// lets add some notarizations
	notarizationRecord, err := testutil.NewNotarizationRecord(conf.Logger, sigAggregrator, block, nodes[0:quorum])
	require.NoError(t, err)

	require.NoError(t, wal.Append(notarizationRecord))

	records, err := wal.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)
	require.Equal(t, record, records[0])
	require.Equal(t, notarizationRecord, records[1])
	_, vote, err := ParseNotarizationRecord(records[1])
	require.NoError(t, err)
	require.Equal(t, uint64(1), vote.Round)

	err = e.Start()
	require.NoError(t, err)

	// require the round was incremented(notarization increases round)
	require.Equal(t, uint64(2), e.Metadata().Round)

	for i := 1; i < quorum; i++ {
		// type assert block to testBlock
		testutil.InjectTestFinalizeVote(t, e, block, nodes[i])
	}

	secondBlock, _, err := storage.Retrieve(1)
	require.NoError(t, err)
	committedData, err := secondBlock.Bytes()
	require.NoError(t, err)
	bBytes, err = block.Bytes()
	require.Equal(t, bBytes, committedData)
	require.Equal(t, uint64(2), e.Storage.NumBlocks())
}

// TestWalCreated tests that the epoch correctly writes to the WAL
func TestWalCreatedProperly(t *testing.T) {
	ctx := context.Background()
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	// ensure no records are written to the WAL
	records, err := e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 0)

	require.NoError(t, e.Start())

	// ensure a block record is written to the WAL
	wal.AssertWALSize(1)
	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 1)
	blockFromWal, err := BlockFromRecord(ctx, conf.BlockDeserializer, records[0])
	require.NoError(t, err)
	block := <-bb.Out
	require.Equal(t, blockFromWal, block)

	// start at one since our node has already voted
	for i := 1; i < quorum; i++ {
		testutil.InjectTestVote(t, e, block, nodes[i])
	}

	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)
	expectedNotarizationRecord, err := testutil.NewNotarizationRecord(conf.Logger, conf.SignatureAggregator, block, nodes[0:quorum])
	require.NoError(t, err)
	require.Equal(t, expectedNotarizationRecord, records[1])

	for i := 1; i < quorum; i++ {
		testutil.InjectTestFinalizeVote(t, e, block, nodes[i])
	}

	// we do not append the finalization record to the WAL if it for the next expected sequence
	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)

	blockRetrieved, _, err := storage.Retrieve(0)
	require.NoError(t, err)
	committedData, err := blockRetrieved.Bytes()
	require.NoError(t, err)
	bBytes, err := block.Bytes()
	require.Equal(t, bBytes, committedData)
}

// TestWalWritesBlockRecord tests that the epoch correctly writes to the WAL
// a block proposed by a node other than the epoch node
func TestWalWritesBlockRecord(t *testing.T) {
	ctx := context.Background()
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	wal := testutil.NewTestWAL(t)
	// nodes[1] is not the leader for the first round
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[1], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	// ensure no records are written to the WAL
	records, err := e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 0)

	require.NoError(t, e.Start())
	// ensure no records are written to the WAL
	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 0)

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
	require.True(t, ok)

	block := <-bb.Out
	// send epoch node this block
	vote, err := testutil.NewTestVote(block, nodes[0])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[0])
	require.NoError(t, err)

	// ensure a block record is written to the WAL
	wal.AssertWALSize(1)
	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 1)
	blockFromWal, err := BlockFromRecord(ctx, conf.BlockDeserializer, records[0])
	require.NoError(t, err)
	require.Equal(t, block, blockFromWal)
}

func TestWalWritesFinalization(t *testing.T) {
	ctx := context.Background()
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	sigAggregrator := &testutil.TestSignatureAggregator{}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())
	firstBlock := <-bb.Out
	// notarize the first block
	for i := 1; i < quorum; i++ {
		testutil.InjectTestVote(t, e, firstBlock, nodes[i])
	}
	records, err := e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)
	blockFromWal, err := BlockFromRecord(ctx, conf.BlockDeserializer, records[0])
	require.NoError(t, err)

	require.Equal(t, firstBlock, blockFromWal)
	expectedNotarizationRecord, err := testutil.NewNotarizationRecord(conf.Logger, sigAggregrator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	require.Equal(t, expectedNotarizationRecord, records[1])

	// send and notarize a second block
	require.Equal(t, uint64(1), e.Metadata().Round)
	md := e.Metadata()
	md.Seq++
	md.Prev = firstBlock.BlockHeader().Digest
	_, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
	require.True(t, ok)
	secondBlock := <-bb.Out

	// increase the round but don't index storage
	require.Equal(t, uint64(1), e.Metadata().Round)
	require.Equal(t, uint64(0), e.Storage.NumBlocks())

	vote, err := testutil.NewTestVote(secondBlock, nodes[1])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: secondBlock,
		},
	}, nodes[1])
	require.NoError(t, err)

	for i := 1; i < quorum; i++ {
		testutil.InjectTestVote(t, e, secondBlock, nodes[i])
	}

	wal.AssertWALSize(4)

	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 4)
	blockFromWal, err = BlockFromRecord(ctx, conf.BlockDeserializer, records[2])
	require.NoError(t, err)
	require.Equal(t, secondBlock, blockFromWal)
	expectedNotarizationRecord, err = testutil.NewNotarizationRecord(conf.Logger, sigAggregrator, secondBlock, nodes[0:quorum])
	require.NoError(t, err)
	require.Equal(t, expectedNotarizationRecord, records[3])

	// finalization for the second block should write to wal
	for i := 1; i < quorum; i++ {
		testutil.InjectTestFinalizeVote(t, e, secondBlock, nodes[i])
	}

	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 5)
	recordType := binary.BigEndian.Uint16(records[4])
	require.Equal(t, record.FinalizationRecordType, recordType)
	_, err = FinalizationFromRecord(records[4], e.QCDeserializer)
	_, expectedFinalizationRecord := testutil.NewFinalizationRecord(t, conf.Logger, sigAggregrator, secondBlock, nodes[0:quorum])
	require.NoError(t, err)
	require.Equal(t, expectedFinalizationRecord, records[4])

	// ensure the finalization is not indexed
	require.Equal(t, uint64(2), e.Metadata().Round)
	require.Equal(t, uint64(0), e.Storage.NumBlocks())
}

// Appends to the wal -> block, notarization, second block, notarization block 2, finalization for block 2.
func TestRecoverFromMultipleNotarizations(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	// Create first block and write to WAL
	e, err := NewEpoch(conf)
	require.NoError(t, err)

	protocolMetadata := e.Metadata()
	firstBlock, ok := bb.BuildBlock(ctx, protocolMetadata, emptyBlacklist)
	require.True(t, ok)
	fBytes, err := firstBlock.Bytes()
	require.NoError(t, err)
	record := BlockRecord(firstBlock.BlockHeader(), fBytes)
	wal.Append(record)

	firstNotarizationRecord, err := testutil.NewNotarizationRecord(conf.Logger, conf.SignatureAggregator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	wal.Append(firstNotarizationRecord)

	protocolMetadata.Round = 1
	protocolMetadata.Seq = 1
	secondBlock, ok := bb.BuildBlock(ctx, protocolMetadata, emptyBlacklist)
	require.True(t, ok)
	sBytes, err := secondBlock.Bytes()
	require.NoError(t, err)
	record = BlockRecord(secondBlock.BlockHeader(), sBytes)
	wal.Append(record)

	// Add notarization for second block
	secondNotarizationRecord, err := testutil.NewNotarizationRecord(conf.Logger, conf.SignatureAggregator, secondBlock, nodes[0:quorum])
	require.NoError(t, err)
	wal.Append(secondNotarizationRecord)

	// Create finalization record for second block
	finalization2, finalizationRecord := testutil.NewFinalizationRecord(t, conf.Logger, conf.SignatureAggregator, secondBlock, nodes[0:quorum])
	wal.Append(finalizationRecord)

	err = e.Start()
	require.NoError(t, err)

	require.Equal(t, uint64(2), e.Metadata().Round)
	require.Equal(t, uint64(0), e.Storage.NumBlocks())

	// now if we send finalization for block 1, we should index both 1 & 2
	finalization1, _ := testutil.NewFinalizationRecord(t, conf.Logger, conf.SignatureAggregator, firstBlock, nodes[0:quorum])
	err = e.HandleMessage(&Message{
		Finalization: &finalization1,
	}, nodes[1])
	require.NoError(t, err)

	require.Equal(t, uint64(2), e.Storage.NumBlocks())
	firstBlockRetrieved, finalizationRetrieved1, err := storage.Retrieve(0)
	require.NoError(t, err)
	storageBytes, err := firstBlockRetrieved.Bytes()
	require.NoError(t, err)
	require.Equal(t, fBytes, storageBytes)

	secondBlockRetrieved, finalizationRetrieved2, err := storage.Retrieve(1)
	require.NoError(t, err)
	storageBytes, err = secondBlockRetrieved.Bytes()
	require.NoError(t, err)
	require.Equal(t, sBytes, storageBytes)
	require.Equal(t, finalization1, finalizationRetrieved1)
	require.Equal(t, finalization2, finalizationRetrieved2)
}

// TestRecoveryBlocksIndexed tests that the epoch properly skips
// block records that are already indexed in the storage.
func TestRecoveryBlocksIndexed(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	protocolMetadata := ProtocolMetadata{Seq: 0, Round: 0, Epoch: 0}
	firstBlock, ok := bb.BuildBlock(ctx, protocolMetadata, emptyBlacklist)
	fBytes, err := firstBlock.Bytes()
	require.NoError(t, err)
	require.True(t, ok)
	record := BlockRecord(firstBlock.BlockHeader(), fBytes)
	wal.Append(record)

	firstNotarizationRecord, err := testutil.NewNotarizationRecord(conf.Logger, conf.SignatureAggregator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	wal.Append(firstNotarizationRecord)

	_, finalizationBytes := testutil.NewFinalizationRecord(t, conf.Logger, conf.SignatureAggregator, firstBlock, nodes[0:quorum])
	wal.Append(finalizationBytes)

	protocolMetadata.Round = 1
	protocolMetadata.Seq = 1
	secondBlock, ok := bb.BuildBlock(ctx, protocolMetadata, emptyBlacklist)
	require.True(t, ok)
	sBytes, err := secondBlock.Bytes()
	require.NoError(t, err)
	record = BlockRecord(secondBlock.BlockHeader(), sBytes)
	wal.Append(record)

	protocolMetadata.Round = 2
	protocolMetadata.Seq = 2
	thirdBlock, ok := bb.BuildBlock(ctx, protocolMetadata, emptyBlacklist)
	require.True(t, ok)
	tBytes, err := thirdBlock.Bytes()
	require.NoError(t, err)
	record = BlockRecord(thirdBlock.BlockHeader(), tBytes)
	wal.Append(record)

	finalization1, _ := testutil.NewFinalizationRecord(t, conf.Logger, conf.SignatureAggregator, firstBlock, nodes[0:quorum])
	finalization2, _ := testutil.NewFinalizationRecord(t, conf.Logger, conf.SignatureAggregator, secondBlock, nodes[0:quorum])
	fCer3, _ := testutil.NewFinalizationRecord(t, conf.Logger, conf.SignatureAggregator, thirdBlock, nodes[0:quorum])

	conf.Storage.Index(ctx, firstBlock, finalization1)
	conf.Storage.Index(ctx, secondBlock, finalization2)
	conf.Storage.Index(ctx, thirdBlock, fCer3)

	e, err := NewEpoch(conf)
	require.NoError(t, err)
	require.Equal(t, uint64(3), e.Storage.NumBlocks())
	require.NoError(t, e.Start())

	// ensure the round is properly set to 3
	require.Equal(t, uint64(3), e.Metadata().Round)
	require.Equal(t, uint64(3), e.Metadata().Seq)
	require.Equal(t, thirdBlock.BlockHeader().Digest, e.Metadata().Prev)
}

func TestEpochCorrectlyInitializesMetadataFromStorage(t *testing.T) {
	ctx := context.Background()
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, _, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	block := testutil.NewTestBlock(ProtocolMetadata{Seq: 0, Round: 0, Epoch: 0}, emptyBlacklist)
	conf.Storage.Index(ctx, block, Finalization{})
	e, err := NewEpoch(conf)
	require.NoError(t, err)
	require.Equal(t, uint64(1), e.Storage.NumBlocks())
	require.NoError(t, e.Start())

	// ensure the round is properly set
	require.Equal(t, uint64(1), e.Metadata().Round)
	require.Equal(t, uint64(1), e.Metadata().Seq)
	require.Equal(t, block.BlockHeader().Digest, e.Metadata().Prev)
}

func TestRecoveryAsLeader(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	finalizedBlocks := createBlocks(t, nodes, 4)
	conf, _, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	for _, finalizedBlock := range finalizedBlocks {
		err := storage.Index(ctx, finalizedBlock.VerifiedBlock, finalizedBlock.Finalization)
		require.NoError(t, err)
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)
	require.Equal(t, uint64(4), e.Storage.NumBlocks())
	require.NoError(t, e.Start())

	<-bb.Out

	// wait for the block to finish verifying
	time.Sleep(50 * time.Millisecond)

	// ensure the round is properly set
	require.Equal(t, uint64(4), e.Metadata().Round)
	require.Equal(t, uint64(4), e.Metadata().Seq)
}

func TestRecoveryReVerifiesBlocks(t *testing.T) {
	ctx := context.Background()
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	finalizedBlocks := createBlocks(t, nodes, 4)
	
	deserializer := &testutil.BlockDeserializer{
		DelayedVerification: make(chan struct{}, 1),
	}
	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)
	for _, finalizedBlock := range finalizedBlocks {
		err := storage.Index(ctx, finalizedBlock.VerifiedBlock, finalizedBlock.Finalization)
		require.NoError(t, err)
	}
	conf.BlockDeserializer = deserializer

	// Create first block and write to WAL
	e, err := NewEpoch(conf)
	require.NoError(t, err)

	protocolMetadata := e.Metadata()
	firstBlock, ok := bb.BuildBlock(ctx, protocolMetadata, emptyBlacklist)
	require.True(t, ok)
	fBytes, err := firstBlock.Bytes()
	require.NoError(t, err)
	record := BlockRecord(firstBlock.BlockHeader(), fBytes)
	wal.Append(record)

	deserializer.DelayedVerification <- struct{}{}
	require.NoError(t, e.Start())
	require.Len(t, deserializer.DelayedVerification, 0)
}

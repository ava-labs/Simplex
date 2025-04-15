// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"context"
	"encoding/binary"
	. "simplex"
	"simplex/record"
	"simplex/testutil"
	"testing"
	"time"

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
	firstBlock, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	record := BlockRecord(firstBlock.BlockHeader(), firstBlock.Bytes())

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
			_, ok := bb.BuildBlock(context.Background(), md)
			require.True(t, ok)
			require.NotEqual(t, 0, rounds)
		}

		block := <-bb.Out
		if rounds == 0 {
			require.Equal(t, firstBlock, block)
		}

		if !isEpochNode {
			// send node a message from the leader
			vote, err := newTestVote(block, leader, conf.Signer)
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
			injectTestVote(t, e, block, nodes[i])
		}

		for i := 1; i < quorum; i++ {
			injectTestFinalization(t, e, block, nodes[i])
		}

		block2 := storage.WaitForBlockCommit(i)

		require.Equal(t, block, block2)
	}
	
	require.Equal(t, rounds, e.Storage.Height())
}

// TestRecoverFromWALNotarized tests that the epoch can recover from a wal
// with a block record written to it, and a notarization record.
func TestRecoverFromNotarization(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	protocolMetadata := e.Metadata()
	block, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	blockRecord := BlockRecord(block.BlockHeader(), block.Bytes())

	// write block blockRecord to wal
	require.NoError(t, wal.Append(blockRecord))

	// lets add some notarizations
	notarizationRecord, err := newNotarizationRecord(l, conf.SignatureAggregator, block, nodes[0:quorum])
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
		injectTestFinalization(t, e, block, nodes[i])
	}

	committedData := storage.Data[0].VerifiedBlock.Bytes()
	require.Equal(t, block.Bytes(), committedData)
	require.Equal(t, uint64(1), e.Storage.Height())
}

// TestRecoverFromWALFinalized tests that the epoch can recover from a wal
// with a block already stored in the storage
func TestRecoverFromWalWithStorage(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	storage.Index(testutil.NewTestBlock(ProtocolMetadata{Seq: 0, Round: 0, Epoch: 0}), FinalizationCertificate{})
	e, err := NewEpoch(conf)
	require.NoError(t, err)
	require.Equal(t, uint64(1), e.Metadata().Round)

	protocolMetadata := e.Metadata()
	block, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	record := BlockRecord(block.BlockHeader(), block.Bytes())

	// write block record to wal
	require.NoError(t, wal.Append(record))

	// lets add some notarizations
	notarizationRecord, err := newNotarizationRecord(l, conf.SignatureAggregator, block, nodes[0:quorum])
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
		injectTestFinalization(t, e, block, nodes[i])
	}

	committedData := storage.Data[1].VerifiedBlock.Bytes()
	require.Equal(t, block.Bytes(), committedData)
	require.Equal(t, uint64(2), e.Storage.Height())
}

// TestWalCreated tests that the epoch correctly writes to the WAL
func TestWalCreatedProperly(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
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
	blockFromWal, err := BlockFromRecord(conf.BlockDeserializer, records[0])
	require.NoError(t, err)
	block := <-bb.Out
	require.Equal(t, blockFromWal, block)

	// start at one since our node has already voted
	for i := 1; i < quorum; i++ {
		injectTestVote(t, e, block, nodes[i])
	}

	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)
	expectedNotarizationRecord, err := newNotarizationRecord(l, conf.SignatureAggregator, block, nodes[0:quorum])
	require.NoError(t, err)
	require.Equal(t, expectedNotarizationRecord, records[1])

	for i := 1; i < quorum; i++ {
		injectTestFinalization(t, e, block, nodes[i])
	}

	// we do not append the finalization record to the WAL if it for the next expected sequence
	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)

	committedData := storage.Data[0].VerifiedBlock.Bytes()
	require.Equal(t, block.Bytes(), committedData)
}

// TestWalWritesBlockRecord tests that the epoch correctly writes to the WAL
// a block proposed by a node other than the epoch node
func TestWalWritesBlockRecord(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

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
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)

	block := <-bb.Out
	// send epoch node this block
	vote, err := newTestVote(block, nodes[0], conf.Signer)
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
	blockFromWal, err := BlockFromRecord(conf.BlockDeserializer, records[0])
	require.NoError(t, err)
	require.Equal(t, block, blockFromWal)
}

func TestWalWritesFinalizationCert(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())
	firstBlock := <-bb.Out
	// notarize the first block
	for i := 1; i < quorum; i++ {
		injectTestVote(t, e, firstBlock, nodes[i])
	}
	records, err := e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)
	blockFromWal, err := BlockFromRecord(conf.BlockDeserializer, records[0])
	require.NoError(t, err)
	require.Equal(t, firstBlock, blockFromWal)
	expectedNotarizationRecord, err := newNotarizationRecord(l, conf.SignatureAggregator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	require.Equal(t, expectedNotarizationRecord, records[1])

	// send and notarize a second block
	require.Equal(t, uint64(1), e.Metadata().Round)
	md := e.Metadata()
	md.Seq++
	md.Prev = firstBlock.BlockHeader().Digest
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	secondBlock := <-bb.Out

	// increase the round but don't index storage
	require.Equal(t, uint64(1), e.Metadata().Round)
	require.Equal(t, uint64(0), e.Storage.Height())

	vote, err := newTestVote(secondBlock, nodes[1], conf.Signer)
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: secondBlock,
		},
	}, nodes[1])
	require.NoError(t, err)

	for i := 1; i < quorum; i++ {
		injectTestVote(t, e, secondBlock, nodes[i])
	}

	wal.AssertWALSize(4)

	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 4)
	blockFromWal, err = BlockFromRecord(conf.BlockDeserializer, records[2])
	require.NoError(t, err)
	require.Equal(t, secondBlock, blockFromWal)
	expectedNotarizationRecord, err = newNotarizationRecord(l, conf.SignatureAggregator, secondBlock, nodes[0:quorum])
	require.NoError(t, err)
	require.Equal(t, expectedNotarizationRecord, records[3])

	// finalization for the second block should write to wal
	for i := 1; i < quorum; i++ {
		injectTestFinalization(t, e, secondBlock, nodes[i])
	}

	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 5)
	recordType := binary.BigEndian.Uint16(records[4])
	require.Equal(t, record.FinalizationRecordType, recordType)
	_, err = FinalizationCertificateFromRecord(records[4], e.QCDeserializer)
	_, expectedFinalizationRecord := newFinalizationRecord(t, l, conf.SignatureAggregator, secondBlock, nodes[0:quorum])
	require.NoError(t, err)
	require.Equal(t, expectedFinalizationRecord, records[4])

	// ensure the finalization certificate is not indexed
	require.Equal(t, uint64(2), e.Metadata().Round)
	require.Equal(t, uint64(0), e.Storage.Height())
}

// Appends to the wal -> block, notarization, second block, notarization block 2, finalization for block 2.
func TestRecoverFromMultipleNotarizations(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	// Create first block and write to WAL
	e, err := NewEpoch(conf)
	require.NoError(t, err)

	protocolMetadata := e.Metadata()
	firstBlock, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	record := BlockRecord(firstBlock.BlockHeader(), firstBlock.Bytes())
	wal.Append(record)

	firstNotarizationRecord, err := newNotarizationRecord(l, conf.SignatureAggregator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	wal.Append(firstNotarizationRecord)

	protocolMetadata.Round = 1
	protocolMetadata.Seq = 1
	secondBlock, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	record = BlockRecord(secondBlock.BlockHeader(), secondBlock.Bytes())
	wal.Append(record)

	// Add notarization for second block
	secondNotarizationRecord, err := newNotarizationRecord(l, conf.SignatureAggregator, secondBlock, nodes[0:quorum])
	require.NoError(t, err)
	wal.Append(secondNotarizationRecord)

	// Create finalization record for second block
	fCert2, finalizationRecord := newFinalizationRecord(t, l, conf.SignatureAggregator, secondBlock, nodes[0:quorum])
	wal.Append(finalizationRecord)

	err = e.Start()
	require.NoError(t, err)

	require.Equal(t, uint64(2), e.Metadata().Round)
	require.Equal(t, uint64(0), e.Storage.Height())

	// now if we send fCert for block 1, we should index both 1 & 2
	fCert1, _ := newFinalizationRecord(t, l, conf.SignatureAggregator, firstBlock, nodes[0:quorum])
	err = e.HandleMessage(&Message{
		FinalizationCertificate: &fCert1,
	}, nodes[1])
	require.NoError(t, err)

	require.Equal(t, uint64(2), e.Storage.Height())
	require.Equal(t, firstBlock.Bytes(), storage.Data[0].VerifiedBlock.Bytes())
	require.Equal(t, secondBlock.Bytes(), storage.Data[1].VerifiedBlock.Bytes())
	require.Equal(t, fCert1, storage.Data[0].FinalizationCertificate)
	require.Equal(t, fCert2, storage.Data[1].FinalizationCertificate)
}

// TestRecoversFromMultipleNotarizations tests that the epoch can recover from a wal
// with its last notarization record being from a less recent round.
func TestRecoveryWithoutNotarization(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	protocolMetadata := ProtocolMetadata{Seq: 0, Round: 0, Epoch: 0}
	firstBlock, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	record := BlockRecord(firstBlock.BlockHeader(), firstBlock.Bytes())
	wal.Append(record)

	firstNotarizationRecord, err := newNotarizationRecord(l, conf.SignatureAggregator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	wal.Append(firstNotarizationRecord)

	protocolMetadata.Round = 1
	protocolMetadata.Seq = 1
	secondBlock, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	record = BlockRecord(secondBlock.BlockHeader(), secondBlock.Bytes())
	wal.Append(record)

	protocolMetadata.Round = 2
	protocolMetadata.Seq = 2
	thirdBlock, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	record = BlockRecord(thirdBlock.BlockHeader(), thirdBlock.Bytes())
	wal.Append(record)

	fCert1, _ := newFinalizationRecord(t, l, conf.SignatureAggregator, firstBlock, nodes[0:quorum])
	fCert2, _ := newFinalizationRecord(t, l, conf.SignatureAggregator, secondBlock, nodes[0:quorum])
	fCer3, _ := newFinalizationRecord(t, l, conf.SignatureAggregator, thirdBlock, nodes[0:quorum])

	conf.Storage.Index(firstBlock, fCert1)
	conf.Storage.Index(secondBlock, fCert2)
	conf.Storage.Index(thirdBlock, fCer3)

	e, err := NewEpoch(conf)
	require.NoError(t, err)
	require.Equal(t, uint64(3), e.Storage.Height())
	require.NoError(t, e.Start())

	// ensure the round is properly set to 3
	require.Equal(t, uint64(3), e.Metadata().Round)
	require.Equal(t, uint64(3), e.Metadata().Seq)
	require.Equal(t, thirdBlock.BlockHeader().Digest, e.Metadata().Prev)
}

func TestEpochCorrectlyInitializesMetadataFromStorage(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, _, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	block := testutil.NewTestBlock(ProtocolMetadata{Seq: 0, Round: 0, Epoch: 0})
	conf.Storage.Index(block, FinalizationCertificate{})
	e, err := NewEpoch(conf)
	require.NoError(t, err)
	require.Equal(t, uint64(1), e.Storage.Height())
	require.NoError(t, e.Start())

	// ensure the round is properly set
	require.Equal(t, uint64(1), e.Metadata().Round)
	require.Equal(t, uint64(1), e.Metadata().Seq)
	require.Equal(t, block.BlockHeader().Digest, e.Metadata().Prev)
}

func TestRecoveryAsLeader(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	finalizedBlocks := createBlocks(t, nodes, bb, 4)
	storage := testutil.NewInMemStorage()
	for _, finalizedBlock := range finalizedBlocks {
		storage.Index(finalizedBlock.VerifiedBlock, finalizedBlock.FCert)
	}

	conf, _, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)
	require.Equal(t, uint64(4), e.Storage.Height())
	require.NoError(t, e.Start())

	<-bb.Out

	// wait for the block to finish verifying
	time.Sleep(50 * time.Millisecond)

	// ensure the round is properly set
	require.Equal(t, uint64(4), e.Metadata().Round)
	require.Equal(t, uint64(4), e.Metadata().Seq)
}

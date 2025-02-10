// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"context"
	"encoding/binary"
	. "simplex"
	"simplex/record"
	"simplex/testutil"
	"simplex/wal"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRecoverFromWALProposed tests that the epoch can recover from
// a wal with a single block record written to it(that we have proposed).
func TestRecoverFromWALProposed(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	wal := newTestWAL(t)
	storage := newInMemStorage()
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
		BlockDeserializer:   &blockDeserializer{},
		QCDeserializer:      &testQCDeserializer{t: t},
	}

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

		block := <-bb.out
		if rounds == 0 {
			require.Equal(t, firstBlock, block)
		}

		if !isEpochNode {
			// send node a message from the leader
			vote, err := newTestVote(block, leader)
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

		block2 := storage.waitForBlockCommit(i)

		require.Equal(t, block, block2)
	}

	require.Equal(t, rounds, e.Storage.Height())
}

// TestRecoverFromWALNotarized tests that the epoch can recover from a wal
// with a block record written to it, and a notarization record.
func TestRecoverFromNotarization(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	wal := wal.NewMemWAL(t)
	storage := newInMemStorage()
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	sigAggregrator := &testSignatureAggregator{}
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: sigAggregrator,
		BlockDeserializer:   &blockDeserializer{},
		QCDeserializer:      &testQCDeserializer{t: t},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	protocolMetadata := e.Metadata()
	block, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	blockRecord := BlockRecord(block.BlockHeader(), block.Bytes())

	// write block blockRecord to wal
	require.NoError(t, wal.Append(blockRecord))

	// lets add some notarizations
	notarizationRecord, err := newNotarizationRecord(l, sigAggregrator, block, nodes[0:quorum])
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

	committedData := storage.data[0].Block.Bytes()
	require.Equal(t, block.Bytes(), committedData)
	require.Equal(t, uint64(1), e.Storage.Height())
}


// // TestRecoveryFCertsOutOfOrder tests that the epoch can recover from a wal
// // with a finalization certificate written to it out of order

// // Block1, notarization 1, block 2 , notarization 2, finalization 2, 
// func TestRecoveryFCertsOutOfOrder(t *testing.T) {
// 	l := testutil.MakeLogger(t, 1)
// 	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
// 	wal := wal.NewMemWAL(t)
// 	storage := newInMemStorage()
// 	ctx := context.Background()
// 	nodes := []NodeID{{1}, {2}, {3}, {4}}
// 	quorum := Quorum(len(nodes))
// 	sigAggregrator := &testSignatureAggregator{}
// 	conf := EpochConfig{
// 		MaxProposalWait:     DefaultMaxProposalWaitTime,
// 		Logger:              l,
// 		ID:                  nodes[0],
// 		Signer:              &testSigner{},
// 		WAL:                 wal,
// 		Verifier:            &testVerifier{},
// 		Storage:             storage,
// 		Comm:                noopComm(nodes),
// 		BlockBuilder:        bb,
// 		SignatureAggregator: sigAggregrator,
// 		BlockDeserializer:   &blockDeserializer{},
// 		QCDeserializer:      &testQCDeserializer{t: t},
// 	}

// 	e, err := NewEpoch(conf)
// 	require.NoError(t, err)

// 	protocolMetadata := e.Metadata()
// 	block, ok := bb.BuildBlock(ctx, protocolMetadata)
// 	require.True(t, ok)
// 	blockRecord := BlockRecord(block.BlockHeader(), block.Bytes())

// 	// write block blockRecord to wal
// 	require.NoError(t, wal.Append(blockRecord))
// }


// TestRecoverFromWALFinalized tests that the epoch can recover from a wal
// with a block already stored in the storage
func TestRecoverFromWalWithStorage(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	wal := wal.NewMemWAL(t)
	storage := newInMemStorage()
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	sigAggregrator := &testSignatureAggregator{}
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: sigAggregrator,
		BlockDeserializer:   &blockDeserializer{},
		QCDeserializer:      &testQCDeserializer{t: t},
	}

	storage.Index(newTestBlock(ProtocolMetadata{Seq: 0, Round: 0, Epoch: 0}), FinalizationCertificate{})
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
	notarizationRecord, err := newNotarizationRecord(l, sigAggregrator, block, nodes[0:quorum])
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

	committedData := storage.data[1].Block.Bytes()
	require.Equal(t, block.Bytes(), committedData)
	require.Equal(t, uint64(2), e.Storage.Height())
}

// TestWalCreated tests that the epoch correctly writes to the WAL
func TestWalCreatedProperly(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	signatureAggregator := &testSignatureAggregator{}
	qd := &testQCDeserializer{t: t}
	wal := newTestWAL(t)
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: signatureAggregator,
		QCDeserializer:      qd,
		BlockDeserializer:   &blockDeserializer{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	// ensure no records are written to the WAL
	records, err := e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 0)

	require.NoError(t, e.Start())

	// ensure a block record is written to the WAL
	wal.assertWALSize(1)
	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 1)
	blockFromWal, err := BlockFromRecord(conf.BlockDeserializer, records[0])
	require.NoError(t, err)
	block := <-bb.out
	require.Equal(t, blockFromWal, block)

	// start at one since our node has already voted
	for i := 1; i < quorum; i++ {
		injectTestVote(t, e, block, nodes[i])
	}

	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)
	expectedNotarizationRecord, err := newNotarizationRecord(l, signatureAggregator, block, nodes[0:quorum])
	require.NoError(t, err)
	require.Equal(t, expectedNotarizationRecord, records[1])

	for i := 1; i < quorum; i++ {
		injectTestFinalization(t, e, block, nodes[i])
	}

	// we do not append the finalization record to the WAL if it for the next expected sequence
	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)

	committedData := storage.data[0].Block.Bytes()
	require.Equal(t, block.Bytes(), committedData)
}

// TestWalWritesBlockRecord tests that the epoch correctly writes to the WAL
// a block proposed by a node other than the epoch node
func TestWalWritesBlockRecord(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()
	blockDeserializer := &blockDeserializer{}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	wal := newTestWAL(t)
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[1], // nodes[1] is not the leader for the first round
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
		BlockDeserializer:   blockDeserializer,
	}

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

	block := <-bb.out
	// send epoch node this block
	vote, err := newTestVote(block, nodes[0])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[0])
	require.NoError(t, err)

	// ensure a block record is written to the WAL
	wal.assertWALSize(1)
	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 1)
	blockFromWal, err := BlockFromRecord(blockDeserializer, records[0])
	require.NoError(t, err)
	require.Equal(t, block, blockFromWal)
}

func TestWalWritesFinalizationCert(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()
	sigAggregrator := &testSignatureAggregator{}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	wal := newTestWAL(t)
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: sigAggregrator,
		BlockDeserializer:   &blockDeserializer{},
		QCDeserializer:      &testQCDeserializer{t: t},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())
	firstBlock := <-bb.out
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
	expectedNotarizationRecord, err := newNotarizationRecord(l, sigAggregrator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	require.Equal(t, expectedNotarizationRecord, records[1])

	// send and notarize a second block
	require.Equal(t, uint64(1), e.Metadata().Round)
	md := e.Metadata()
	md.Seq++
	md.Prev = firstBlock.BlockHeader().Digest
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	secondBlock := <-bb.out

	// increase the round but don't index storage
	require.Equal(t, uint64(1), e.Metadata().Round)
	require.Equal(t, uint64(0), e.Storage.Height())

	vote, err := newTestVote(secondBlock, nodes[1])
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

	wal.assertWALSize(4)

	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 4)
	blockFromWal, err = BlockFromRecord(conf.BlockDeserializer, records[2])
	require.NoError(t, err)
	require.Equal(t, secondBlock, blockFromWal)
	expectedNotarizationRecord, err = newNotarizationRecord(l, sigAggregrator, secondBlock, nodes[0:quorum])
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
	_, expectedFinalizationRecord := newFinalizationRecord(t, l, sigAggregrator, secondBlock, nodes[0:quorum])
	require.NoError(t, err)
	require.Equal(t, expectedFinalizationRecord, records[4])

	// ensure the finalization certificate is not indexed
	require.Equal(t, uint64(2), e.Metadata().Round)
	require.Equal(t, uint64(0), e.Storage.Height())
}

// TestRecoverFromMultipleRounds tests that the epoch can recover from a wal with multiple rounds written to it.
func TestRecoverFromMultipleRounds(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	wal := wal.NewMemWAL(t)
	storage := newInMemStorage()
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	sigAggregrator := &testSignatureAggregator{}
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: sigAggregrator,
		BlockDeserializer:   &blockDeserializer{},
		QCDeserializer:      &testQCDeserializer{t: t},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	protocolMetadata := e.Metadata()
	firstBlock, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	record := BlockRecord(firstBlock.BlockHeader(), firstBlock.Bytes())
	wal.Append(record)

	firstNotarizationRecord, err := newNotarizationRecord(l, sigAggregrator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	wal.Append(firstNotarizationRecord)

	_, finalizationRecord := newFinalizationRecord(t, l, sigAggregrator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	wal.Append(finalizationRecord)

	// when we start we should recover to round 1
	err = e.Start()
	require.NoError(t, err)

	// Verify the epoch recovered to the correct round
	require.Equal(t, uint64(1), e.Metadata().Round)
	require.Equal(t, uint64(1), e.Storage.Height())
	require.Equal(t, firstBlock.Bytes(), storage.data[0].Block.Bytes())
}

// TestRecoverFromMultipleRounds tests that the epoch can recover from a wal with multiple rounds written to it.
// Appends to the wal -> block, notarization, second block, notarization block 2, finalization for block 2.
func TestRecoverFromMultipleNotarizations(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	wal := wal.NewMemWAL(t)
	storage := newInMemStorage()
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	sigAggregrator := &testSignatureAggregator{}
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: sigAggregrator,
		BlockDeserializer:   &blockDeserializer{},
		QCDeserializer:      &testQCDeserializer{t: t},
	}

	// Create first block and write to WAL
	e, err := NewEpoch(conf)
	require.NoError(t, err)

	protocolMetadata := e.Metadata()
	firstBlock, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	record := BlockRecord(firstBlock.BlockHeader(), firstBlock.Bytes())
	wal.Append(record)

	firstNotarizationRecord, err := newNotarizationRecord(l, sigAggregrator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	wal.Append(firstNotarizationRecord)

	protocolMetadata.Round = 1
	protocolMetadata.Seq = 1
	secondBlock, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	record = BlockRecord(secondBlock.BlockHeader(), secondBlock.Bytes())
	wal.Append(record)

	// Add notarization for second block
	secondNotarizationRecord, err := newNotarizationRecord(l, sigAggregrator, secondBlock, nodes[0:quorum])
	require.NoError(t, err)
	wal.Append(secondNotarizationRecord)

	// Create finalization record for first block
	fCert2, finalizationRecord := newFinalizationRecord(t, l, sigAggregrator, secondBlock, nodes[0:quorum])
	wal.Append(finalizationRecord)

	err = e.Start()
	require.NoError(t, err)

	require.Equal(t, uint64(2), e.Metadata().Round)
	require.Equal(t, uint64(0), e.Storage.Height())

	// now if we send fCert for block 1, we should index both 1 & 2
 	fCert1, _ := newFinalizationRecord(t, l, sigAggregrator, firstBlock, nodes[0:quorum])
	err = e.HandleMessage(&Message{
		FinalizationCertificate: &fCert1,
	}, nodes[1])
	require.NoError(t, err)

	require.Equal(t, uint64(2), e.Storage.Height())
	require.Equal(t, firstBlock.Bytes(), storage.data[0].Block.Bytes())
	require.Equal(t, secondBlock.Bytes(), storage.data[1].Block.Bytes())
	require.Equal(t, fCert1, storage.data[0].FinalizationCertificate)
	require.Equal(t, fCert2, storage.data[1].FinalizationCertificate)
}

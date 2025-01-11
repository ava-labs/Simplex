// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"context"
	. "simplex"
	"simplex/wal"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRecoverFromWALProposed tests that the epoch can recover from
// a wal with a single block record written to it(that we have proposed).
func TestRecoverFromWALProposed(t *testing.T) {
	l := makeLogger(t, 1)
	bb := make(testBlockBuilder, 1)
	wal := &wal.InMemWAL{}
	storage := newInMemStorage()
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	conf := EpochConfig{
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
	wal.Append(record)

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
		}

		block := <-bb
		if rounds == 0 {
			require.Equal(t, firstBlock, block)
		}

		if !isEpochNode {
			// send node a message from the leader
			vote, err := newVote(block, leader, conf.Signer)
			require.NoError(t, err)
			e.HandleMessage(&Message{
				BlockMessage: &BlockMessage{
					Vote:  *vote,
					Block: block,
				},
			}, leader)
		}

		// start at one since our node has already voted
		for i := 1; i < quorum; i++ {
			injectVote(t, e, block, nodes[i], conf.Signer)
		}

		for i := 1; i < quorum; i++ {
			injectFinalization(t, e, block, nodes[i])
		}

		committedData := storage.data[i].Block.Bytes()
		require.Equal(t, block.Bytes(), committedData)
	}

	require.Equal(t, rounds, e.Storage.Height())
}

// TestRecoverFromWALNotarized tests that the epoch can recover from a wal
// with a block record written to it, and a notarization record.
func TestRecoverFromNotarization(t *testing.T) {
	l := makeLogger(t, 1)
	bb := make(testBlockBuilder, 1)
	wal := &wal.InMemWAL{}
	storage := newInMemStorage()
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	sigAggregrator := &testSignatureAggregator{}
	conf := EpochConfig{
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
	record := BlockRecord(block.BlockHeader(), block.Bytes())

	// write block record to wal
	wal.Append(record)

	// lets add some notarizations
	notarizationRecord, err := newNotarizationRecord(sigAggregrator, block, nodes[0:quorum], conf.Signer)
	require.NoError(t, err)

	// when we start this we should kickoff the finalization process by broadcasting a finalization message and then waiting for incoming finalization messages
	wal.Append(notarizationRecord)

	records, err := wal.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)
	require.Equal(t, record, records[0])
	require.Equal(t, notarizationRecord, records[1])

	require.Equal(t, uint64(0), e.Metadata().Round)
	err = e.Start()
	require.NoError(t, err)

	// require the round was incremented(notarization increases round)
	require.Equal(t, uint64(1), e.Metadata().Round)

	// type assert block to testBlock
	testBlock := block.(*testBlock)
	for i := 1; i < quorum; i++ {
		injectFinalization(t, e, testBlock, nodes[i])
	}

	committedData := storage.data[0].Block.Bytes()
	require.Equal(t, block.Bytes(), committedData)
	require.Equal(t, uint64(1), e.Storage.Height())
}

// TestRecoverFromWALFinalized tests that the epoch can recover from a wal
// with a block already stored in the storage
func TestRecoverFromWalWithStorage(t *testing.T) {
	l := makeLogger(t, 1)
	bb := make(testBlockBuilder, 1)
	wal := &wal.InMemWAL{}
	storage := newInMemStorage()
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	sigAggregrator := &testSignatureAggregator{}
	conf := EpochConfig{
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
	wal.Append(record)

	// lets add some notarizations
	notarizationRecord, err := newNotarizationRecord(sigAggregrator, block, nodes[0:quorum], conf.Signer)
	require.NoError(t, err)

	wal.Append(notarizationRecord)

	records, err := wal.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)
	require.Equal(t, record, records[0])
	require.Equal(t, notarizationRecord, records[1])

	err = e.Start()
	require.NoError(t, err)

	// require the round was incremented(notarization increases round)
	require.Equal(t, uint64(2), e.Metadata().Round)

	testBlock := block.(*testBlock)
	for i := 1; i < quorum; i++ {
		// type assert block to testBlock
		injectFinalization(t, e, testBlock, nodes[i])
	}

	committedData := storage.data[1].Block.Bytes()
	require.Equal(t, block.Bytes(), committedData)
	require.Equal(t, uint64(2), e.Storage.Height())
}

// TestWalCreated tests that the epoch correctly writes to the WAL
func TestWalCreatedProperly(t *testing.T) {
	l := makeLogger(t, 1)
	bb := make(testBlockBuilder, 1)
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	signatureAggregator := &testSignatureAggregator{}
	qd := &testQCDeserializer{t: t}
	conf := EpochConfig{
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 &wal.InMemWAL{},
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
	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 1)
	blockFromWal, err := BlockFromRecord(conf.BlockDeserializer, records[0])
	require.NoError(t, err)
	block := <-bb
	require.Equal(t, blockFromWal, block)

	// start at one since our node has already voted
	for i := 1; i < quorum; i++ {
		injectVote(t, e, block, nodes[i], conf.Signer)
	}

	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)
	// expectedNotarizationRecord, err := newNotarizationRecord(signatureAggregator, block, nodes[0:quorum], conf.Signer)
	// require.NoError(t, err)
	// require.Equal(t, expectedNotarizationRecord, records[1])

	for i := 1; i < quorum; i++ {
		injectFinalization(t, e, block, nodes[i])
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
	l := makeLogger(t, 1)
	bb := make(testBlockBuilder, 1)
	storage := newInMemStorage()
	blockDeserializer := &blockDeserializer{}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf := EpochConfig{
		Logger:              l,
		ID:                  nodes[1], // nodes[1] is not the leader for the first round
		Signer:              &testSigner{},
		WAL:                 &wal.InMemWAL{},
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

	block := <-bb
	// send epoch node this block
	vote, err := newVote(block, nodes[0], &testSigner{})
	require.NoError(t, err)
	e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[0])

	// ensure a block record is written to the WAL
	records, err = e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 1)
	blockFromWal, err := BlockFromRecord(blockDeserializer, records[0])
	require.NoError(t, err)
	require.Equal(t, block, blockFromWal)
}

func TestWalWritesFinalizationCert(t *testing.T) {
	l := makeLogger(t, 1)
	bb := make(testBlockBuilder, 1)
	storage := newInMemStorage()
	sigAggregrator := &testSignatureAggregator{}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf := EpochConfig{
		Logger:              l,
		ID:                  nodes[1],
		Signer:              &testSigner{},
		WAL:                 &wal.InMemWAL{},
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: sigAggregrator,
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	records, err := e.WAL.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 0)

	require.NoError(t, e.Start())

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	block := <-bb

	md2 := md
	md2.Seq = 1
	md2.Round = 1
	md2.Prev = block.BlockHeader().Digest
	_, ok = bb.BuildBlock(context.Background(), md2)
	require.True(t, ok)

	secondBlock := <-bb
	// create the finalization cert for round 2
	fCert, _, err := newFinalizationRecord(sigAggregrator, secondBlock, nodes[1:])
	require.NoError(t, err)
	// send the fcert message
	fCertMessage := &Message{FinalizationCertificate: &fCert}
	e.HandleMessage(fCertMessage, nodes[1])

	// TODO: uncomment when https://github.com/ava-labs/Simplex/pull/38 is merged
	// cannot check here since we currently do not accept fCerts from rounds we do not know
	// ensure a finalization cert is written to the WAL
	// records, err = e.WAL.ReadAll()
	// require.NoError(t, err)
	// require.Len(t, records, 1)
	// require.Equal(t, recordBytes, records[0])
}

// TestRecoverFromMultipleRounds tests that the epoch can recover from a wal with multiple rounds written to it.
func TestRecoverFromMultipleRounds(t *testing.T) {
	l := makeLogger(t, 1)
	bb := make(testBlockBuilder, 1)
	wal := &wal.InMemWAL{}
	storage := newInMemStorage()
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	sigAggregrator := &testSignatureAggregator{}
	conf := EpochConfig{
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

	firstNotarizationRecord, err := newNotarizationRecord(sigAggregrator, firstBlock, nodes[0:quorum], conf.Signer)
	require.NoError(t, err)
	wal.Append(firstNotarizationRecord)

	_, finalizationRecord, err := newFinalizationRecord(sigAggregrator, firstBlock, nodes[0:quorum])
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
// Appends to the wal -> block, notarization, second block, notarization block 2, finalization for block 1.
func TestRecoverFromMultipleNotarizations(t *testing.T) {
	l := makeLogger(t, 1)
	bb := make(testBlockBuilder, 1)
	wal := &wal.InMemWAL{}
	storage := newInMemStorage()
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	sigAggregrator := &testSignatureAggregator{}
	conf := EpochConfig{
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

	firstNotarizationRecord, err := newNotarizationRecord(sigAggregrator, firstBlock, nodes[0:quorum], conf.Signer)
	require.NoError(t, err)
	wal.Append(firstNotarizationRecord)

	protocolMetadata.Round = 2
	secondBlock, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	record = BlockRecord(secondBlock.BlockHeader(), secondBlock.Bytes())
	wal.Append(record)

	// Add notarization for second block
	secondNotarizationRecord, err := newNotarizationRecord(sigAggregrator, secondBlock, nodes[0:quorum], conf.Signer)
	require.NoError(t, err)
	wal.Append(secondNotarizationRecord)

	// Create finalization record for first block
	_, finalizationRecord, err := newFinalizationRecord(sigAggregrator, firstBlock, nodes[0:quorum])
	require.NoError(t, err)
	wal.Append(finalizationRecord)

	err = e.Start()
	require.NoError(t, err)

	require.Equal(t, uint64(3), e.Metadata().Round)
	// require that we correctly persisted block 1 to storage
	require.Equal(t, uint64(1), e.Storage.Height())
	committedData := storage.data[0].Block.Bytes()
	require.Equal(t, firstBlock.Bytes(), committedData)
}

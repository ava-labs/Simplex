// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/binary"
	"fmt"
	. "simplex"
	"simplex/wal"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestEpochSimpleFlow(t *testing.T) {
	l := makeLogger(t, 1)
	bb := make(testBlockBuilder, 1)
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	conf := EpochConfig{
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 &wal.InMemWAL{},
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	rounds := uint64(100)
	for i := uint64(0); i < rounds; i++ {
		// leader is the proposer of the new block for the given round
		leader := LeaderForRound(nodes, i)
		// only create blocks if we are not the node running the epoch
		isEpochNode := leader.Equals(e.ID)
		if !isEpochNode {
			md := e.Metadata()
			_, ok := bb.BuildBlock(context.Background(), md)
			require.True(t, ok)
		}

		block := <-bb

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
}

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

// TestWalCreated tests that the epoch correctly writes to the WAL
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


// TestRecoveryOutOfOrder tests that the epoch can recover from a wal with out of order records
// block -> notarization -> notarization round 2 ->
func TestRecoveryFromWalOutOfOrder(t *testing.T) {
}

func makeLogger(t *testing.T, node int) *testLogger {
	logger, err := zap.NewDevelopment(zap.AddCallerSkip(1))
	require.NoError(t, err)

	logger = logger.With(zap.Int("node", node))
	l := &testLogger{Logger: logger}
	return l
}

func newVote(block *testBlock, id NodeID, signer Signer) (*Vote, error) {
	vote := ToBeSignedVote{
		BlockHeader: block.BlockHeader(),
	}
	sig, err := vote.Sign(signer)
	if err != nil {
		return nil, err
	}

	return &Vote{
		Signature: Signature{
			Signer: id,
			Value:  sig,
		},
		Vote: vote,
	}, nil
}

func injectVote(t *testing.T, e *Epoch, block *testBlock, id NodeID, signer Signer) {
	vote, err := newVote(block, id, signer)
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		VoteMessage: vote,
	}, id)
	require.NoError(t, err)
}

func newFinalization(block *testBlock, id NodeID) *Finalization {
	return &Finalization{
		Signature: Signature{
			Signer: id,
		},
		Finalization: ToBeSignedFinalization{
			BlockHeader: block.BlockHeader(),
		},
	}
}

func injectFinalization(t *testing.T, e *Epoch, block *testBlock, id NodeID) {
	err := e.HandleMessage(&Message{
		Finalization: newFinalization(block, id),
	}, id)
	require.NoError(t, err)
}

type testLogger struct {
	*zap.Logger
}

func (tl *testLogger) Trace(msg string, fields ...zap.Field) {
	tl.Log(zapcore.DebugLevel, msg, fields...)
}

func (tl *testLogger) Verbo(msg string, fields ...zap.Field) {
	tl.Log(zapcore.DebugLevel, msg, fields...)
}

// TODO: this isn't used, but it is needed for crash recovery
type testQCDeserializer struct {
	t *testing.T
}

func (t *testQCDeserializer) DeserializeQuorumCertificate(bytes []byte) (QuorumCertificate, error) {
	var qc []Signature
	rest, err := asn1.Unmarshal(bytes, &qc)
	require.NoError(t.t, err)
	require.Empty(t.t, rest)
	return testQC(qc), err
}

type testSignatureAggregator struct {
}

func (t *testSignatureAggregator) Aggregate(signatures []Signature) (QuorumCertificate, error) {
	return testQC(signatures), nil
}

type testQC []Signature

func (t testQC) Signers() []NodeID {
	res := make([]NodeID, 0, len(t))
	for _, sig := range t {
		res = append(res, sig.Signer)
	}
	return res
}

func (t testQC) Verify(msg []byte) error {
	return nil
}

func (t testQC) Bytes() []byte {
	bytes, err := asn1.Marshal(t)
	if err != nil {
		panic(err)
	}
	return bytes
}

type testSigner struct {
}

func (t *testSigner) Sign([]byte) ([]byte, error) {
	return []byte{1, 2, 3}, nil
}

type testVerifier struct {
}

func (t *testVerifier) VerifyBlock(Block) error {
	return nil
}

func (t *testVerifier) Verify(_ []byte, _ []byte, _ NodeID) error {
	return nil
}

type noopComm []NodeID

func (n noopComm) ListNodes() []NodeID {
	return n
}

func (n noopComm) SendMessage(*Message, NodeID) {

}

func (n noopComm) Broadcast(msg *Message) {

}

type testBlockBuilder chan *testBlock

// BuildBlock builds a new testblock and sends it to the BlockBuilder channel
func (t testBlockBuilder) BuildBlock(_ context.Context, metadata ProtocolMetadata) (Block, bool) {
	tb := newTestBlock(metadata)

	select {
	case t <- tb:
	default:
	}

	return tb, true
}

func (t testBlockBuilder) IncomingBlock(ctx context.Context) {
	panic("should not be invoked")
}

type testBlock struct {
	data     []byte
	metadata ProtocolMetadata
	digest   [32]byte
}

func (tb *testBlock) Verify() error {
	return nil
}

func newTestBlock(metadata ProtocolMetadata) *testBlock {
	tb := testBlock{
		metadata: metadata,
		data:     make([]byte, 32),
	}

	_, err := rand.Read(tb.data)
	if err != nil {
		panic(err)
	}

	tb.computeDigest()

	return &tb
}

func (tb *testBlock) computeDigest() {
	var bb bytes.Buffer
	bb.Write(tb.Bytes())
	tb.digest = sha256.Sum256(bb.Bytes())
}

func (t *testBlock) BlockHeader() BlockHeader {
	return BlockHeader{
		ProtocolMetadata: t.metadata,
		Digest:           t.digest,
	}
}

func (t *testBlock) Bytes() []byte {
	bh := BlockHeader{
		ProtocolMetadata: t.metadata,
	}

	mdBuff := bh.Bytes()

	buff := make([]byte, len(t.data)+len(mdBuff)+4)
	binary.BigEndian.PutUint32(buff, uint32(len(t.data)))
	copy(buff[4:], t.data)
	copy(buff[4+len(t.data):], mdBuff)
	return buff
}

type InMemStorage struct {
	data map[uint64]struct {
		Block
		FinalizationCertificate
	}

	lock   sync.Mutex
	signal sync.Cond
}

func newInMemStorage() *InMemStorage {
	s := &InMemStorage{
		data: make(map[uint64]struct {
			Block
			FinalizationCertificate
		}),
	}

	s.signal = *sync.NewCond(&s.lock)

	return s
}

func (mem *InMemStorage) waitForBlockCommit(seq uint64) {
	mem.lock.Lock()
	defer mem.lock.Unlock()

	for {
		if _, exists := mem.data[seq]; exists {
			return
		}

		mem.signal.Wait()
	}
}

func (mem *InMemStorage) Height() uint64 {
	return uint64(len(mem.data))
}

func (mem *InMemStorage) Retrieve(seq uint64) (Block, FinalizationCertificate, bool) {
	item, ok := mem.data[seq]
	if !ok {
		return nil, FinalizationCertificate{}, false
	}
	return item.Block, item.FinalizationCertificate, true
}

func (mem *InMemStorage) Index(block Block, certificate FinalizationCertificate) {
	mem.lock.Lock()
	defer mem.lock.Unlock()

	seq := block.BlockHeader().Seq

	_, ok := mem.data[seq]
	if ok {
		panic(fmt.Sprintf("block with seq %d already indexed!", seq))
	}
	mem.data[seq] = struct {
		Block
		FinalizationCertificate
	}{block,
		certificate,
	}

	mem.signal.Signal()
}

type blockDeserializer struct {
}

func (b *blockDeserializer) DeserializeBlock(buff []byte) (Block, error) {
	blockLen := binary.BigEndian.Uint32(buff[:4])
	bh := BlockHeader{}
	if err := bh.FromBytes(buff[4+blockLen:]); err != nil {
		return nil, err
	}

	tb := testBlock{
		data:     buff[4 : 4+blockLen],
		metadata: bh.ProtocolMetadata,
	}

	tb.computeDigest()

	return &tb, nil
}

func TestBlockDeserializer(t *testing.T) {
	var blockDeserializer blockDeserializer

	tb := newTestBlock(ProtocolMetadata{Seq: 1, Round: 2, Epoch: 3})
	tb2, err := blockDeserializer.DeserializeBlock(tb.Bytes())
	require.NoError(t, err)
	require.Equal(t, tb, tb2)
}

func TestQuorum(t *testing.T) {
	for _, testCase := range []struct {
		n int
		f int
		q int
	}{
		{
			n: 1, f: 0,
			q: 1,
		},
		{
			n: 2, f: 0,
			q: 2,
		},
		{
			n: 3, f: 0,
			q: 2,
		},
		{
			n: 4, f: 1,
			q: 3,
		},
		{
			n: 5, f: 1,
			q: 4,
		},
		{
			n: 6, f: 1,
			q: 4,
		},
		{
			n: 7, f: 2,
			q: 5,
		},
		{
			n: 8, f: 2,
			q: 6,
		},
		{
			n: 9, f: 2,
			q: 6,
		},
		{
			n: 10, f: 3,
			q: 7,
		},
		{
			n: 11, f: 3,
			q: 8,
		},
		{
			n: 12, f: 3,
			q: 8,
		},
	} {
		t.Run(fmt.Sprintf("%d", testCase.n), func(t *testing.T) {
			require.Equal(t, testCase.q, Quorum(testCase.n))
		})
	}
}

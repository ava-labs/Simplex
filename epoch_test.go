// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	. "simplex"
	"simplex/record"
	"simplex/wal"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestEpochSimpleFlow(t *testing.T) {
	l := makeLogger(t)
	bb := make(testBlockBuilder, 1)
	storage := make(InMemStorage)
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	e := &Epoch{
		BlockDigester: blockDigester{},
		Logger:        l,
		ID:            NodeID{1},
		Signer:        &testSigner{},
		WAL:           &wal.InMemWAL{},
		Verifier:      &testVerifier{},
		BlockVerifier: &testVerifier{},
		Storage:       storage,
		Comm:          noopComm(nodes),
		BlockBuilder:  bb,
	}
	err := e.Start()
	require.NoError(t, err)
	
	rounds := uint64(100)
	for i := uint64(0); i < rounds; i++ {
		// leader ID is the proposer of the new block
		leader := LeaderForRound(nodes, uint64(i))
		isEpochNode := string(leader) == string(e.ID)
		// only create a block if we are not the node running the epoch
		// because the node running the epoch will propose the block when it receives the finalization messages(and is the leader)
		if !isEpochNode {
			md := e.Metadata()
			_, ok := bb.BuildBlock(context.Background(), md)
			require.True(t, ok)
		}

		block := <-bb

		if !isEpochNode {
			// send node a message from the leader
			e.HandleMessage(&Message{
				BlockMessage: &BlockMessage{
					Block: block,
				},
			}, leader)
		}

		// start at one since our node has already voted
		for i := 1; i < quorum; i++ {
			injectVote(e, block, nodes[i])
		}
		
		// now we have reached a quorum since nodes 1(the one running the epoch) and 2, 3 have voted for the block
		// this will kickoff the next round and have the new block proposed
		for i := 1; i < quorum; i++ {
			injectFinalization(e, block, nodes[i])
		}

		committedData := storage[i].Block.Bytes()
		require.Equal(t, block.Bytes(), committedData)
	}

	require.Equal(t, rounds, e.Storage.Height())
}

// Crashes after writing a block record to WAL
func TestRecoverFromWALProposed(t *testing.T) {
	// write block record to wal, 
	// start epoch with wal
	l := makeLogger(t)
	ctx := context.Background()
	bb := make(testBlockBuilder, 1)
	storage := make(InMemStorage)
	wal := &wal.InMemWAL{}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	e := &Epoch{
		BlockDigester: blockDigester{},
		BlockDeserializer: &blockDeserializer{},
		Logger:        l,
		ID:            NodeID{1},
		Signer:        &testSigner{},
		WAL:           wal,
		Verifier:      &testVerifier{},
		BlockVerifier: &testVerifier{},
		Storage:       storage,
		Comm:          noopComm(nodes),
		BlockBuilder:  bb,
	}

	// seems like we duplicate the block metadata here
	protocolMetadata := e.Metadata()
	block, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	record := BlockRecord(block.Metadata(), block.Bytes())

	// write block record to wal
	wal.Append(&record)

	// this check must be commented out since inMemWal consumes the record
	// records := wal.ReadAll()
	// require.Len(t, records, 1)
	// require.Equal(t, record, records[0])

	err := e.Start()
	require.NoError(t, err)

	rounds := uint64(100)
	for i := uint64(0); i < rounds; i++ {
		leader := LeaderForRound(nodes, uint64(i))
		isEpochNode := string(leader) == string(e.ID)
		if !isEpochNode {
			md := e.Metadata()
			_, ok := bb.BuildBlock(context.Background(), md)
			require.True(t, ok)
		}

		block := <-bb

		if !isEpochNode {
			e.HandleMessage(&Message{
				BlockMessage: &BlockMessage{
					Block: block,
				},
			}, leader)
		}

		for i := 1; i < quorum; i++ {
			injectVote(e, block, nodes[i])
		}
		
		for i := 1; i < quorum; i++ {
			injectFinalization(e, block, nodes[i])
		}

		committedData := storage[i].Block.Bytes()
		require.Equal(t, block.Bytes(), committedData)
	}

	require.Equal(t, rounds, e.Storage.Height())
}


// Crashes after writing a block record to WAL
func TestRecoverFromNotarization(t *testing.T) {
	// write block record to wal, 
	// start epoch with wal
	l := makeLogger(t)
	ctx := context.Background()
	bb := make(testBlockBuilder, 1)
	storage := make(InMemStorage)
	wal := &wal.InMemWAL{}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	e := &Epoch{
		BlockDigester: blockDigester{},
		BlockDeserializer: &blockDeserializer{},
		Logger:        l,
		ID:            NodeID{1},
		Signer:        &testSigner{},
		WAL:           wal,
		Verifier:      &testVerifier{},
		BlockVerifier: &testVerifier{},
		Storage:       storage,
		Comm:          noopComm(nodes),
		BlockBuilder:  bb,
	}

	// seems like we duplicate the block metadata here
	protocolMetadata := e.Metadata()
	block, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	record := BlockRecord(block.Metadata(), block.Bytes())

	// write block record to wal
	wal.Append(&record)

	// lets add some notarizations
	notarizationRecord, err := newNotarizationRecord(e, block, nodes[0:quorum])
	require.NoError(t, err)

	// when we start this we should kickoff the finalization process by broadcasting a finalization message and then waiting for incoming finalization messages
	wal.Append(&notarizationRecord)

	// this check must be commented out since inMemWal consumes the record
	// records := wal.ReadAll()
	// require.Len(t, records, 1)
	// require.Equal(t, record, records[0])

	err = e.Start()
	require.NoError(t, err)

	for i := 1; i < quorum; i++ {
		// type assert block to testBlock
		testBlock := block.(*testBlock)
		injectFinalization(e, testBlock, nodes[i])
	}


	committedData := storage[0].Block.Bytes()
	require.Equal(t, block.Bytes(), committedData)

	require.Equal(t, uint64(1), e.Storage.Height())
}


// Crashes after writing a block record to WAL
func TestRecoverFromMultipleRounds(t *testing.T) {
	// write block record to wal, 
	// start epoch with wal
	l := makeLogger(t)
	ctx := context.Background()
	bb := make(testBlockBuilder, 1)
	storage := make(InMemStorage)
	wal := &wal.InMemWAL{}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	e := &Epoch{
		BlockDigester: blockDigester{},
		BlockDeserializer: &blockDeserializer{},
		Logger:        l,
		ID:            NodeID{1},
		Signer:        &testSigner{},
		WAL:           wal,
		Verifier:      &testVerifier{},
		BlockVerifier: &testVerifier{},
		Storage:       storage,
		Comm:          noopComm(nodes),
		BlockBuilder:  bb,
	}

	// seems like we duplicate the block metadata here
	protocolMetadata := e.Metadata()
	block, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	record := BlockRecord(block.Metadata(), block.Bytes())

	// write block record to wal
	wal.Append(&record)

	// lets add some notarizations
	notarizationRecord, err := newNotarizationRecord(e, block, nodes[0:quorum])
	require.NoError(t, err)

	// when we start this we should kickoff the finalization process by broadcasting a finalization message and then waiting for incoming finalization messages
	wal.Append(&notarizationRecord)

	// we only store in the wal a finalization for a sequence in the future
	futureMetadata := e.Metadata()
	futureMetadata.Seq = 1
	secondBlock := newTestBlock(futureMetadata)
	finalizationRecord, err := newFinalizationRecord(e, secondBlock, nodes[0:quorum])
	require.NoError(t, err)

	// write finalization record to wal
	wal.Append(&finalizationRecord)
	// this check must be commented out since inMemWal consumes the record
	// records := wal.ReadAll()
	// require.Len(t, records, 1)
	// require.Equal(t, record, records[0])
	
	err = e.Start()
	require.NoError(t, err)

	// inject so we can move onto seq 1
	for i := 1; i < quorum; i++ {
		testBlock := block.(*testBlock)
		injectFinalization(e, testBlock, nodes[i])
	}

	err = e.Start()
	require.NoError(t, err)
	require.Equal(t, uint64(1), e.Storage.Height())


	// rounds := uint64(100)
	// for i := uint64(1); i < rounds; i++ {
	// 	leader := LeaderForRound(nodes, uint64(i))
	// 	isEpochNode := string(leader) == string(e.ID)
	// 	if !isEpochNode {
	// 		md := e.Metadata()
	// 		_, ok := bb.BuildBlock(context.Background(), md)
	// 		require.True(t, ok)
	// 	}

	// 	block := <-bb

	// 	if !isEpochNode {
	// 		e.HandleMessage(&Message{
	// 			BlockMessage: &BlockMessage{
	// 				Block: block,
	// 			},
	// 		}, leader)
	// 	}

	// 	for i := 1; i < quorum; i++ {
	// 		injectVote(e, block, nodes[i])
	// 	}
		
	// 	for i := 1; i < quorum; i++ {
	// 		injectFinalization(e, block, nodes[i])
	// 	}

	// 	committedData := storage[i].Block.Bytes()
	// 	require.Equal(t, block.Bytes(), committedData)
	// }

	// require.Equal(t, rounds, e.Storage.Height())
}


// We write to the WAL when someone else proposes a block. lets crash after that happens and try to recover
func TestRecoverFromWalProposed(t *testing.T) {{
// write block record to wal, 
	// start epoch with wal
	l := makeLogger(t)
	ctx := context.Background()
	bb := make(testBlockBuilder, 1)
	storage := make(InMemStorage)
	wal := &wal.InMemWAL{}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	// quorum := Quorum(len(nodes))

	e := &Epoch{
		BlockDigester: blockDigester{},
		BlockDeserializer: &blockDeserializer{},
		Logger:        l,
		ID:            NodeID{1},
		Signer:        &testSigner{},
		WAL:           wal,
		Verifier:      &testVerifier{},
		BlockVerifier: &testVerifier{},
		Storage:       storage,
		Comm:          noopComm(nodes),
		BlockBuilder:  bb,
	}

	// seems like we duplicate the block metadata here
	protocolMetadata := e.Metadata()
	block, ok := bb.BuildBlock(ctx, protocolMetadata)
	require.True(t, ok)
	record := BlockRecord(block.Metadata(), block.Bytes())

	// write block record to wal
	wal.Append(&record)
}}

// block -> notariztion -> finalization -> block from other node -> crash
func TestRecoverFromWalMultipleSeq(t *testing.T) {
}

func makeLogger(t *testing.T) *testLogger {
	logger, err := zap.NewDevelopment(zap.AddCallerSkip(1))
	require.NoError(t, err)

	l := &testLogger{Logger: logger}
	return l
}

func newSignedVoteMessage(e *Epoch, block *testBlock, id NodeID) *SignedVoteMessage {
	return &SignedVoteMessage{
		Signer: id,
		Vote: Vote{
			Metadata: block.Metadata(),
		},
	}
}

func injectVote(e *Epoch, block *testBlock, id NodeID) {
	e.HandleMessage(&Message{
		VoteMessage: newSignedVoteMessage(e, block, id),
	}, id)
}

func newNotarizationRecord(e *Epoch, block Block, ids []NodeID) (record.Record, error) {
	if len(ids) < Quorum(len(e.Comm.ListNodes())) {
		return record.Record{}, fmt.Errorf("not enough signers")
	}

	votesForCurrentRound := make(map[string]*SignedVoteMessage)
	for _, id := range ids {
		vote := newSignedVoteMessage(e, &testBlock{}, id)
		votesForCurrentRound[string(id)] = vote
	}
	
	_, signatures, signers, vote := e.NewNotarization(votesForCurrentRound, block.Metadata().Digest)
	record := NewQuorumRecord(signatures, signers, vote.Bytes(), record.NotarizationRecordType)
	
	return record, nil
}

// makes a finalization certificate
func newFinalizationRecord(e *Epoch, block Block, ids []NodeID) (record.Record, error) {
	if len(ids) < Quorum(len(e.Comm.ListNodes())) {
		return record.Record{}, fmt.Errorf("not enough signers")
	}

	finalizations := make([]*SignedFinalizationMessage, len(ids))
	for i, id := range ids {
		finalizations[i] = &SignedFinalizationMessage{
			Signer: id,
			Finalization: Finalization{
				Metadata: block.Metadata(),
			},
		}
	}

	fCert := e.NewFinalizationCertificate(finalizations)
	signatures, signers := e.SignaturesAndSignersFromFinalizationCertificate(fCert)

	record := NewQuorumRecord(signatures, signers, fCert.Finalization.Bytes(), record.FinalizationRecordType)
	
	return record, nil
}

func injectFinalization(e *Epoch, block *testBlock, id NodeID) {
	md := block.Metadata()
	md.Digest = (blockDigester{}).Digest(block)
	e.HandleMessage(&Message{
		Finalization: &SignedFinalizationMessage{
			Signer: id,
			Finalization: Finalization{
				Metadata: md,
			},
		},
	}, id)
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

func (t *testVerifier) Verify(_ []byte, _ []byte, _ ...NodeID) error {
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
	digest   []byte
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

	var digester blockDigester
	tb.digest = digester.Digest(&tb)

	return &tb
}

func (t *testBlock) Metadata() Metadata {
	return Metadata{
		ProtocolMetadata: t.metadata,
		Digest:           t.digest,
	}
}

func (t testBlock) Bytes() []byte {
	md := Metadata{
		ProtocolMetadata: t.metadata,
		// The only thing an application needs to serialize, is the protocol metadata.
		// We have an implementation of Bytes() only in Metadata.
		// Although it's a hack, we pretend we have a digest in order to be able to persist it.
		// In a real implementation, it is up to the application to properly serialize the protocol metadata.
		Digest: make([]byte, 32),
	}

	mdBuff := md.Bytes()

	buff := make([]byte, len(t.data)+len(mdBuff)+4)
	binary.BigEndian.PutUint32(buff, uint32(len(t.data)))
	copy(buff[4:], t.data)
	copy(buff[4+len(t.data):], mdBuff)
	return buff
}

type blockDigester struct {
}

func (b blockDigester) Digest(block Block) []byte {
	var bb bytes.Buffer
	bb.Write(block.Bytes())
	digest := sha256.Sum256(bb.Bytes())
	return digest[:]
}

type InMemStorage map[uint64]struct {
	Block
	FinalizationCertificate
}

func (mem InMemStorage) Height() uint64 {
	return uint64(len(mem))
}

func (mem InMemStorage) Retrieve(seq uint64) (Block, FinalizationCertificate, bool) {
	item, ok := mem[seq]
	if !ok {
		return nil, FinalizationCertificate{}, false
	}
	return item.Block, item.FinalizationCertificate, true
}

func (mem InMemStorage) Index(seq uint64, block Block, certificate FinalizationCertificate) {
	_, ok := mem[seq]
	if ok {
		panic(fmt.Sprintf("block with seq %d already indexed!", seq))
	}
	mem[seq] = struct {
		Block
		FinalizationCertificate
	}{block,
		certificate,
	}
}

type blockDeserializer struct {
}

func (b *blockDeserializer) DeserializeBlock(buff []byte) (Block, error) {
	fmt.Println("in here")
	blockLen := binary.BigEndian.Uint32(buff[:4])
	fmt.Println("blockLen", blockLen)
	md := Metadata{}
	if err := md.FromBytes(buff[4+blockLen:]); err != nil {
		return nil, err
	}


	md.Digest = make([]byte, 32)

	tb := testBlock{
		data:     buff[4 : 4+blockLen],
		metadata: md.ProtocolMetadata,
	}

	var digester blockDigester
	tb.digest = digester.Digest(&tb)
	return &tb, nil
}

func TestBlockDeserializer(t *testing.T) {
	var blockDeserializer blockDeserializer

	tb := newTestBlock(ProtocolMetadata{Seq: 1, Round: 2, Epoch: 3, Prev: make([]byte, 32)})
	tb2, err := blockDeserializer.DeserializeBlock(tb.Bytes())
	require.NoError(t, err)
	require.Equal(t, tb, tb2)
}

func TestQuorum(t *testing.T) {
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	require.Equal(t, 3, quorum)
}
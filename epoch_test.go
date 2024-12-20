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

	e := &Epoch{
		BlockDigester: blockDigester{},
		Logger:        l,
		ID:            NodeID{1},
		Signer:        &testSigner{},
		WAL:           &wal.InMemWAL{},
		Verifier:      &testVerifier{},
		BlockVerifier: &testVerifier{},
		Storage:       storage,
		Comm:          noopComm([]NodeID{{1}, {2}, {3}, {4}}),
		BlockBuilder:  bb,
	}
	err := e.Start()
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		leaderID := i%4 + 1
		shouldPropose := leaderID == 1

		if !shouldPropose {
			md := e.Metadata()
			_, ok := bb.BuildBlock(context.Background(), md)
			require.True(t, ok)
		}

		block := <-bb

		if !shouldPropose {
			e.HandleMessage(&Message{
				BlockMessage: &BlockMessage{
					Block: block,
				},
			}, NodeID{byte(leaderID)})
		}

		injectVote(e, block, NodeID{2})
		injectVote(e, block, NodeID{3})

		injectFinalization(e, block, NodeID{2})
		injectFinalization(e, block, NodeID{3})

		committedData := storage[uint64(i)].Block.Bytes()
		require.Equal(t, block.Bytes(), committedData)
	}
}

func makeLogger(t *testing.T) *testLogger {
	logger, err := zap.NewDevelopment(zap.AddCallerSkip(1))
	require.NoError(t, err)

	l := &testLogger{Logger: logger}
	return l
}

func injectVote(e *Epoch, block *testBlock, id NodeID) {
	e.HandleMessage(&Message{
		VoteMessage: &SignedVoteMessage{
			Signer: id,
			Vote: Vote{
				Metadata: block.Metadata(),
			},
		},
	}, id)
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
	blockLen := binary.BigEndian.Uint32(buff[:4])
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

// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:generate go run github.com/StephenButtolph/canoto/canoto@v0.10.0 --concurrent=false --import=simplex/internal/canoto $GOFILE

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
	quorum := QuorumSize(len(nodes))
	conf := EpochConfig{
		Logger:              l,
		ID:                  NodeID{1},
		Signer:              &testSigner{},
		WAL:                 &wal.InMemWAL{},
		Verifier:            &testVerifier{},
		BlockDeserializer:   &blockDeserializer{},
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
			vote := newVote(block, leader)
			e.HandleMessage(&Message{
				Proposal: &Proposal{
					Vote:  *vote,
					Block: block.Bytes(),
				},
			}, leader)
		}

		// start at one since our node has already voted
		for i := 1; i < quorum; i++ {
			injectVote(t, e, block, nodes[i])
		}

		for i := 1; i < quorum; i++ {
			injectFinalization(t, e, block, nodes[i])
		}

		committedData := storage.data[i].Block.Bytes()
		require.Equal(t, block.Bytes(), committedData)
	}
}

func makeLogger(t *testing.T, node int) *testLogger {
	logger, err := zap.NewDevelopment(zap.AddCallerSkip(1))
	require.NoError(t, err)

	logger = logger.With(zap.Int("node", node))

	l := &testLogger{Logger: logger}
	return l
}

func newVote(block *testBlock, id NodeID) *Vote {
	return &Vote{
		Signature: Signature{
			Signer: id,
		},
		Header: block.BlockHeader(),
	}
}

func injectVote(t *testing.T, e *Epoch, block *testBlock, id NodeID) {
	err := e.HandleMessage(&Message{
		Vote: newVote(block, id),
	}, id)

	require.NoError(t, err)
}

func injectFinalization(t *testing.T, e *Epoch, block *testBlock, id NodeID) {
	md := block.BlockHeader()
	err := e.HandleMessage(&Message{
		Finalize: &Vote{
			Signature: Signature{
				Signer: id,
			},
			Header: md,
		},
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
	var tqc testQC
	err := tqc.UnmarshalCanoto(bytes)
	require.NoError(t.t, err)
	return tqc, err
}

type testSignatureAggregator struct {
}

func (t *testSignatureAggregator) Aggregate(signatures []Signature) (QuorumCertificate, error) {
	return testQC{
		qc: signatures,
	}, nil
}

type testQC struct {
	qc []Signature `canoto:"repeated value,1"`

	canotoData canotoData_testQC
}

func (t testQC) Signers() []NodeID {
	res := make([]NodeID, 0, len(t.qc))
	for _, sig := range t.qc {
		res = append(res, sig.Signer)
	}
	return res
}

func (t testQC) Verify(msg []byte) error {
	return nil
}

func (t testQC) Bytes() []byte {
	return t.MarshalCanoto()
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

	mdBuff := bh.MarshalCanoto()

	buff := make([]byte, len(t.data)+len(mdBuff)+4)
	binary.BigEndian.PutUint32(buff, uint32(len(t.data)))
	copy(buff[4:], t.data)
	copy(buff[4+len(t.data):], mdBuff)
	return buff
}

type InMemStorage struct {
	data map[uint64]struct {
		Block
		Quorum
	}

	lock   sync.Mutex
	signal sync.Cond
}

func newInMemStorage() *InMemStorage {
	s := &InMemStorage{
		data: make(map[uint64]struct {
			Block
			Quorum
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

func (mem *InMemStorage) Retrieve(seq uint64) (Block, Quorum, bool) {
	item, ok := mem.data[seq]
	if !ok {
		return nil, Quorum{}, false
	}
	return item.Block, item.Quorum, true
}

func (mem *InMemStorage) Index(block Block, certificate Quorum) {
	mem.lock.Lock()
	defer mem.lock.Unlock()

	seq := block.BlockHeader().Seq

	_, ok := mem.data[seq]
	if ok {
		panic(fmt.Sprintf("block with seq %d already indexed!", seq))
	}
	mem.data[seq] = struct {
		Block
		Quorum
	}{block,
		certificate,
	}

	mem.signal.Signal()
}

type blockDeserializer struct {
}

func (b *blockDeserializer) DeserializeBlock(buff []byte) (Block, error) {
	blockLen := binary.BigEndian.Uint32(buff[:4])
	var bh BlockHeader
	if err := bh.UnmarshalCanoto(buff[4+blockLen:]); err != nil {
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

func TestQuorumSize(t *testing.T) {
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
			require.Equal(t, testCase.q, QuorumSize(testCase.n))
		})
	}
}

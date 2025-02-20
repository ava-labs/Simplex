// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
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
	"math"
	rand2 "math/rand"
	. "simplex"
	"simplex/testutil"
	"simplex/wal"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestEpochConsecutiveProposalsDoNotGetVerified(t *testing.T) {
	shouldOnlyBeClosedOnce := make(chan struct{})

	l := testutil.MakeLogger(t, 1)
	l.Intercept(func(entry zapcore.Entry) error {
		if entry.Message == "Scheduling new ready task" {
			select {
			case <-shouldOnlyBeClosedOnce:
				require.FailNow(t, "The block verification task should have been scheduled only once")
			default:
				close(shouldOnlyBeClosedOnce)
			}
		}
		return nil
	})

	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	wal := newTestWAL(t)

	nodes := []NodeID{{1}, {2}, {3}, {4}}

	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[1],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	leader := nodes[0]

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	require.Equal(t, md.Round, md.Seq)

	block := <-bb.out

	vote, err := newTestVote(block, leader)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(DefaultMaxPendingBlocks)

	for i := 0; i < DefaultMaxPendingBlocks; i++ {
		go func() {
			defer wg.Done()

			err := e.HandleMessage(&Message{
				BlockMessage: &BlockMessage{
					Vote:  *vote,
					Block: block,
				},
			}, leader)
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	select {
	case <-shouldOnlyBeClosedOnce:
	case <-time.After(time.Minute):
		require.Fail(t, "timeout waiting for shouldOnlyBeClosedOnce")
	}
}

func TestEpochFinalizeThenNotarize(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	wal := newTestWAL(t)

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
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	t.Run("commit without notarization, only with finalization", func(t *testing.T) {
		for round := 0; round < 100; round++ {
			notarizeAndFinalizeRound(t, nodes, uint64(round), uint64(round), e, bb, quorum, storage, true)
			storage.waitForBlockCommit(uint64(round))
		}
	})

	t.Run("notarization after commit without notarizations", func(t *testing.T) {
		// leader is the proposer of the new block for the given round
		leader := LeaderForRound(nodes, uint64(100))
		// only create blocks if we are not the node running the epoch
		if !leader.Equals(e.ID) {
			md := e.Metadata()
			_, ok := bb.BuildBlock(context.Background(), md)
			require.True(t, ok)
		}

		block := <-bb.out

		vote, err := newTestVote(block, nodes[0])
		require.NoError(t, err)
		err = e.HandleMessage(&Message{
			BlockMessage: &BlockMessage{
				Vote:  *vote,
				Block: block,
			},
		}, nodes[0])
		require.NoError(t, err)

		for i := 1; i < quorum; i++ {
			injectTestVote(t, e, block, nodes[i])
		}

		wal.assertNotarization(100)
	})

}

func TestEpochSimpleFlow(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal.NewMemWAL(t),
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
	for round := uint64(0); round < rounds; round++ {
		notarizeAndFinalizeRound(t, nodes, round, round, e, bb, quorum, storage, false)
	}
}

func notarizeAndFinalizeRound(t *testing.T, nodes []NodeID, round, seq uint64, e *Epoch, bb *testBlockBuilder, quorum int, storage *InMemStorage, skipNotarization bool) {
	// leader is the proposer of the new block for the given round
	leader := LeaderForRound(nodes, round)
	// only create blocks if we are not the node running the epoch
	isEpochNode := leader.Equals(e.ID)
	if !isEpochNode {
		md := e.Metadata()
		md.Seq = seq
		_, ok := bb.BuildBlock(context.Background(), md)
		require.True(t, ok)
		require.Equal(t, md.Round, md.Seq)
	}

	block := <-bb.out

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

	if !skipNotarization {
		// start at one since our node has already voted
		for i := 1; i < quorum; i++ {
			// Skip the vote of the block proposer
			if leader.Equals(nodes[i]) {
				continue
			}
			injectTestVote(t, e, block, nodes[i])
		}
	}

	for i := 1; i < quorum; i++ {
		injectTestFinalization(t, e, block, nodes[i])
	}

	if skipNotarization {
		injectTestFinalization(t, e, block, nodes[quorum])
	}

	block2 := storage.waitForBlockCommit(seq)
	require.Equal(t, block, block2)
}

func FuzzEpochInterleavingMessages(f *testing.F) {
	f.Fuzz(func(t *testing.T, seed int64) {
		testEpochInterleavingMessages(t, seed)
	})
}

func TestEpochInterleavingMessages(t *testing.T) {
	buff := make([]byte, 8)

	for i := 0; i < 100; i++ {
		_, err := rand.Read(buff)
		require.NoError(t, err)
		seed := int64(binary.BigEndian.Uint64(buff))
		testEpochInterleavingMessages(t, seed)
	}
}

func testEpochInterleavingMessages(t *testing.T, seed int64) {
	l := testutil.MakeLogger(t, 1)
	rounds := 10

	bb := &testBlockBuilder{in: make(chan *testBlock, rounds)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal.NewMemWAL(t),
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	var protocolMetadata ProtocolMetadata

	callbacks := createCallbacks(t, rounds, protocolMetadata, nodes, e, bb)

	require.NoError(t, e.Start())

	r := rand2.New(rand2.NewSource(seed))
	for i, index := range r.Perm(len(callbacks)) {
		t.Log("Called callback", i, "out of", len(callbacks))
		callbacks[index]()
	}

	for i := 0; i < rounds; i++ {
		t.Log("Waiting for commit of round", i)
		storage.waitForBlockCommit(uint64(i))
	}
}

func createCallbacks(t *testing.T, rounds int, protocolMetadata ProtocolMetadata, nodes []NodeID, e *Epoch, bb *testBlockBuilder) []func() {
	blocks := make([]Block, 0, rounds)

	callbacks := make([]func(), 0, rounds*4+len(blocks))

	for i := 0; i < rounds; i++ {
		block := newTestBlock(protocolMetadata)
		blocks = append(blocks, block)

		protocolMetadata.Seq++
		protocolMetadata.Round++
		protocolMetadata.Prev = block.BlockHeader().Digest

		leader := LeaderForRound(nodes, uint64(i))

		if !leader.Equals(e.ID) {
			vote, err := newTestVote(block, leader)
			require.NoError(t, err)

			callbacks = append(callbacks, func() {
				t.Log("Injecting block", block.BlockHeader().Round)
				e.HandleMessage(&Message{
					BlockMessage: &BlockMessage{
						Block: block,
						Vote:  *vote,
					},
				}, leader)
			})
		} else {
			bb.in <- block
		}

		for j := 1; j <= 2; j++ {
			node := nodes[j]
			vote, err := newTestVote(block, node)
			require.NoError(t, err)
			msg := Message{
				VoteMessage: vote,
			}

			callbacks = append(callbacks, func() {
				t.Log("Injecting vote for round",
					msg.VoteMessage.Vote.Round, msg.VoteMessage.Vote.Digest, msg.VoteMessage.Signature.Signer)
				err := e.HandleMessage(&msg, node)
				require.NoError(t, err)
			})
		}

		for j := 1; j <= 2; j++ {
			node := nodes[j]
			finalization := newTestFinalization(t, block, node)
			msg := Message{
				Finalization: finalization,
			}
			callbacks = append(callbacks, func() {
				t.Log("Injecting finalization for round", msg.Finalization.Finalization.Round, msg.Finalization.Finalization.Digest)
				err := e.HandleMessage(&msg, node)
				require.NoError(t, err)
			})
		}
	}
	return callbacks
}

func TestEpochBlockSentTwice(t *testing.T) {
	l := testutil.MakeLogger(t, 1)

	var tooFarMsg, alreadyReceivedMsg bool

	l.Intercept(func(entry zapcore.Entry) error {
		if entry.Message == "Got block of a future round" {
			tooFarMsg = true
		}

		if entry.Message == "Already received a proposal from this node for the round" {
			alreadyReceivedMsg = true
		}

		return nil
	})

	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	wal := newTestWAL(t)

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[1],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	md := e.Metadata()
	md.Round = 2

	block, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)

	vote, err := newTestVote(block, nodes[2])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[2])
	require.NoError(t, err)

	wal.assertWALSize(0)
	require.True(t, tooFarMsg)

	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[2])
	require.NoError(t, err)

	wal.assertWALSize(0)
	require.True(t, alreadyReceivedMsg)

}

func TestEpochBlockSentFromNonLeader(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	nonLeaderMessage := false

	l.Intercept(func(entry zapcore.Entry) error {
		if entry.Message == "Got block from a block proposer that is not the leader of the round" {
			nonLeaderMessage = true
		}
		return nil
	})

	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()
	wal := newTestWAL(t)
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[1],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	md := e.Metadata()
	block, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)

	notLeader := nodes[3]
	vote, err := newTestVote(block, notLeader)
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, notLeader)
	require.NoError(t, err)
	require.True(t, nonLeaderMessage)
	records, err := wal.WriteAheadLog.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 0)
}

func TestEpochBlockTooHighRound(t *testing.T) {
	l := testutil.MakeLogger(t, 1)

	var rejectedBlock bool

	l.Intercept(func(entry zapcore.Entry) error {
		if entry.Message == "Received a block message for a too high round" {
			rejectedBlock = true
		}
		return nil
	})

	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	wal := newTestWAL(t)

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[1],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	t.Run("block from higher round is rejected", func(t *testing.T) {
		defer func() {
			rejectedBlock = false
		}()

		md := e.Metadata()
		md.Round = math.MaxUint64 - 3

		block, ok := bb.BuildBlock(context.Background(), md)
		require.True(t, ok)

		vote, err := newTestVote(block, nodes[0])
		require.NoError(t, err)
		err = e.HandleMessage(&Message{
			BlockMessage: &BlockMessage{
				Vote:  *vote,
				Block: block,
			},
		}, nodes[0])
		require.NoError(t, err)
		require.True(t, rejectedBlock)

		wal.assertWALSize(0)
	})

	t.Run("block is accepted", func(t *testing.T) {
		defer func() {
			rejectedBlock = false
		}()

		md := e.Metadata()
		block, ok := bb.BuildBlock(context.Background(), md)
		require.True(t, ok)

		vote, err := newTestVote(block, nodes[0])
		require.NoError(t, err)
		err = e.HandleMessage(&Message{
			BlockMessage: &BlockMessage{
				Vote:  *vote,
				Block: block,
			},
		}, nodes[0])
		require.NoError(t, err)
		require.False(t, rejectedBlock)

		wal.assertWALSize(1)
	})
}

func newTestVote(block Block, id NodeID) (*Vote, error) {
	vote := ToBeSignedVote{
		BlockHeader: block.BlockHeader(),
	}
	sig, err := vote.Sign(&testSigner{})
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

func injectTestVote(t *testing.T, e *Epoch, block Block, id NodeID) {
	vote, err := newTestVote(block, id)
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		VoteMessage: vote,
	}, id)
	require.NoError(t, err)
}

func newTestFinalization(t *testing.T, block Block, id NodeID) *Finalization {
	f := ToBeSignedFinalization{BlockHeader: block.BlockHeader()}
	sig, err := f.Sign(&testSigner{})
	require.NoError(t, err)
	return &Finalization{
		Signature: Signature{
			Signer: id,
			Value:  sig,
		},
		Finalization: ToBeSignedFinalization{
			BlockHeader: block.BlockHeader(),
		},
	}
}

func injectTestFinalization(t *testing.T, e *Epoch, block Block, id NodeID) {
	err := e.HandleMessage(&Message{
		Finalization: newTestFinalization(t, block, id),
	}, id)
	require.NoError(t, err)
}

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
	err error
}

func (t *testSignatureAggregator) Aggregate(signatures []Signature) (QuorumCertificate, error) {
	return testQC(signatures), t.err
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

type testBlockBuilder struct {
	out                chan *testBlock
	in                 chan *testBlock
	blockShouldBeBuilt chan struct{}
}

// BuildBlock builds a new testblock and sends it to the BlockBuilder channel
func (t *testBlockBuilder) BuildBlock(_ context.Context, metadata ProtocolMetadata) (Block, bool) {
	if len(t.in) > 0 {
		block := <-t.in
		return block, true
	}

	tb := newTestBlock(metadata)

	select {
	case t.out <- tb:
	default:
	}

	return tb, true
}

func (t *testBlockBuilder) IncomingBlock(ctx context.Context) {
	select {
	case <-t.blockShouldBeBuilt:
	case <-ctx.Done():
	}
}

type testBlock struct {
	data     []byte
	metadata ProtocolMetadata
	digest   [32]byte
	verificationDelay chan struct{}
}

func (tb *testBlock) Verify() error {
	if tb.verificationDelay == nil {
		return nil
	}
	
	<- tb.verificationDelay

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

func (mem *InMemStorage) waitForBlockCommit(seq uint64) Block {
	mem.lock.Lock()
	defer mem.lock.Unlock()

	for {
		if data, exists := mem.data[seq]; exists {
			return data.Block
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

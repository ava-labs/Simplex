// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	. "github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/record"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

// TestEpochLeaderFailoverWithEmptyNotarization ensures leader failover works with
// future empty notarizations.
// The order of the test are as follows
// index block @ round 0 into storage.
// create but don't send blocks for rounds 1,3
// send empty notarization for round 2 to epoch
// notarize and finalize block for round 1
// we expect the future empty notarization for round 2 to increment the round
func TestEpochLeaderFailoverWithEmptyNotarization(t *testing.T) {
	l := testutil.MakeLogger(t, 1)

	bb := &testBlockBuilder{
		out:                make(chan *testBlock, 2),
		blockShouldBeBuilt: make(chan struct{}, 1),
		in:                 make(chan *testBlock, 2),
	}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}

	wal := newTestWAL(t)

	start := time.Now()
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		StartTime:           start,
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

	// Agree on the first block, and then receive an empty notarization for round 3.
	// Afterward, run through rounds 1 and 2.
	// The node should move to round 4 via the empty notarization it has received
	// from earlier.

	notarizeAndFinalizeRound(t, e, bb)

	block0, _, ok := storage.Retrieve(0)
	require.True(t, ok)

	block1, ok := bb.BuildBlock(context.Background(), ProtocolMetadata{
		Round: 1,
		Prev:  block0.BlockHeader().Digest,
		Seq:   1,
	})
	require.True(t, ok)

	block2, ok := bb.BuildBlock(context.Background(), ProtocolMetadata{
		Round: 3,
		Prev:  block1.BlockHeader().Digest,
		Seq:   2,
	})
	require.True(t, ok)

	// Artificially force the block builder to output the blocks we want.
	for len(bb.out) > 0 {
		<-bb.out
	}
	for _, block := range []VerifiedBlock{block1, block2} {
		bb.out <- block.(*testBlock)
		bb.in <- block.(*testBlock)
	}

	emptyNotarization := newEmptyNotarization(nodes[:3], 2, 1)

	e.HandleMessage(&Message{
		EmptyNotarization: emptyNotarization,
	}, nodes[1])

	notarizeAndFinalizeRound(t, e, bb)

	wal.assertNotarization(2)
	nextBlockSeqToCommit := uint64(2)
	nextRoundToCommit := uint64(3)

	runCrashAndRestartExecution(t, e, bb, wal, storage, func(t *testing.T, e *Epoch, bb *testBlockBuilder, storage *InMemStorage, wal *testWAL) {
		// Ensure our node proposes block with sequence 3 for round 4
		block, _ := notarizeAndFinalizeRound(t, e, bb)
		require.Equal(t, nextBlockSeqToCommit, block.BlockHeader().Seq)
		require.Equal(t, nextRoundToCommit, block.BlockHeader().Round)
		require.Equal(t, uint64(3), storage.Height())
	})
}

// newEmptyNotarization creates a new empty notarization
func newEmptyNotarization(nodes []NodeID, round uint64, seq uint64) *EmptyNotarization {
	var qc testQC

	for i, node := range nodes {
		qc = append(qc, Signature{Signer: node, Value: []byte{byte(i)}})
	}

	return &EmptyNotarization{
		QC: qc,
		Vote: ToBeSignedEmptyVote{ProtocolMetadata: ProtocolMetadata{
			Round: round,
		}},
	}
}

func TestEpochLeaderFailoverReceivesEmptyVotesEarly(t *testing.T) {
	l := testutil.MakeLogger(t, 1)

	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	wal := newTestWAL(t)

	start := time.Now()
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		StartTime:           start,
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

	// Run through 3 blocks, to make the block proposals be:
	// 1 --> 2 --> 3 --> X (node 4 doesn't propose a block)

	// Then, don't do anything and wait for our node
	// to start complaining about a block not being notarized

	for round := uint64(0); round < 3; round++ {
		notarizeAndFinalizeRound(t, e, bb)
	}

	lastBlock, _, ok := storage.Retrieve(storage.Height() - 1)
	require.True(t, ok)

	prev := lastBlock.BlockHeader().Digest

	emptyBlockMd := ProtocolMetadata{
		Round: 3,
		Seq:   2,
		Prev:  prev,
	}

	emptyVoteFrom1 := createEmptyVote(emptyBlockMd, nodes[1])
	emptyVoteFrom2 := createEmptyVote(emptyBlockMd, nodes[2])

	e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom1,
	}, nodes[1])
	e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom2,
	}, nodes[2])

	bb.blockShouldBeBuilt <- struct{}{}

	waitForBlockProposerTimeout(t, e, &start, e.Metadata().Round)

	runCrashAndRestartExecution(t, e, bb, wal, storage, func(t *testing.T, e *Epoch, bb *testBlockBuilder, storage *InMemStorage, wal *testWAL) {
		wal.lock.Lock()
		walContent, err := wal.ReadAll()
		require.NoError(t, err)
		wal.lock.Unlock()

		rawEmptyVote, rawEmptyNotarization, rawProposal := walContent[len(walContent)-3], walContent[len(walContent)-2], walContent[len(walContent)-1]
		emptyVote, err := ParseEmptyVoteRecord(rawEmptyVote)
		require.NoError(t, err)
		require.Equal(t, createEmptyVote(emptyBlockMd, nodes[0]).Vote, emptyVote)

		emptyNotarization, err := EmptyNotarizationFromRecord(rawEmptyNotarization, &testQCDeserializer{t: t})
		require.NoError(t, err)
		require.Equal(t, emptyVoteFrom1.Vote, emptyNotarization.Vote)
		require.Equal(t, uint64(3), emptyNotarization.Vote.Round)
		require.Equal(t, uint64(0), emptyNotarization.Vote.Seq)
		require.Equal(t, uint64(3), storage.Height())

		header, _, err := ParseBlockRecord(rawProposal)
		require.NoError(t, err)
		require.Equal(t, uint64(4), header.Round)
		require.Equal(t, uint64(3), header.Seq)

		// Ensure our node proposes block with sequence 3 for round 4
		block := <-bb.out

		for i := 1; i <= quorum; i++ {
			injectTestFinalizeVote(t, e, block, nodes[i])
		}

		block2 := storage.waitForBlockCommit(3)
		require.Equal(t, block, block2)
		require.Equal(t, uint64(4), storage.Height())
		require.Equal(t, uint64(4), block2.BlockHeader().Round)
		require.Equal(t, uint64(3), block2.BlockHeader().Seq)
	})

}

func TestReceiveEmptyNotarizationWithNoQC(t *testing.T) {
	l := testutil.MakeLogger(t, 1)

	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}

	wal := newTestWAL(t)

	start := time.Now()
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		StartTime:           start,
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

	emptyNotarization := newEmptyNotarization(nodes[:3], 0, 0)

	e.HandleMessage(&Message{
		EmptyNotarization: &EmptyNotarization{Vote: emptyNotarization.Vote},
	}, nodes[0])
}

func TestEpochLeaderFailover(t *testing.T) {
	l := testutil.MakeLogger(t, 1)

	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}

	wal := newTestWAL(t)

	start := time.Now()
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		StartTime:           start,
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

	// Run through 3 blocks, to make the block proposals be:
	// 1 --> 2 --> 3 --> X (node 4 doesn't propose a block)

	// Then, don't do anything and wait for our node
	// to start complaining about a block not being notarized

	for round := uint64(0); round < 3; round++ {
		notarizeAndFinalizeRound(t, e, bb)
	}

	bb.blockShouldBeBuilt <- struct{}{}

	waitForBlockProposerTimeout(t, e, &start, e.Metadata().Round)

	runCrashAndRestartExecution(t, e, bb, wal, storage, func(t *testing.T, e *Epoch, bb *testBlockBuilder, storage *InMemStorage, wal *testWAL) {
		lastBlock, _, ok := storage.Retrieve(storage.Height() - 1)
		require.True(t, ok)

		prev := lastBlock.BlockHeader().Digest

		emptyBlockMd := ProtocolMetadata{
			Round: 3,
			Seq:   2,
			Prev:  prev,
		}

		emptyVoteFrom1 := createEmptyVote(emptyBlockMd, nodes[1])
		emptyVoteFrom2 := createEmptyVote(emptyBlockMd, nodes[2])

		e.HandleMessage(&Message{
			EmptyVoteMessage: emptyVoteFrom1,
		}, nodes[1])
		e.HandleMessage(&Message{
			EmptyVoteMessage: emptyVoteFrom2,
		}, nodes[2])

		wal.lock.Lock()
		walContent, err := wal.ReadAll()
		require.NoError(t, err)
		wal.lock.Unlock()

		rawEmptyVote, rawEmptyNotarization := walContent[len(walContent)-2], walContent[len(walContent)-1]
		emptyVote, err := ParseEmptyVoteRecord(rawEmptyVote)
		require.NoError(t, err)
		require.Equal(t, createEmptyVote(emptyBlockMd, nodes[0]).Vote, emptyVote)

		emptyNotarization, err := EmptyNotarizationFromRecord(rawEmptyNotarization, &testQCDeserializer{t: t})
		require.NoError(t, err)
		require.Equal(t, emptyVoteFrom1.Vote, emptyNotarization.Vote)
		require.Equal(t, uint64(3), emptyNotarization.Vote.Round)
		require.Equal(t, uint64(0), emptyNotarization.Vote.Seq)
		require.Equal(t, uint64(3), storage.Height())

		nextBlockSeqToCommit := uint64(3)
		nextRoundToCommit := uint64(4)

		// Ensure our node proposes block with sequence 3 for round 4
		block, _ := notarizeAndFinalizeRound(t, e, bb)
		require.Equal(t, nextRoundToCommit, block.BlockHeader().Round)
		require.Equal(t, nextBlockSeqToCommit, block.BlockHeader().Seq)

		require.Equal(t, uint64(4), storage.Height())
	})
}

func TestEpochNoFinalizationAfterEmptyVote(t *testing.T) {
	l := testutil.MakeLogger(t, 1)

	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	wal := newTestWAL(t)

	recordedMessages := make(chan *Message, 7)
	comm := &recordingComm{Communication: noopComm(nodes), BroadcastMessages: recordedMessages}

	start := time.Now()
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		StartTime:           start,
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                comm,
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	notarizeAndFinalizeRound(t, e, bb)

	// Drain the messages recorded
	for len(recordedMessages) > 0 {
		<-recordedMessages
	}

	bb.blockShouldBeBuilt <- struct{}{}
	waitForBlockProposerTimeout(t, e, &start, e.Metadata().Round)
	b, _, ok := storage.Retrieve(0)
	require.True(t, ok)

	leader := LeaderForRound(nodes, 1)
	_, ok = bb.BuildBlock(context.Background(), ProtocolMetadata{
		Prev:  b.BlockHeader().Digest,
		Round: 1,
		Seq:   1,
	})
	require.True(t, ok)

	block := <-bb.out

	vote, err := newTestVote(block, leader)
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, leader)
	require.NoError(t, err)

	for i := 1; i <= quorum; i++ {
		injectTestVote(t, e, block, nodes[i])
	}

	wal.assertNotarization(1)

	for i := 1; i < quorum; i++ {
		injectTestFinalizeVote(t, e, block, nodes[i])
	}

	// A block should not have been committed because we do not include our own finalization.
	storage.ensureNoBlockCommit(t, 1)

	// There should only two messages sent, which are an empty vote and a notarization.
	// This proves that a finalization or a regular vote were never sent by us.
	msg := <-recordedMessages
	require.NotNil(t, msg.EmptyVoteMessage)

	msg = <-recordedMessages
	require.NotNil(t, msg.Notarization)

	require.Empty(t, recordedMessages)
}

func TestEpochLeaderFailoverAfterProposal(t *testing.T) {
	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}

	wal := newTestWAL(t)

	logger := testutil.MakeLogger(t, 1)

	start := time.Now()
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		StartTime:           start,
		Logger:              logger,
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

	// Run through 3 blocks, to make the block proposals be:
	// 1 --> 2 --> 3 --> 4
	// Node 4 proposes a block, but node 1 cannot collect votes until the timeout.
	// After the timeout expires, node 1 is sent all the votes, and it should notarize the block.

	for range 3 {
		notarizeAndFinalizeRound(t, e, bb)
	}

	wal.assertWALSize(6) // (block, notarization) x 3 rounds

	// leader is the proposer of the new block for the given round
	leader := LeaderForRound(nodes, 3)
	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	require.Equal(t, md.Round, md.Seq)

	block := <-bb.out

	vote, err := newTestVote(block, leader)
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, leader)
	require.NoError(t, err)

	// Wait until we have verified the block and written it to the WAL
	wal.assertWALSize(7)

	// Send a timeout from the application
	bb.blockShouldBeBuilt <- struct{}{}
	waitForBlockProposerTimeout(t, e, &start, e.Metadata().Round)

	runCrashAndRestartExecution(t, e, bb, wal, storage, func(t *testing.T, e *Epoch, bb *testBlockBuilder, storage *InMemStorage, wal *testWAL) {

		lastBlock, _, ok := storage.Retrieve(storage.Height() - 1)
		require.True(t, ok)

		prev := lastBlock.BlockHeader().Digest

		md = ProtocolMetadata{
			Round: 3,
			Seq:   2,
			Prev:  prev,
		}

		nextBlockSeqToCommit := uint64(3)
		nextRoundToCommit := uint64(4)

		emptyVoteFrom1 := createEmptyVote(md, nodes[1])
		emptyVoteFrom2 := createEmptyVote(md, nodes[2])

		e.HandleMessage(&Message{
			EmptyVoteMessage: emptyVoteFrom1,
		}, nodes[1])
		e.HandleMessage(&Message{
			EmptyVoteMessage: emptyVoteFrom2,
		}, nodes[2])

		// Ensure our node proposes block with sequence 3 for round 4
		block, _ := notarizeAndFinalizeRound(t, e, bb)
		require.Equal(t, nextRoundToCommit, block.BlockHeader().Round)
		require.Equal(t, nextBlockSeqToCommit, block.BlockHeader().Seq)

		// WAL must contain an empty vote and an empty block.
		walContent, err := wal.ReadAll()
		require.NoError(t, err)

		// WAL should be: [..., <empty vote>, <empty block>, <notarization for 4>, <block3>]
		rawEmptyVote, rawEmptyNotarization := walContent[len(walContent)-4], walContent[len(walContent)-3]

		emptyVote, err := ParseEmptyVoteRecord(rawEmptyVote)
		require.NoError(t, err)
		require.Equal(t, createEmptyVote(md, nodes[0]).Vote, emptyVote)

		emptyNotarization, err := EmptyNotarizationFromRecord(rawEmptyNotarization, &testQCDeserializer{t: t})
		require.NoError(t, err)
		require.Equal(t, emptyVoteFrom1.Vote, emptyNotarization.Vote)
		require.Equal(t, uint64(3), emptyNotarization.Vote.Round)
		require.Equal(t, uint64(0), emptyNotarization.Vote.Seq)
		require.Equal(t, uint64(4), storage.Height())
	})
}

func TestEpochLeaderFailoverTwice(t *testing.T) {
	l := testutil.MakeLogger(t, 1)

	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}

	wal := newTestWAL(t)

	start := time.Now()
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		StartTime:           start,
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

	for range 2 {
		notarizeAndFinalizeRound(t, e, bb)
	}

	t.Log("Node 2 crashes, leader failover to node 3")

	bb.blockShouldBeBuilt <- struct{}{}

	waitForBlockProposerTimeout(t, e, &start, e.Metadata().Round)

	runCrashAndRestartExecution(t, e, bb, wal, storage, func(t *testing.T, e *Epoch, bb *testBlockBuilder, storage *InMemStorage, wal *testWAL) {
		lastBlock, _, ok := storage.Retrieve(storage.Height() - 1)
		require.True(t, ok)

		prev := lastBlock.BlockHeader().Digest

		md := ProtocolMetadata{
			Round: 2,
			Seq:   1,
			Prev:  prev,
		}

		emptyVoteFrom2 := createEmptyVote(md, nodes[2])
		emptyVoteFrom3 := createEmptyVote(md, nodes[3])

		e.HandleMessage(&Message{
			EmptyVoteMessage: emptyVoteFrom2,
		}, nodes[2])
		e.HandleMessage(&Message{
			EmptyVoteMessage: emptyVoteFrom3,
		}, nodes[3])

		wal.assertNotarization(2)

		t.Log("Node 3 crashes and node 2 comes back up (just in time)")

		bb.blockShouldBeBuilt <- struct{}{}

		waitForBlockProposerTimeout(t, e, &start, e.Metadata().Round)

		runCrashAndRestartExecution(t, e, bb, wal, storage, func(t *testing.T, e *Epoch, bb *testBlockBuilder, storage *InMemStorage, wal *testWAL) {
			md := ProtocolMetadata{
				Round: 3,
				Seq:   1,
				Prev:  prev,
			}

			emptyVoteFrom1 := createEmptyVote(md, nodes[1])
			emptyVoteFrom3 = createEmptyVote(md, nodes[3])

			e.HandleMessage(&Message{
				EmptyVoteMessage: emptyVoteFrom1,
			}, nodes[1])
			e.HandleMessage(&Message{
				EmptyVoteMessage: emptyVoteFrom3,
			}, nodes[3])

			wal.assertNotarization(3)

			// Ensure our node proposes block with sequence 2 for round 4
			nextRoundToCommit := uint64(4)
			nextBlockSeqToCommit := uint64(2)
			block, _ := notarizeAndFinalizeRound(t, e, bb)
			require.Equal(t, nextRoundToCommit, block.BlockHeader().Round)
			require.Equal(t, nextBlockSeqToCommit, block.BlockHeader().Seq)

			// WAL must contain an empty vote and an empty block.
			walContent, err := wal.ReadAll()
			require.NoError(t, err)

			// WAL should be: [..., <empty vote>, <empty block>, <notarization for 4>, <block2>]
			rawEmptyVote, rawEmptyNotarization := walContent[len(walContent)-4], walContent[len(walContent)-3]

			emptyVote, err := ParseEmptyVoteRecord(rawEmptyVote)
			require.NoError(t, err)
			require.Equal(t, createEmptyVote(md, nodes[0]).Vote, emptyVote)

			emptyNotarization, err := EmptyNotarizationFromRecord(rawEmptyNotarization, &testQCDeserializer{t: t})
			require.NoError(t, err)
			require.Equal(t, emptyVoteFrom1.Vote, emptyNotarization.Vote)
			require.Equal(t, uint64(3), emptyNotarization.Vote.Round)
			require.Equal(t, uint64(0), emptyNotarization.Vote.Seq)
			require.Equal(t, uint64(3), storage.Height())
		})
	})
}

func TestEpochLeaderFailoverBecauseOfBadBlock(t *testing.T) {
	// This test ensures that if a block is proposed by a node, but it is invalid,
	// the node will immediately proceed to notarize the empty block.
	l := testutil.MakeLogger(t, 1)

	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}

	wal := newTestWAL(t)

	recordedMessages := make(chan *Message, 100)
	comm := &recordingComm{Communication: noopComm(nodes), BroadcastMessages: recordedMessages}

	start := time.Now()
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		StartTime:           start,
		Logger:              l,
		ID:                  nodes[3],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                comm,
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	notarizeAndFinalizeRound(t, e, bb)

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	block := <-bb.out
	block.verificationError = errors.New("invalid block")

	vote, err := newTestVote(block, nodes[1])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[1])
	require.NoError(t, err)

	waitForBlockProposerTimeout(t, e, &start, 1)
	emptyVoteFrom1 := createEmptyVote(md, nodes[0])
	emptyVoteFrom2 := createEmptyVote(md, nodes[2])

	e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom1,
	}, nodes[0])
	e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom2,
	}, nodes[2])

	require.Equal(t, record.EmptyNotarizationRecordType, wal.assertNotarization(1))
	notarizeAndFinalizeRound(t, e, bb)
}

func createEmptyVote(md ProtocolMetadata, signer NodeID) *EmptyVote {
	md.Prev = Digest{}
	md.Seq = 0
	emptyVoteFrom2 := &EmptyVote{
		Signature: Signature{
			Signer: signer,
		},
		Vote: ToBeSignedEmptyVote{
			ProtocolMetadata: md,
		},
	}
	return emptyVoteFrom2
}

func waitForBlockProposerTimeout(t *testing.T, e *Epoch, startTime *time.Time, startRound uint64) {
	timeout := time.NewTimer(time.Minute)
	defer timeout.Stop()

	for {
		if e.WAL.(*testWAL).containsEmptyVote(startRound) || e.WAL.(*testWAL).containsEmptyNotarization(startRound) {
			return
		}

		// if we are expected to time out for this round, we should not have a notarization
		require.False(t, e.WAL.(*testWAL).containsNotarization(startRound))

		*startTime = startTime.Add(e.EpochConfig.MaxProposalWait / 5)
		e.AdvanceTime(*startTime)
		select {
		case <-time.After(time.Millisecond * 10):
			continue
		case <-timeout.C:
			require.Fail(t, "timed out waiting for event")
		}
	}
}

func TestEpochLeaderFailoverNotNeeded(t *testing.T) {
	var timedOut atomic.Bool

	l := testutil.MakeLogger(t, 1)
	l.Intercept(func(entry zapcore.Entry) error {
		if entry.Message == `Timed out on block agreement` {
			timedOut.Store(true)
		}
		return nil
	})

	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	wal := newTestWAL(t)

	start := time.Now()

	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		StartTime:           start,
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

	// Run through 3 blocks, to make the block proposals be:
	// 1 --> 2 --> 3 --> 4 (node 4 proposes a block eventually but not immediately

	rounds := uint64(3)

	for round := uint64(0); round < rounds; round++ {
		notarizeAndFinalizeRound(t, e, bb)
	}
	bb.blockShouldBeBuilt <- struct{}{}
	e.AdvanceTime(start.Add(conf.MaxProposalWait / 2))

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)

	block := <-bb.out

	vote, err := newTestVote(block, nodes[3])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[3])
	require.NoError(t, err)

	// start at one since our node has already voted
	for i := 1; i < quorum; i++ {
		injectTestVote(t, e, block, nodes[i])
	}

	wal.assertNotarization(3)

	e.AdvanceTime(start.Add(conf.MaxProposalWait / 2))
	e.AdvanceTime(start.Add(conf.MaxProposalWait / 2))

	require.False(t, timedOut.Load())
}

type rebroadcastComm struct {
	nodes      []NodeID
	emptyVotes chan *EmptyVote
}

func newRebroadcastComm(nodes []NodeID) *rebroadcastComm {
	return &rebroadcastComm{
		nodes:      nodes,
		emptyVotes: make(chan *EmptyVote, 10),
	}
}

func (r *rebroadcastComm) Nodes() []NodeID {
	return r.nodes
}

func (r *rebroadcastComm) Send(*Message, NodeID) {

}

func (r *rebroadcastComm) Broadcast(msg *Message) {
	if msg.EmptyVoteMessage != nil {
		r.emptyVotes <- msg.EmptyVoteMessage
	}
}

func TestEpochRebroadcastsEmptyVote(t *testing.T) {
	l := testutil.MakeLogger(t, 2)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}

	wal := newTestWAL(t)

	epochTime := time.Now()
	comm := newRebroadcastComm(nodes)
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		MaxRebroadcastWait:  DefaultEmptyVoteRebroadcastTimeout,
		StartTime:           epochTime,
		Logger:              l,
		ID:                  nodes[3], // so we are not the leader
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                comm,
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())
	require.Equal(t, uint64(0), e.Metadata().Round)
	require.Equal(t, uint64(0), e.Metadata().Round)
	require.False(t, wal.containsEmptyVote(0))

	bb.blockShouldBeBuilt <- struct{}{}

	// wait for the initial empty vote broadcast
	// Wait for the initial empty vote broadcast for round 0
	waitForEmptyVote(t, comm, e, 0, epochTime)
	require.Len(t, comm.emptyVotes, 0)

	// Continue to rebroadcast for round 0
	for i := 0; i < 10; i++ {
		waitForEmptyVote(t, comm, e, 0, epochTime)
		wal.assertWALSize(1)
	}

	emptyNotarization := newEmptyNotarization(nodes, 0, 0)
	e.HandleMessage(&simplex.Message{
		EmptyNotarization: emptyNotarization,
	}, nodes[2])

	wal.assertNotarization(0)

	// Ensure rebroadcast was canceled
	epochTime = epochTime.Add(e.MaxRebroadcastWait * 2)
	e.AdvanceTime(epochTime)
	require.Len(t, comm.emptyVotes, 0)

	// Wait for empty vote broadcast for the next round (1)
	bb.blockShouldBeBuilt <- struct{}{}
	waitForEmptyVote(t, comm, e, 1, epochTime)
	wal.assertWALSize(3)

	// Wait for rebroadcast of round 1
	waitForEmptyVote(t, comm, e, 1, epochTime)
}

func waitForEmptyVote(t *testing.T, comm *rebroadcastComm, e *Epoch, expectedRound uint64, epochTime time.Time) {
	timeout := time.NewTimer(1 * time.Minute)
	defer timeout.Stop()

	for {
		select {
		case emptyVote := <-comm.emptyVotes:
			require.Equal(t, expectedRound, emptyVote.Vote.Round)
			return
		case <-timeout.C:
			t.Fatalf("Timed out waiting for empty vote for round %d", expectedRound)
		case <-time.After(10 * time.Millisecond):
			epochTime = epochTime.Add(e.MaxRebroadcastWait)
			e.AdvanceTime(epochTime)
		}
	}
}

func runCrashAndRestartExecution(t *testing.T, e *Epoch, bb *testBlockBuilder, wal *testWAL, storage *InMemStorage, f epochExecution) {
	// Split the test into two scenarios:
	// 1) The node proceeds as usual.
	// 2) The node crashes and restarts.
	cloneWAL := wal.Clone()
	cloneStorage := storage.Clone()

	nodes := e.Comm.Nodes()

	// Clone the block builder
	bbAfterCrash := &testBlockBuilder{
		out:                cloneBlockChan(bb.out),
		in:                 cloneBlockChan(bb.in),
		blockShouldBeBuilt: make(chan struct{}, cap(bb.blockShouldBeBuilt)),
	}

	// Case 1:
	t.Run(fmt.Sprintf("%s-no-crash", t.Name()), func(t *testing.T) {
		f(t, e, bb, storage, wal)
	})

	// Case 2:
	t.Run(fmt.Sprintf("%s-with-crash", t.Name()), func(t *testing.T) {
		conf := EpochConfig{
			QCDeserializer:      &testQCDeserializer{t: t},
			BlockDeserializer:   &blockDeserializer{},
			MaxProposalWait:     DefaultMaxProposalWaitTime,
			StartTime:           time.Now(),
			Logger:              testutil.MakeLogger(t, 1),
			ID:                  nodes[0],
			Signer:              &testSigner{},
			WAL:                 cloneWAL,
			Verifier:            &testVerifier{},
			Storage:             cloneStorage,
			Comm:                noopComm(nodes),
			BlockBuilder:        bbAfterCrash,
			SignatureAggregator: &testSignatureAggregator{},
		}

		e, err := NewEpoch(conf)
		require.NoError(t, err)

		require.NoError(t, e.Start())
		f(t, e, bbAfterCrash, cloneStorage, cloneWAL)
	})
}

func cloneBlockChan(in chan *testBlock) chan *testBlock {
	tmp := make(chan *testBlock, cap(in))
	out := make(chan *testBlock, cap(in))

	for len(in) > 0 {
		block := <-in
		tmp <- block
		out <- block
	}

	for len(tmp) > 0 {
		in <- <-tmp
	}

	return out
}

type recordingComm struct {
	Communication
	BroadcastMessages chan *Message
}

func (rc *recordingComm) Broadcast(msg *Message) {
	rc.BroadcastMessages <- msg
	rc.Communication.Broadcast(msg)
}

type epochExecution func(t *testing.T, e *Epoch, bb *testBlockBuilder, storage *InMemStorage, wal *testWAL)

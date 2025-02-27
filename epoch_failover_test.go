// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"context"
	"fmt"
	. "simplex"
	"simplex/testutil"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestEpochLeaderFailoverWithEmptyNotarization(t *testing.T) {
	l := testutil.MakeLogger(t, 1)

	bb := &testBlockBuilder{
		out:                make(chan *testBlock, 3),
		blockShouldBeBuilt: make(chan struct{}, 1),
		in:                 make(chan *testBlock, 3),
	}
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

	// Agree on the first block, and then receive an empty notarization for round 3.
	// Afterward, run through rounds 1 and 2.
	// The node should move to round 4 via the empty notarization it has received
	// from earlier.

	notarizeAndFinalizeRound(t, nodes, 0, 0, e, bb, quorum, storage, false)

	block0, _, ok := storage.Retrieve(0)
	require.True(t, ok)

	block1, ok := bb.BuildBlock(context.Background(), ProtocolMetadata{
		Round: 1,
		Prev:  block0.BlockHeader().Digest,
		Seq:   1,
	})
	require.True(t, ok)

	block2, ok := bb.BuildBlock(context.Background(), ProtocolMetadata{
		Round: 2,
		Prev:  block1.BlockHeader().Digest,
		Seq:   2,
	})
	require.True(t, ok)

	block3, ok := bb.BuildBlock(context.Background(), ProtocolMetadata{
		Round: 4,
		Prev:  block2.BlockHeader().Digest,
		Seq:   3,
	})
	require.True(t, ok)

	var qc testQC
	for i := 1; i <= quorum; i++ {
		qc = append(qc, Signature{Signer: NodeID{byte(i)}, Value: []byte{byte(i)}})
	}

	// Artificially force the block builder to output the blocks we want.
	for len(bb.out) > 0 {
		<-bb.out
	}
	for _, block := range []Block{block1, block2, block3} {
		bb.out <- block.(*testBlock)
		bb.in <- block.(*testBlock)
	}

	e.HandleMessage(&Message{
		EmptyNotarization: &EmptyNotarization{
			Vote: ToBeSignedEmptyVote{ProtocolMetadata: ProtocolMetadata{
				Prev:  block2.BlockHeader().Digest,
				Round: 3,
				Seq:   2,
			}},
			QC: qc,
		},
	}, nodes[1])

	for round := uint64(1); round <= 2; round++ {
		notarizeAndFinalizeRound(t, nodes, round, round, e, bb, quorum, storage, false)
	}

	bb.blockShouldBeBuilt <- struct{}{}

	wal.assertNotarization(3)

	nextBlockSeqToCommit := uint64(3)
	nextRoundToCommit := uint64(4)

	runCrashAndRestartExecution(t, e, bb, wal, storage, func(t *testing.T, e *Epoch, bb *testBlockBuilder, storage *InMemStorage, wal *testWAL) {
		// Ensure our node proposes block with sequence 3 for round 4
		notarizeAndFinalizeRound(t, nodes, nextRoundToCommit, nextBlockSeqToCommit, e, bb, quorum, storage, false)
		require.Equal(t, uint64(4), storage.Height())
	})
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
		notarizeAndFinalizeRound(t, nodes, round, round, e, bb, quorum, storage, false)
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

	waitForBlockProposerTimeout(t, e, start)

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
		require.Equal(t, uint64(2), emptyNotarization.Vote.Seq)
		require.Equal(t, uint64(3), storage.Height())

		header, _, err := ParseBlockRecord(rawProposal)
		require.NoError(t, err)
		require.Equal(t, uint64(4), header.Round)
		require.Equal(t, uint64(3), header.Seq)

		// Ensure our node proposes block with sequence 3 for round 4
		block := <-bb.out

		for i := 1; i <= quorum; i++ {
			injectTestFinalization(t, e, block, nodes[i])
		}

		block2 := storage.waitForBlockCommit(3)
		require.Equal(t, block, block2)
		require.Equal(t, uint64(4), storage.Height())
		require.Equal(t, uint64(4), block2.BlockHeader().Round)
		require.Equal(t, uint64(3), block2.BlockHeader().Seq)
	})

}

func TestEpochLeaderFailover(t *testing.T) {
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
		notarizeAndFinalizeRound(t, nodes, round, round, e, bb, quorum, storage, false)
	}

	bb.blockShouldBeBuilt <- struct{}{}

	waitForBlockProposerTimeout(t, e, start)

	runCrashAndRestartExecution(t, e, bb, wal, storage, func(t *testing.T, e *Epoch, bb *testBlockBuilder, storage *InMemStorage, wal *testWAL) {
		lastBlock, _, ok := storage.Retrieve(storage.Height() - 1)
		require.True(t, ok)

		prev := lastBlock.BlockHeader().Digest

		emptyBlockMd := ProtocolMetadata{
			Round: 3,
			Seq:   2,
			Prev:  prev,
		}

		nextBlockSeqToCommit := uint64(3)
		nextRoundToCommit := uint64(4)

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
		require.Equal(t, uint64(2), emptyNotarization.Vote.Seq)
		require.Equal(t, uint64(3), storage.Height())

		// Ensure our node proposes block with sequence 3 for round 4
		notarizeAndFinalizeRound(t, nodes, nextRoundToCommit, nextBlockSeqToCommit, e, bb, quorum, storage, false)
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

	notarizeAndFinalizeRound(t, nodes, 0, 0, e, bb, quorum, storage, false)

	// Drain the messages recorded
	for len(recordedMessages) > 0 {
		<-recordedMessages
	}

	bb.blockShouldBeBuilt <- struct{}{}

	waitForBlockProposerTimeout(t, e, start)

	block, _, ok := storage.Retrieve(0)
	require.True(t, ok)

	leader := LeaderForRound(nodes, 1)
	_, ok = bb.BuildBlock(context.Background(), ProtocolMetadata{
		Prev:  block.BlockHeader().Digest,
		Round: 1,
		Seq:   1,
	})
	require.True(t, ok)

	block = <-bb.out

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
		injectTestFinalization(t, e, block, nodes[i])
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
	quorum := Quorum(len(nodes))

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

	for _, round := range []uint64{0, 1, 2} {
		notarizeAndFinalizeRound(t, nodes, round, round, e, bb, quorum, storage, false)
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
	waitForBlockProposerTimeout(t, e, start)

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
		notarizeAndFinalizeRound(t, nodes, nextRoundToCommit, nextBlockSeqToCommit, e, bb, quorum, storage, false)

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
		require.Equal(t, uint64(2), emptyNotarization.Vote.Seq)
		require.Equal(t, uint64(4), storage.Height())
	})
}

func TestEpochLeaderFailoverTwice(t *testing.T) {
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

	for _, round := range []uint64{0, 1} {
		notarizeAndFinalizeRound(t, nodes, round, round, e, bb, quorum, storage, false)
	}

	t.Log("Node 2 crashes, leader failover to node 3")

	bb.blockShouldBeBuilt <- struct{}{}

	waitForBlockProposerTimeout(t, e, start)

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

		waitForBlockProposerTimeout(t, e, start)

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
			notarizeAndFinalizeRound(t, nodes, nextRoundToCommit, nextBlockSeqToCommit, e, bb, quorum, storage, false)

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
			require.Equal(t, uint64(1), emptyNotarization.Vote.Seq)
			require.Equal(t, uint64(3), storage.Height())
		})
	})
}

func createEmptyVote(md ProtocolMetadata, signer NodeID) *EmptyVote {
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

func waitForBlockProposerTimeout(t *testing.T, e *Epoch, startTime time.Time) {
	startRound := e.Metadata().Round
	timeout := time.NewTimer(time.Minute)
	defer timeout.Stop()

	for {
		if e.WAL.(*testWAL).containsEmptyVote(startRound) {
			return
		}
		startTime = startTime.Add(e.EpochConfig.MaxProposalWait / 5)
		e.AdvanceTime(startTime)
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
		notarizeAndFinalizeRound(t, nodes, round, round, e, bb, quorum, storage, false)
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

func runCrashAndRestartExecution(t *testing.T, e *Epoch, bb *testBlockBuilder, wal *testWAL, storage *InMemStorage, f epochExecution) {
	// Split the test into two scenarios:
	// 1) The node proceeds as usual.
	// 2) The node crashes and restarts.
	cloneWAL := wal.Clone()
	cloneStorage := storage.Clone()
	
	nodes := e.Comm.ListNodes()

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
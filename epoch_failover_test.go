// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"context"
	. "simplex"
	"simplex/testutil"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestEpochLeaderFailover(t *testing.T) {
	timeoutDetected := make(chan struct{})

	l := testutil.MakeLogger(t, 1)
	l.Intercept(func(entry zapcore.Entry) error {
		if entry.Message == `Timed out on block agreement` {
			close(timeoutDetected)
		}
		return nil
	})

	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	start := time.Now()

	wal := newTestWAL(t)

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

	waitForEvent(t, start, e, timeoutDetected)
	
	emptyBlockMd := e.Metadata()
	emptyBlockMd.Seq = emptyBlockMd.Seq - 1

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

	walContent, err := wal.ReadAll()
	require.NoError(t, err)

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
}

func TestEpochLeaderFailoverAfterProposal(t *testing.T) {
	timeoutDetected := make(chan struct{})
	alreadyTimedOut := make(chan struct{})

	l := testutil.MakeLogger(t, 1)
	l.Intercept(func(entry zapcore.Entry) error {
		if entry.Message == `Timed out on block agreement` {
			close(timeoutDetected)
		}
		if entry.Message == `Received a vote but already timed out in that round` {
			select {
			case <-alreadyTimedOut:
				return nil
			default:
				close(alreadyTimedOut)
			}
		}
		return nil
	})

	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	start := time.Now()

	wal := newTestWAL(t)

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
	// 1 --> 2 --> 3 --> 4
	// Node 4 proposes a block, but node 1 cannot collect votes until the timeout.
	// After the timeout expires, node 1 is sent all the votes, but it should ignore them.

	for _, round := range []uint64{0, 1, 2} {
		notarizeAndFinalizeRound(t, nodes, round, round, e, bb, quorum, storage, false)
	}

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

	bb.blockShouldBeBuilt <- struct{}{}

	waitForEvent(t, start, e, timeoutDetected)

	for i := 1; i < quorum; i++ {
		// Skip the vote of the block proposer
		if leader.Equals(nodes[i]) {
			continue
		}
		injectTestVote(t, e, block, nodes[i])
	}

	waitForEvent(t, start, e, alreadyTimedOut)

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

}

func TestEpochLeaderFailoverTwice(t *testing.T) {
	timeoutDetected := make(chan struct{})

	l := testutil.MakeLogger(t, 1)
	l.Intercept(func(entry zapcore.Entry) error {
		if entry.Message == `Timed out on block agreement` {
			close(timeoutDetected)
			timeoutDetected = make(chan struct{})
		}
		return nil
	})

	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	start := time.Now()

	wal := newTestWAL(t)

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

	waitForEvent(t, start, e, timeoutDetected)

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

	waitForEvent(t, start, e, timeoutDetected)

	md = ProtocolMetadata{
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

func waitForEvent(t *testing.T, start time.Time, e *Epoch, events chan struct{}) {
	now := start
	timeout := time.NewTimer(time.Minute)
	defer timeout.Stop()

	for {
		now = now.Add(e.EpochConfig.MaxProposalWait / 5)
		e.AdvanceTime(now)
		select {
		case <-events:
			return
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

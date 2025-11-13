// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
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
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	bb := &testutil.TestBlockBuilder{
		Out:                make(chan *testutil.TestBlock, 2),
		BlockShouldBeBuilt: make(chan struct{}, 1),
		In:                 make(chan *testutil.TestBlock, 2),
	}
	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	// Agree on the first block, and then receive an empty notarization for round 3.
	// Afterward, run through rounds 1 and 2.
	// The node should move to round 4 via the empty notarization it has received
	// from earlier.

	notarizeAndFinalizeRound(t, e, bb)

	block0, _, err := storage.Retrieve(0)
	require.NoError(t, err)
	block1, ok := bb.BuildBlock(context.Background(), ProtocolMetadata{
		Round: 1,
		Prev:  block0.BlockHeader().Digest,
		Seq:   1,
	}, emptyBlacklist)
	require.True(t, ok)

	block2, ok := bb.BuildBlock(context.Background(), ProtocolMetadata{
		Round: 3,
		Prev:  block1.BlockHeader().Digest,
		Seq:   2,
	}, emptyBlacklist)
	require.True(t, ok)

	// Artificially force the block builder to output the blocks we want.
	for len(bb.Out) > 0 {
		<-bb.Out
	}
	for _, block := range []VerifiedBlock{block1, block2} {
		bb.Out <- block.(*testutil.TestBlock)
		bb.In <- block.(*testutil.TestBlock)
	}

	emptyNotarization := testutil.NewEmptyNotarization(nodes[:3], 2)
	fmt.Println("Sending empty notarization for round 2:", emptyNotarization)
	e.HandleMessage(&Message{
		EmptyNotarization: emptyNotarization,
	}, nodes[1])

	notarizeAndFinalizeRound(t, e, bb)

	wal.AssertNotarization(2)
	// nextBlockSeqToCommit := uint64(2)
	// nextRoundToCommit := uint64(3)

	// runCrashAndRestartExecution(t, e, bb, wal, storage, func(t *testing.T, e *Epoch, bb *testutil.TestBlockBuilder, storage *testutil.InMemStorage, wal *testutil.TestWAL) {
	// 	// Ensure our node proposes block with sequence 3 for round 4
	// 	block, _ := notarizeAndFinalizeRound(t, e, bb)
	// 	require.Equal(t, nextBlockSeqToCommit, block.BlockHeader().Seq)
	// 	require.Equal(t, nextRoundToCommit, block.BlockHeader().Round)
	// 	require.Equal(t, uint64(3), storage.NumBlocks())
	// })
}

func TestEpochRebroadcastsEmptyVoteAfterBlockProposalReceived(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}

	comm := newRebroadcastComm(nodes)
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[3], comm, bb)
	epochTime := conf.StartTime
	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())
	require.Equal(t, uint64(0), e.Metadata().Round)
	require.Equal(t, uint64(0), e.Metadata().Round)
	require.False(t, wal.ContainsEmptyVote(0))

	bb.BlockShouldBeBuilt <- struct{}{}

	// receive the block proposal for round 0
	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
	require.True(t, ok)
	block := <-bb.Out

	vote, err := testutil.NewTestVote(block, nodes[0])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[0])
	require.NoError(t, err)

	// ensure we have the block in our wal
	wal.AssertBlockProposal(0)
	wal.AssertWALSize(1)

	// wait for the initial empty vote broadcast
	waitForEmptyVote(t, comm, e, 0, epochTime)
	require.Len(t, comm.emptyVotes, 0)

	// advance another rebroadcast period and ensure more empty votes are sent
	waitForEmptyVote(t, comm, e, 0, epochTime)
	require.Len(t, comm.emptyVotes, 0)
}

func TestEpochLeaderFailoverReceivesEmptyVotesEarly(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

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

	lastBlock, _, err := storage.Retrieve(storage.NumBlocks() - 1)
	require.NoError(t, err)

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

	bb.BlockShouldBeBuilt <- struct{}{}

	testutil.WaitForBlockProposerTimeout(t, e, &e.StartTime, e.Metadata().Round)

	runCrashAndRestartExecution(t, e, bb, wal, storage, func(t *testing.T, e *Epoch, bb *testutil.TestBlockBuilder, storage *testutil.InMemStorage, wal *testutil.TestWAL) {
		walContent, err := wal.ReadAll()
		require.NoError(t, err)

		rawEmptyVote, rawEmptyNotarization, rawProposal := walContent[len(walContent)-3], walContent[len(walContent)-2], walContent[len(walContent)-1]
		emptyVote, err := ParseEmptyVoteRecord(rawEmptyVote)
		require.NoError(t, err)
		require.Equal(t, createEmptyVote(emptyBlockMd, nodes[0]).Vote, emptyVote)

		emptyNotarization, err := EmptyNotarizationFromRecord(rawEmptyNotarization, e.QCDeserializer)
		require.NoError(t, err)
		require.Equal(t, emptyVoteFrom1.Vote, emptyNotarization.Vote)
		require.Equal(t, uint64(3), emptyNotarization.Vote.Round)
		require.Equal(t, uint64(3), storage.NumBlocks())

		header, _, err := ParseBlockRecord(rawProposal)
		require.NoError(t, err)
		require.Equal(t, uint64(4), header.Round)
		require.Equal(t, uint64(3), header.Seq)

		// Ensure our node proposes block with sequence 3 for round 4
		block := <-bb.Out

		for i := 1; i <= quorum; i++ {
			testutil.InjectTestFinalizeVote(t, e, block, nodes[i])
		}

		block2 := storage.WaitForBlockCommit(3)
		require.Equal(t, block, block2)
		require.Equal(t, uint64(4), storage.NumBlocks())
		require.Equal(t, uint64(4), block2.BlockHeader().Round)
		require.Equal(t, uint64(3), block2.BlockHeader().Seq)
	})

}

func TestReceiveEmptyNotarizationWithNoQC(t *testing.T) {
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}

	conf, _, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[1], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)
	require.NoError(t, e.Start())

	emptyNotarization := testutil.NewEmptyNotarization(nodes[:3], 0)

	e.HandleMessage(&Message{
		EmptyNotarization: &EmptyNotarization{Vote: emptyNotarization.Vote},
	}, nodes[0])
}

func TestEpochLeaderFailover(t *testing.T) {
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

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

	bb.BlockShouldBeBuilt <- struct{}{}

	testutil.WaitForBlockProposerTimeout(t, e, &e.StartTime, e.Metadata().Round)

	runCrashAndRestartExecution(t, e, bb, wal, storage, func(t *testing.T, e *Epoch, bb *testutil.TestBlockBuilder, storage *testutil.InMemStorage, wal *testutil.TestWAL) {
		lastBlock, _, err := storage.Retrieve(storage.NumBlocks() - 1)
		require.NoError(t, err)

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

		walContent, err := wal.ReadAll()
		require.NoError(t, err)

		rawEmptyVote, rawEmptyNotarization := walContent[len(walContent)-2], walContent[len(walContent)-1]
		emptyVote, err := ParseEmptyVoteRecord(rawEmptyVote)
		require.NoError(t, err)
		require.Equal(t, createEmptyVote(emptyBlockMd, nodes[0]).Vote, emptyVote)

		emptyNotarization, err := EmptyNotarizationFromRecord(rawEmptyNotarization, e.QCDeserializer)
		require.NoError(t, err)
		require.Equal(t, emptyVoteFrom1.Vote, emptyNotarization.Vote)
		require.Equal(t, uint64(3), emptyNotarization.Vote.Round)
		require.Equal(t, uint64(3), storage.NumBlocks())

		nextBlockSeqToCommit := uint64(3)
		nextRoundToCommit := uint64(4)

		// Ensure our node proposes block with sequence 3 for round 4
		block, _ := notarizeAndFinalizeRound(t, e, bb)
		require.Equal(t, nextRoundToCommit, block.BlockHeader().Round)
		require.Equal(t, nextBlockSeqToCommit, block.BlockHeader().Seq)

		require.Equal(t, uint64(4), storage.NumBlocks())
	})
}

func TestEpochLeaderFailoverDoNotPersistEmptyRoundTwice(t *testing.T) {
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	for round := uint64(0); round < 3; round++ {
		notarizeAndFinalizeRound(t, e, bb)
	}

	bb.BlockShouldBeBuilt <- struct{}{}

	testutil.WaitForBlockProposerTimeout(t, e, &e.StartTime, e.Metadata().Round)

	emptyBlockMd := ProtocolMetadata{
		Round: 3,
		Seq:   2,
	}

	emptyVoteFrom1 := createEmptyVote(emptyBlockMd, nodes[1])
	emptyVoteFrom2 := createEmptyVote(emptyBlockMd, nodes[2])

	err = e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom1,
	}, nodes[1])
	require.NoError(t, err)

	err = e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom2,
	}, nodes[2])
	require.NoError(t, err)

	require.True(t, wal.ContainsEmptyNotarization(3))

	walContent, err := wal.ReadAll()
	require.NoError(t, err)

	prevWALSize := len(walContent)

	en := testutil.NewEmptyNotarization(nodes, 3)

	err = e.HandleMessage(&Message{
		EmptyNotarization: en,
	}, nodes[2])
	require.NoError(t, err)

	walContent, err = wal.ReadAll()
	require.NoError(t, err)

	nextWALSize := len(walContent)

	require.Equal(t, prevWALSize, nextWALSize, "WAL size should not increase after receiving duplicate empty notarization")
}

func TestEpochLeaderRecursivelyFetchNotarizedBlocks(t *testing.T) {
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}

	recordedMessages := make(chan *Message, 100)

	comm := &recordingComm{Communication: testutil.NoopComm(nodes), SentMessages: recordedMessages}
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], comm, bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	nodeID := nodes[0]

	// Send the last block and notarization

	require.NoError(t, e.Start())

	startTime := e.StartTime

	// Feed the node empty notarizations until it advances to the round of the last block.
	for round := uint64(0); round < 6; round++ {
		emptyNotarization := testutil.NewEmptyNotarization(nodes[1:], round)

		if LeaderForRound(nodes, round).Equals(nodeID) {
			testutil.WaitToEnterRound(t, e, round)
			t.Log("Triggering block to be built for round", round)
			bb.BlockShouldBeBuilt <- struct{}{}
			testutil.WaitForBlockProposerTimeout(t, e, &startTime, round)

			err = e.HandleMessage(&Message{
				EmptyNotarization: emptyNotarization,
			}, nodes[2])
			require.NoError(t, err)

			wal.AssertNotarization(round)
			continue
		}

		// Otherwise, just receive the empty notarization
		// and advance to the next round
		err = e.HandleMessage(&Message{
			EmptyNotarization: emptyNotarization,
		}, nodes[2])
		require.NoError(t, err)
		wal.AssertNotarization(round)
		testutil.WaitToEnterRound(t, e, round)
	}
}

func TestEpochLeaderFailoverInLeaderRound(t *testing.T) {
	nodes := []NodeID{{1}, {2}, {3}, {4}}

	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
	recordedMessages := make(chan *Message, 100)
	comm := &recordingComm{Communication: testutil.NewNoopComm(nodes), BroadcastMessages: recordedMessages}
	conf, _, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[3], comm, bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	// Round 0
	notarizeAndFinalizeRound(t, e, bb)
	// Round 1
	notarizeAndFinalizeRound(t, e, bb)
	// Round 2
	notarizeAndFinalizeRound(t, e, bb)

	// In the third round, we wait for our epoch to propose a block.

	bb.BlockShouldBeBuilt <- struct{}{}

	var sentBlockMessage bool
	for !sentBlockMessage {
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for block proposal")
		case msg := <-recordedMessages:
			if msg.VerifiedBlockMessage != nil {
				sentBlockMessage = true
				break
			}
		}
	}

	block := <-bb.Out
	md := block.BlockHeader().ProtocolMetadata

	// Receive empty votes from other nodes
	emptyVoteFrom1 := createEmptyVote(md, nodes[1])
	emptyVoteFrom2 := createEmptyVote(md, nodes[2])

	e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom1,
	}, nodes[1])
	e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom2,
	}, nodes[2])

	startTime := e.StartTime
	testutil.WaitForBlockProposerTimeout(t, e, &startTime, md.Round)
}

func TestEpochNoFinalizationAfterEmptyVote(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	recordedMessages := make(chan *Message, 7)
	comm := &recordingComm{Communication: testutil.NewNoopComm(nodes), BroadcastMessages: recordedMessages}
	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], comm, bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	notarizeAndFinalizeRound(t, e, bb)

	// Drain the messages recorded
	for len(recordedMessages) > 0 {
		<-recordedMessages
	}

	bb.BlockShouldBeBuilt <- struct{}{}
	testutil.WaitForBlockProposerTimeout(t, e, &e.StartTime, e.Metadata().Round)
	b, _, err := storage.Retrieve(0)
	require.NoError(t, err)

	leader := LeaderForRound(nodes, 1)
	_, ok := bb.BuildBlock(context.Background(), ProtocolMetadata{
		Prev:  b.BlockHeader().Digest,
		Round: 1,
		Seq:   1,
	}, emptyBlacklist)
	require.True(t, ok)

	block := <-bb.Out

	vote, err := testutil.NewTestVote(block, leader)
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, leader)
	require.NoError(t, err)

	for i := 1; i <= quorum; i++ {
		testutil.InjectTestVote(t, e, block, nodes[i])
	}

	wal.AssertNotarization(1)

	for i := 1; i < quorum; i++ {
		testutil.InjectTestFinalizeVote(t, e, block, nodes[i])
	}

	// A block should not have been committed because we do not include our own finalization.
	storage.EnsureNoBlockCommit(t, 1)

	// There should only two messages sent, which are an empty vote and a notarization.
	// This proves that a finalization or a regular vote were never sent by us.
	msg := <-recordedMessages
	require.NotNil(t, msg.EmptyVoteMessage)

	msg = <-recordedMessages
	require.NotNil(t, msg.Notarization)

	require.Empty(t, recordedMessages)
}

func TestEpochLeaderFailoverAfterProposal(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}

	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

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

	wal.AssertWALSize(6) // (block, notarization) x 3 rounds

	// leader is the proposer of the new block for the given round
	leader := LeaderForRound(nodes, 3)
	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
	require.True(t, ok)
	require.Equal(t, md.Round, md.Seq)

	block := <-bb.Out

	vote, err := testutil.NewTestVote(block, leader)
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, leader)
	require.NoError(t, err)

	// Wait until we have verified the block and written it to the WAL
	wal.AssertWALSize(7)

	// Send a timeout from the application
	bb.BlockShouldBeBuilt <- struct{}{}
	testutil.WaitForBlockProposerTimeout(t, e, &e.StartTime, e.Metadata().Round)

	runCrashAndRestartExecution(t, e, bb, wal, storage, func(t *testing.T, e *Epoch, bb *testutil.TestBlockBuilder, storage *testutil.InMemStorage, wal *testutil.TestWAL) {

		lastBlock, _, err := storage.Retrieve(storage.NumBlocks() - 1)
		require.NoError(t, err)

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

		emptyNotarization, err := EmptyNotarizationFromRecord(rawEmptyNotarization, e.QCDeserializer)
		require.NoError(t, err)
		require.Equal(t, emptyVoteFrom1.Vote, emptyNotarization.Vote)
		require.Equal(t, uint64(3), emptyNotarization.Vote.Round)
		require.Equal(t, uint64(4), storage.NumBlocks())
	})
}

func TestEpochLeaderFailoverTwice(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}

	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	for range 2 {
		notarizeAndFinalizeRound(t, e, bb)
	}

	t.Log("Node 2 crashes, leader failover to node 3")

	bb.BlockShouldBeBuilt <- struct{}{}

	testutil.WaitForBlockProposerTimeout(t, e, &e.StartTime, e.Metadata().Round)

	runCrashAndRestartExecution(t, e, bb, wal, storage, func(t *testing.T, e *Epoch, bb *testutil.TestBlockBuilder, storage *testutil.InMemStorage, wal *testutil.TestWAL) {
		lastBlock, _, err := storage.Retrieve(storage.NumBlocks() - 1)
		require.NoError(t, err)

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

		wal.AssertNotarization(2)

		t.Log("Node 3 crashes and node 2 comes back up (just in time)")

		bb.BlockShouldBeBuilt <- struct{}{}

		testutil.WaitForBlockProposerTimeout(t, e, &e.StartTime, e.Metadata().Round)

		runCrashAndRestartExecution(t, e, bb, wal, storage, func(t *testing.T, e *Epoch, bb *testutil.TestBlockBuilder, storage *testutil.InMemStorage, wal *testutil.TestWAL) {
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

			wal.AssertNotarization(3)

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

			emptyNotarization, err := EmptyNotarizationFromRecord(rawEmptyNotarization, e.QCDeserializer)
			require.NoError(t, err)
			require.Equal(t, emptyVoteFrom1.Vote, emptyNotarization.Vote)
			require.Equal(t, uint64(3), emptyNotarization.Vote.Round)
			require.Equal(t, uint64(3), storage.NumBlocks())
		})
	})
}

func TestEpochLeaderFailoverGarbageCollectedEmptyVotes(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, _, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	var waitForTimeout sync.WaitGroup
	waitForTimeout.Add(1)

	var triggerEmptyBlockAgreement sync.WaitGroup
	triggerEmptyBlockAgreement.Add(1)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	l := conf.Logger.(*testutil.TestLogger)
	l.Intercept(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "It is time to build a block") {
			emptyNotarization := testutil.NewEmptyNotarization(nodes[1:], 3)
			e.HandleMessage(&Message{
				EmptyNotarization: emptyNotarization,
			}, nodes[1])

			waitForTimeout.Done()
		}

		if strings.Contains(entry.Message, "empty block agreement") {
			triggerEmptyBlockAgreement.Done()
		}

		return nil
	})

	require.NoError(t, e.Start())

	// Run through 3 blocks, to make the block proposals be:
	// 1 --> 2 --> 3 --> X (node 4 doesn't propose a block)

	// Then, don't do anything and wait for our node
	// to start complaining about a block not being notarized

	for round := uint64(0); round < 3; round++ {
		notarizeAndFinalizeRound(t, e, bb)
	}

	bb.BlockShouldBeBuilt <- struct{}{}

	// Wait until we detect it is time to build a block, and we also advance to the next round because of it.
	waitForTimeout.Wait()

	startTime := e.StartTime
	startTime = startTime.Add(e.EpochConfig.MaxProposalWait)
	e.AdvanceTime(startTime)

	triggerEmptyBlockAgreement.Wait()

	// At this point, if we have initialized the timeout process, handling any message fails because we have a halted error.
	// The halted error takes place because we attempted to timeout on a round that has already been garbage collected.
	err = e.HandleMessage(&Message{}, nodes[1])
	require.NoError(t, err)
}

func TestEpochLeaderFailoverBecauseOfBadBlock(t *testing.T) {
	// This test ensures that if a block is proposed by a node, but it is invalid,
	// the node will immediately proceed to notarize the empty block.

	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}

	recordedMessages := make(chan *Message, 100)
	comm := &recordingComm{Communication: testutil.NewNoopComm(nodes), BroadcastMessages: recordedMessages}
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[3], comm, bb)
	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	notarizeAndFinalizeRound(t, e, bb)

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
	require.True(t, ok)
	block := <-bb.Out
	block.VerificationError = errors.New("invalid block")

	vote, err := testutil.NewTestVote(block, nodes[1])
	require.NoError(t, err)

	go func() {
		err = e.HandleMessage(&Message{
			BlockMessage: &BlockMessage{
				Vote:  *vote,
				Block: block,
			},
		}, nodes[1])
		require.NoError(t, err)
	}()

	emptyVoteFrom1 := createEmptyVote(md, nodes[0])
	emptyVoteFrom2 := createEmptyVote(md, nodes[2])

	e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom1,
	}, nodes[0])
	e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom2,
	}, nodes[2])

	testutil.WaitForBlockProposerTimeout(t, e, &e.StartTime, 1)

	require.Equal(t, record.EmptyNotarizationRecordType, wal.AssertNotarization(1))
	notarizeAndFinalizeRound(t, e, bb)
}

func createEmptyVote(md ProtocolMetadata, signer NodeID) *EmptyVote {
	emptyVoteFrom2 := &EmptyVote{
		Signature: Signature{
			Signer: signer,
		},
		Vote: ToBeSignedEmptyVote{
			EmptyVoteMetadata: EmptyVoteMetadata{
				Round: md.Round,
				Epoch: 0,
			},
		},
	}
	return emptyVoteFrom2
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

	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)
	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	// Run through 3 blocks, to make the block proposals be:
	// 1 --> 2 --> 3 --> 4 (node 4 proposes a block eventually but not immediately

	rounds := uint64(3)

	for round := uint64(0); round < rounds; round++ {
		notarizeAndFinalizeRound(t, e, bb)
	}
	bb.BlockShouldBeBuilt <- struct{}{}
	e.AdvanceTime(e.StartTime.Add(conf.MaxProposalWait / 2))

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
	require.True(t, ok)

	block := <-bb.Out

	vote, err := testutil.NewTestVote(block, nodes[3])
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
		testutil.InjectTestVote(t, e, block, nodes[i])
	}

	wal.AssertNotarization(3)

	e.AdvanceTime(e.StartTime.Add(conf.MaxProposalWait / 2))
	e.AdvanceTime(e.StartTime.Add(conf.MaxProposalWait))

	require.False(t, timedOut.Load())
}

func TestEpochBlacklist(t *testing.T) {
	blacklistedLeaderInLogs := make(chan struct{})
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 3)}

	nodes := []NodeID{{1}, {2}, {3}, {4}}

	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)
	l := conf.Logger.(*testutil.TestLogger)
	l.Intercept(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Leader is blacklisted, will not wait for it to propose a block") {
			select {
			case <-blacklistedLeaderInLogs:
			default:
				close(blacklistedLeaderInLogs)
			}
		}
		return nil
	})

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	// Run through 3 blocks, to make the block proposals be:
	// 1 --> 2 --> 3 --> X (node 4 doesn't propose a block)

	// Then, don't do anything and wait for our node
	// to time out on the block and assist it to agree on an empty block.

	for round := uint64(0); round < 3; round++ {
		notarizeAndFinalizeRound(t, e, bb)
	}

	timedOutRound := e.Metadata().Round

	// Forcefully time out the last node.
	err = e.HandleMessage(&Message{
		EmptyNotarization: testutil.NewEmptyNotarization(nodes, timedOutRound),
	}, nodes[1])
	require.NoError(t, err)

	testutil.WaitForBlockProposerTimeout(t, e, &e.StartTime, timedOutRound)

	lastBlock, _, err := storage.Retrieve(storage.NumBlocks() - 1)
	require.NoError(t, err)

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

	wal.AssertNotarization(emptyBlockMd.Round)

	block, _ := notarizeAndFinalizeRound(t, e, bb)
	require.Equal(t, nodes[0], LeaderForRound(nodes, block.BlockHeader().Round)) // our node will propose the empty block

	// Make sure our node properly constructed the blacklist
	require.Equal(t, Blacklist{
		NodeCount:      4,
		SuspectedNodes: SuspectedNodes{{NodeIndex: 3, SuspectingCount: 1, OrbitSuspected: 1}},
		Updates:        []BlacklistUpdate{{Type: BlacklistOpType_NodeSuspected, NodeIndex: 3}},
	}, block.Blacklist())

	// Compute next blacklist
	blacklist := NewBlacklist(4)
	blacklist.Updates = []BlacklistUpdate{{Type: BlacklistOpType_NodeSuspected, NodeIndex: 3}}
	blacklist.SuspectedNodes = []SuspectedNode{{NodeIndex: 3, SuspectingCount: 2, OrbitSuspected: 1}}

	// Build the block and prime it to be proposed in the next round
	block, _ = bb.BuildBlock(context.Background(), e.Metadata(), blacklist)

	// Agree on the block
	block, _ = notarizeAndFinalizeRound(t, e, bb)

	// It should have the blacklist we have previously constructed
	require.Equal(t, blacklist, block.Blacklist())

	// Do it again but preserve the blacklist until we get to the round of the last node.
	// This time, there are no updates to the blacklist, it just carries over.
	blacklist.Updates = nil
	block, _ = bb.BuildBlock(context.Background(), e.Metadata(), blacklist)
	block, _ = notarizeAndFinalizeRound(t, e, bb)

	// The next round is the fourth node's round, and since it is blacklisted,
	// we should not time out but immediately vote on the empty block.
	// Find evidence for this by looking at the logs.
	select {
	case <-blacklistedLeaderInLogs:
	case <-time.After(time.Second * 15):
		require.Fail(t, "timed out waiting for blacklisted leader log")
	}

	wal.AssertEmptyVote(7)

	emptyBlockMd = ProtocolMetadata{
		Round: 7,
	}
	emptyVoteFrom1 = createEmptyVote(emptyBlockMd, nodes[1])
	emptyVoteFrom2 = createEmptyVote(emptyBlockMd, nodes[2])

	e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom1,
	}, nodes[1])
	e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom2,
	}, nodes[2])

	wal.AssertNotarization(7)

	// Now it's our turn to propose a new block.
	bb.BlockShouldBeBuilt <- struct{}{}

	block = <-bb.Out

	// Inject specifically votes from the last two nodes, to ensure the blacklisted node will be redeemed
	// the next time we will propose a block.
	for _, node := range nodes[2:] {
		testutil.InjectTestVote(t, e, block, node)
	}

	wal.AssertNotarization(8)

	// Now node 2 proposes a block.
	blacklist.Updates = nil
	block, _ = bb.BuildBlock(context.Background(), e.Metadata(), blacklist)
	block, _ = notarizeAndFinalizeRound(t, e, bb)

	// Now node 3 proposes a block.
	blacklist.Updates = nil
	block, _ = bb.BuildBlock(context.Background(), e.Metadata(), blacklist)
	block, _ = notarizeAndFinalizeRound(t, e, bb)

	// Node 4 is still blacklisted.
	emptyBlockMd = ProtocolMetadata{
		Round: 11,
	}
	emptyVoteFrom1 = createEmptyVote(emptyBlockMd, nodes[1])
	emptyVoteFrom2 = createEmptyVote(emptyBlockMd, nodes[2])

	e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom1,
	}, nodes[1])
	e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom2,
	}, nodes[2])

	wal.AssertNotarization(11)

	// Now it's our turn to propose a new block.
	bb.BlockShouldBeBuilt <- struct{}{}

	block = <-bb.Out

	require.Equal(t, Blacklist{
		NodeCount: 4,
		SuspectedNodes: SuspectedNodes{
			{
				NodeIndex:       3,
				SuspectingCount: 2,
				OrbitSuspected:  1,
				RedeemingCount:  1,
				OrbitToRedeem:   Orbit(12, 3, 4),
			},
		},
		Updates: []BlacklistUpdate{{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 3}},
	}, block.Blacklist(), "Node should vote to redeem the previously failed node")

	blacklist = Blacklist{
		NodeCount: 4,
		SuspectedNodes: SuspectedNodes{
			{
				NodeIndex:       3,
				SuspectingCount: 2,
				OrbitSuspected:  1,
				RedeemingCount:  2,
				OrbitToRedeem:   Orbit(12, 3, 4),
			},
		},
		Updates: []BlacklistUpdate{{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 3}},
	}

	block, _ = bb.BuildBlock(context.Background(), e.Metadata(), blacklist)
	block, _ = notarizeAndFinalizeRound(t, e, bb)

	// The next blacklist garbage collects node 3 from the blacklist.

	blacklist = Blacklist{
		NodeCount: 4,
		Updates:   []BlacklistUpdate{{Type: BlacklistOpType_NodeRedeemed, NodeIndex: 3}},
	}

	block, _ = bb.BuildBlock(context.Background(), e.Metadata(), blacklist)
	block, _ = notarizeAndFinalizeRound(t, e, bb)

	// Node 4 can now propose a block
	blacklist = Blacklist{
		NodeCount: 4,
	}

	block, _ = bb.BuildBlock(context.Background(), e.Metadata(), blacklist)
	notarizeAndFinalizeRound(t, e, bb)
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
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}

	comm := newRebroadcastComm(nodes)
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[3], comm, bb)
	epochTime := conf.StartTime
	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())
	require.Equal(t, uint64(0), e.Metadata().Round)
	require.Equal(t, uint64(0), e.Metadata().Round)
	require.False(t, wal.ContainsEmptyVote(0))

	bb.BlockShouldBeBuilt <- struct{}{}

	// wait for the initial empty vote broadcast
	// Wait for the initial empty vote broadcast for round 0
	waitForEmptyVote(t, comm, e, 0, epochTime)
	require.Len(t, comm.emptyVotes, 0)

	// Continue to rebroadcast for round 0
	for i := 0; i < 10; i++ {
		waitForEmptyVote(t, comm, e, 0, epochTime)
		wal.AssertWALSize(1)
	}

	emptyNotarization := testutil.NewEmptyNotarization(nodes, 0)
	e.HandleMessage(&simplex.Message{
		EmptyNotarization: emptyNotarization,
	}, nodes[2])

	wal.AssertNotarization(0)

	// Ensure rebroadcast was canceled
	epochTime = epochTime.Add(e.MaxRebroadcastWait * 2)
	e.AdvanceTime(epochTime)
	require.Len(t, comm.emptyVotes, 0)

	// Wait for empty vote broadcast for the next round (1)
	bb.BlockShouldBeBuilt <- struct{}{}
	waitForEmptyVote(t, comm, e, 1, epochTime)
	wal.AssertWALSize(3)

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

func runCrashAndRestartExecution(t *testing.T, e *Epoch, bb *testutil.TestBlockBuilder, wal *testutil.TestWAL, storage *testutil.InMemStorage, f epochExecution) {
	// Split the test into two scenarios:
	// 1) The node proceeds as usual.
	// 2) The node crashes and restarts.
	cloneWAL := wal.Clone()
	cloneStorage := storage.Clone()

	nodes := e.Comm.Nodes()

	// Clone the block builder
	bbAfterCrash := &testutil.TestBlockBuilder{
		Out:                cloneBlockChan(bb.Out),
		In:                 cloneBlockChan(bb.In),
		BlockShouldBeBuilt: make(chan struct{}, cap(bb.BlockShouldBeBuilt)),
	}

	// Case 1:
	t.Run(fmt.Sprintf("%s-no-crash", t.Name()), func(t *testing.T) {
		f(t, e, bb, storage, wal)
	})

	// Case 2:
	t.Run(fmt.Sprintf("%s-with-crash", t.Name()), func(t *testing.T) {
		conf, _, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bbAfterCrash)
		conf.Storage = cloneStorage
		conf.WAL = cloneWAL

		e, err := NewEpoch(conf)
		require.NoError(t, err)

		require.NoError(t, e.Start())
		f(t, e, bbAfterCrash, cloneStorage, cloneWAL)
	})
}

func cloneBlockChan(in chan *testutil.TestBlock) chan *testutil.TestBlock {
	tmp := make(chan *testutil.TestBlock, cap(in))
	out := make(chan *testutil.TestBlock, cap(in))

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
	SentMessages      chan *Message
}

func (rc *recordingComm) Broadcast(msg *Message) {
	if rc.BroadcastMessages != nil {
		rc.BroadcastMessages <- msg
	}
	rc.Communication.Broadcast(msg)
}

func (rc *recordingComm) Send(msg *Message, node NodeID) {
	if rc.SentMessages != nil {
		rc.SentMessages <- msg
	}
	rc.Communication.Send(msg, node)
}

type epochExecution func(t *testing.T, e *Epoch, bb *testutil.TestBlockBuilder, storage *testutil.InMemStorage, wal *testutil.TestWAL)

package simplex_test

import (
	"context"
	"testing"

	. "github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

// func TestEpochLeaderRecursivelyFetchPastNotarizedBlocks(t *testing.T) {
// 	l := testutil.MakeLogger(t, 3)

// 	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}

// 	nodes := NodeIDs{{1}, {2}, {3}, {4}}

// 	nodeID := nodes[0]

// 	recordedMessages := make(chan *Message, 100)
// 	comm := &recordingComm{Communication: testutil.NoopComm(nodes), SentMessages: recordedMessages, BroadcastMessages: recordedMessages}

// 	bb2 := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
// 	roundCount := 7
// 	var blocks []simplex.VerifiedBlock
// 	var notarizations []Notarization

// 	// Create blocks from 0 to roundCount - 1, skipping the rounds where the first node is the leader.
// 	var prevDigest Digest
// 	for round := 0; round < roundCount; round++ {
// 		if LeaderForRound(nodes, uint64(round)).Equals(nodeID) {
// 			continue
// 		}

// 		md := ProtocolMetadata{
// 			Round: uint64(round),
// 			Seq:   uint64(len(blocks)),
// 			Prev:  prevDigest,
// 		}

// 		block, ok := bb2.BuildBlock(context.Background(), md, emptyBlacklist)
// 		require.True(t, ok)

// 		prevDigest = block.BlockHeader().Digest

// 		notarization, err := testutil.NewNotarization(l, &testutil.TestSignatureAggregator{}, block, nodes[1:])
// 		require.NoError(t, err)

// 		blocks = append(blocks, block)
// 		notarizations = append(notarizations, notarization)
// 	}

// 	// Send the last block and notarization
// 	lastBlock := blocks[len(blocks)-1]
// 	lastNotarization := notarizations[len(blocks)-1]

// 	t.Log("Last block is", lastBlock.BlockHeader().Seq, "for round", lastBlock.BlockHeader().Round)

// 	leaderIndexOfLastBlock := (int(lastBlock.BlockHeader().Round)) % len(nodes)
// 	voteOnLastBlock, err := testutil.NewTestVote(lastBlock, nodes[leaderIndexOfLastBlock])

// 	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], comm, bb)

// 	e, err := NewEpoch(conf)
// 	require.NoError(t, err)

// 	require.NoError(t, e.Start())

// 	// Feed the node empty notarizations until it advances to the round of the last block.
// 	lastRound := lastBlock.BlockHeader().Round - 1
// 	notarizeEmptyRounds(t, e, lastRound)

// 	createResponseAndNodeOrigin := func(msg *Message) ([]*Message, []NodeID) {
// 		if msg.FinalizeVote != nil {
// 			var block simplex.VerifiedBlock
// 			// Find the block this finalize vote is for
// 			for _, b := range blocks {
// 				if b.BlockHeader().Seq == msg.FinalizeVote.Finalization.Seq {
// 					block = b
// 				}
// 			}
// 			require.NotNil(t, block)

// 			fv1 := testutil.NewTestFinalizeVote(t, block, nodes[1])
// 			fv2 := testutil.NewTestFinalizeVote(t, block, nodes[2])

// 			msg1 := &Message{
// 				FinalizeVote: fv1,
// 			}

// 			msg2 := &Message{
// 				FinalizeVote: fv2,
// 			}

// 			return []*Message{msg1, msg2}, []NodeID{nodes[1], nodes[2]}
// 		}

// 		nbr := msg.NotarizedBlockRequest
// 		if nbr == nil {
// 			return nil, nil
// 		}
// 		seq := msg.NotarizedBlockRequest.Seq
// 		for i, b := range blocks {
// 			if b.BlockHeader().Seq == seq {
// 				response := &NotarizedBlockResponse{
// 					Block:        b.(*testutil.TestBlock),
// 					Notarization: &notarizations[i],
// 				}
// 				round := response.Block.BlockHeader().Round
// 				leaderForRound := int(round) % len(nodes)
// 				return []*Message{{
// 					NotarizedBlockResponse: response,
// 				}}, []NodeID{nodes[leaderForRound]}
// 			}
// 		}
// 		require.FailNow(t, "could not find block with seq", seq)
// 		return nil, nil
// 	}

// 	quit := make(chan struct{})
// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	defer wg.Wait()
// 	defer close(quit)

// 	go func() {
// 		defer wg.Done()
// 		respondToNotarizationRequests(t, e, quit, recordedMessages, createResponseAndNodeOrigin)
// 	}()

// 	err = e.HandleMessage(&Message{
// 		BlockMessage: &BlockMessage{
// 			Block: lastBlock.(*testutil.TestBlock),
// 			Vote:  *voteOnLastBlock,
// 		},
// 	}, nodes[leaderIndexOfLastBlock])
// 	require.NoError(t, err)

// 	err = e.HandleMessage(&Message{
// 		Notarization: &lastNotarization,
// 	}, nodes[leaderIndexOfLastBlock])
// 	require.NoError(t, err)

// 	wal.AssertNotarization(lastBlock.BlockHeader().Round)

// 	err = e.HandleMessage(&Message{
// 		FinalizeVote: testutil.NewTestFinalizeVote(t, lastBlock, nodes[1]),
// 	}, nodes[1])
// 	require.NoError(t, err)

// 	err = e.HandleMessage(&Message{
// 		FinalizeVote: testutil.NewTestFinalizeVote(t, lastBlock, nodes[2]),
// 	}, nodes[2])
// 	require.NoError(t, err)

// 	t.Log("Waiting for block commit of last block", lastBlock.BlockHeader().Seq)
// 	storage.WaitForBlockCommit(lastBlock.BlockHeader().Seq)
// }

// func notarizeEmptyRounds(t *testing.T, e *Epoch, lastRound uint64) {
// 	nodes := e.Comm.Nodes()
// 	nodeID := e.EpochConfig.ID
// 	bb := e.BlockBuilder.(*testutil.TestBlockBuilder)
// 	wal := e.WAL.(*testutil.TestWAL)
// 	startTime := e.StartTime
// 	for round := uint64(0); round <= lastRound; round++ {
// 		emptyNotarization := testutil.NewEmptyNotarization(nodes[1:], round)

// 		if LeaderForRound(nodes, round).Equals(nodeID) {
// 			testutil.WaitToEnterRound(t, e, round)
// 			t.Log("Triggering block to be built for round", round)
// 			bb.BlockShouldBeBuilt <- struct{}{}
// 			testutil.WaitForBlockProposerTimeout(t, e, &startTime, round)

// 			err := e.HandleMessage(&Message{
// 				EmptyNotarization: emptyNotarization,
// 			}, nodes[2])
// 			require.NoError(t, err)

// 			wal.AssertNotarization(round)
// 			continue
// 		}

// 		// Otherwise, just receive the empty notarization
// 		// and advance to the next round
// 		err := e.HandleMessage(&Message{
// 			EmptyNotarization: emptyNotarization,
// 		}, nodes[2])
// 		require.NoError(t, err)
// 		wal.AssertNotarization(round)
// 		testutil.WaitToEnterRound(t, e, round)
// 	}
// }

// func TestDoubleFinalize(t *testing.T) {
// 	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
// 	ctx := context.Background()
// 	nodes := []NodeID{{1}, {2}, {3}, {4}}
// 	initialBlock := createBlocks(t, nodes, 1)[0]
// 	recordingComm := &recordingComm{Communication: testutil.NewNoopComm(nodes), BroadcastMessages: make(chan *Message, 100), SentMessages: make(chan *Message, 100)}
// 	conf, _, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], recordingComm, bb)
// 	storage.Index(ctx, initialBlock.VerifiedBlock, initialBlock.Finalization)

// 	e, err := NewEpoch(conf)
// 	require.NoError(t, err)

// 	require.NoError(t, e.Start())
// 	require.Equal(t, uint64(1), e.Metadata().Seq)

// 	// we receive a block, but then empty notarize
// 	roundOne(t, e, nodes, bb)

// }

// func roundOne(t *testing.T, e *Epoch, nodes []NodeID, bb *testutil.TestBlockBuilder) {
// 	ctx := context.Background()
// 	// this block gets sent but not finalized
// 	bb.BuildBlock(ctx, e.Metadata(), emptyBlacklist)
// 	block1 := <-bb.Out
// 	vote, err := testutil.NewTestVote(block1, nodes[1])
// 	require.NoError(t, err)
// 	err = e.HandleMessage(&Message{
// 		BlockMessage: &BlockMessage{
// 			Block: block1,
// 			Vote:  *vote,
// 		},
// 	}, nodes[1])
// 	require.NoError(t, err)
// 	bb.BlockShouldBeBuilt <- struct{}{}

// 	time.Sleep(100 * time.Millisecond)
// 	// send two empty votes
// 	emptyVoteRound1Node0 := createEmptyVote(e.Metadata(), nodes[1])
// 	err = e.HandleMessage(&Message{
// 		EmptyVoteMessage: emptyVoteRound1Node0,
// 	}, nodes[1])
// 	require.NoError(t, err)
// 	emptyVoteRound1Node1 := createEmptyVote(e.Metadata(), nodes[2])
// 	err = e.HandleMessage(&Message{
// 		EmptyVoteMessage: emptyVoteRound1Node1,
// 	}, nodes[2])
// 	require.NoError(t, err)

// 	// we end up timing out and moving to the next round
// 	e.AdvanceTime(e.StartTime.Add(e.EpochConfig.MaxProposalWait))
// 	testutil.WaitToEnterRound(t, e, 2)
// }

func TestChainBreak(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	initialBlock := createBlocks(t, nodes, 1)[0]
	recordingComm := &recordingComm{Communication: testutil.NewNoopComm(nodes), BroadcastMessages: make(chan *Message, 100), SentMessages: make(chan *Message, 100)}
	conf, _, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], recordingComm, bb)
	storage.Index(ctx, initialBlock.VerifiedBlock, initialBlock.Finalization)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())
	require.Equal(t, uint64(1), e.Metadata().Seq)

	// we receive a block, but then empty notarize
	advanceRoundFromNotarization(t, e, bb)
	require.Equal(t, uint64(2), e.Metadata().Seq)


	advanceRoundWithMD(t, e, bb, true, true,  ProtocolMetadata{
		Round: 2,
		Seq:   1, // next seq is 1 not 2
		Prev:  initialBlock.VerifiedBlock.BlockHeader().Prev,
	})
	require.NotEqual(t, uint64(3), e.Metadata().Seq)
}
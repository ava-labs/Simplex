package simplex_test

import (
	"context"
	"testing"

	"github.com/ava-labs/simplex"
	. "github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

// TestChainBreak tests that a node should not send two finalize votes for the same sequence number
// It does so by advancing round 1 from a notarization, then advancing round 2 using a block built off an supposed empty notarization from round 1.
func TestChainBreak(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	initialBlock := createBlocks(t, nodes, 1)[0]
	recordingComm := &recordingComm{Communication: testutil.NewNoopComm(nodes), BroadcastMessages: make(chan *Message, 100), SentMessages: make(chan *Message, 100)}
	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], recordingComm, bb)
	storage.Index(ctx, initialBlock.VerifiedBlock, initialBlock.Finalization)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())
	require.Equal(t, uint64(1), e.Metadata().Seq)

	// we receive a block and then notarize(this sends out a finalize vote for the block)
	advanceRoundFromNotarization(t, e, bb)
	require.Equal(t, uint64(2), e.Metadata().Seq)
	require.Equal(t, uint64(2), e.Metadata().Round)

	// wait for finalize votes
	for {
		msg := <-recordingComm.BroadcastMessages
		if msg.FinalizeVote != nil {
			require.Equal(t, uint64(1), msg.FinalizeVote.Finalization.Round)
			require.Equal(t, uint64(1), msg.FinalizeVote.Finalization.Seq)
			break
		}
	}

	// clear the recorded messages
	for len(recordingComm.BroadcastMessages) > 0 {
		<-recordingComm.BroadcastMessages
	}

	md := ProtocolMetadata{
		Round: 2,
		Seq:   1, // set next seq to 1 not 2
		Prev:  initialBlock.VerifiedBlock.BlockHeader().Digest,
	}
	_, ok := bb.BuildBlock(context.Background(), md, simplex.Blacklist{
		NodeCount: uint16(len(e.EpochConfig.Comm.Nodes())),
	})
	require.True(t, ok)

	block := <-bb.Out
	block.OnVerify = func() {
		require.Fail(t, "block should not be verified since we don't have empty notarization for round 1")
	}

	// send block from leader
	vote, err := testutil.NewTestVote(block, nodes[2])
	require.NoError(t, err)
	err = e.HandleMessage(&simplex.Message{
		BlockMessage: &simplex.BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[2])
	require.NoError(t, err)

	block.OnVerify = nil
	// now lets send empty notarization
	emptyNotarization := testutil.NewEmptyNotarization(nodes, 1)
	err = e.HandleMessage(&simplex.Message{
		EmptyNotarization: emptyNotarization,
	}, nodes[3])
	require.NoError(t, err)

	// create a notarization and now we should send a finalize vote for seq 1 again
	notarization, err := testutil.NewNotarization(e.Logger, e.SignatureAggregator, block, nodes[1:])
	require.NoError(t, err)
	testutil.InjectTestNotarization(t, e, notarization, nodes[1])

	wal.AssertNotarization(block.Metadata.Round)

	// wait for finalize votes
	for {
		msg := <-recordingComm.BroadcastMessages
		if msg.FinalizeVote != nil {
			require.Equal(t, uint64(2), msg.FinalizeVote.Finalization.Round)
			require.Equal(t, uint64(1), msg.FinalizeVote.Finalization.Seq)
			break
		}
	}

	require.Equal(t, uint64(2), e.Metadata().Seq)
	require.Equal(t, uint64(3), e.Metadata().Round)
	// advanceRoundWithMD(t, e, bb, true, true,  ProtocolMetadata{
	// 	Round: 2,
	// 	Seq:   1, // next seq is 1 not 2
	// 	Prev:  initialBlock.VerifiedBlock.BlockHeader().Digest,
	// })


	// for {
	// 	msg := <-recordingComm.BroadcastMessages
	// 	if msg.FinalizeVote != nil {
	// 		// we should not have sent two different finalize votes for the same seq
	// 		require.NotEqual(t, uint64(2), msg.FinalizeVote.Finalization.Round)
	// 		require.NotEqual(t, uint64(1), msg.FinalizeVote.Finalization.Seq)
	// 		break
	// 	}

	// 	if len(recordingComm.BroadcastMessages) == 0 {
	// 		break
	// 	}
	// }
}

// TestChainBreakComplement does the complement of TestChainBreak. It assumes our node gets an empty notarization first, but then receives a block assuming there was a notarization.
func TestChainBreakComplement(t *testing.T) {
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

	// we receive a block and then notarize(this sends out a finalize vote for the block)
	advanceRoundFromEmpty(t, e)
	require.Equal(t, uint64(1), e.Metadata().Seq)
	require.Equal(t, uint64(2), e.Metadata().Round)

	_, ok := bb.BuildBlock(context.Background(), ProtocolMetadata{
		Round: 1,
		Seq:   1,
		Prev:  initialBlock.VerifiedBlock.BlockHeader().Digest,
	}, simplex.Blacklist{
		NodeCount: uint16(len(e.EpochConfig.Comm.Nodes())),
	})
	require.True(t, ok)
	missingNotarizationBlock := <-bb.Out

	md := ProtocolMetadata{
		Round: 2,
		Seq:   2, // set next seq to 2(our node expects 1)
		Prev:  missingNotarizationBlock.Digest,
	}
	_, ok = bb.BuildBlock(context.Background(), md, simplex.Blacklist{
		NodeCount: uint16(len(e.EpochConfig.Comm.Nodes())),
	})
	require.True(t, ok)
	round2Block := <-bb.Out


	round2Block.OnVerify = func() {
		require.Fail(t, "block should not be verified since we don't have empty notarization for round 1")
	}

	// send block from leader
	vote, err := testutil.NewTestVote(round2Block, nodes[2])
	require.NoError(t, err)
	err = e.HandleMessage(&simplex.Message{
		BlockMessage: &simplex.BlockMessage{
			Vote:  *vote,
			Block: round2Block,
		},
	}, nodes[2])
	require.NoError(t, err)

	// currently we reject blocks if we dont have its parent ^
	// but we shouldnt when we receive a notarization for that block

	// create a notarization and now we should send a finalize vote for seq 1 again
	notarization, err := testutil.NewNotarization(e.Logger, e.SignatureAggregator, round2Block, nodes[1:])
	require.NoError(t, err)
	testutil.InjectTestNotarization(t, e, notarization, nodes[1])

	// wal.AssertNotarization(block.Metadata.Round)

	// // wait for finalize votes
	// for {
	// 	msg := <-recordingComm.BroadcastMessages
	// 	if msg.FinalizeVote != nil {
	// 		require.Equal(t, uint64(2), msg.FinalizeVote.Finalization.Round)
	// 		require.Equal(t, uint64(1), msg.FinalizeVote.Finalization.Seq)
	// 		break
	// 	}
	// }

	// require.Equal(t, uint64(2), e.Metadata().Seq)
	// require.Equal(t, uint64(3), e.Metadata().Round)
	// advanceRoundWithMD(t, e, bb, true, true,  ProtocolMetadata{
	// 	Round: 2,
	// 	Seq:   1, // next seq is 1 not 2
	// 	Prev:  initialBlock.VerifiedBlock.BlockHeader().Digest,
	// })


	// for {
	// 	msg := <-recordingComm.BroadcastMessages
	// 	if msg.FinalizeVote != nil {
	// 		// we should not have sent two different finalize votes for the same seq
	// 		require.NotEqual(t, uint64(2), msg.FinalizeVote.Finalization.Round)
	// 		require.NotEqual(t, uint64(1), msg.FinalizeVote.Finalization.Seq)
	// 		break
	// 	}

	// 	if len(recordingComm.BroadcastMessages) == 0 {
	// 		break
	// 	}
	// }
}



// TODO: test where we have the case above, but we received the block proposal
// ^^ hmm we would have received the block proposal but never added it to rounds since there is no prev digest

// returns the seq and the digest we have sent a finalize vote for
func advanceWithFinalizeCheck(t *testing.T, e *Epoch, recordingComm *recordingComm, bb *testutil.TestBlockBuilder) (uint64, Digest){
	round := e.Metadata().Round
	seq := e.Metadata().Seq
	advanceRoundFromNotarization(t, e, bb)

	// wait for finalize votes for the round
	for {
		msg := <-recordingComm.BroadcastMessages
		if msg.FinalizeVote != nil {
			require.Equal(t, round, msg.FinalizeVote.Finalization.Round)
			require.Equal(t, seq, msg.FinalizeVote.Finalization.Seq)
			return seq, msg.FinalizeVote.Finalization.Digest
		}
	}
}

func TestChainBreakLargeGap(t *testing.T) {
	for numEmpty := range uint64(5) {
		for numNotarizations := range uint64(5) {
			for seqToDoubleFinalize := range numNotarizations {
				testChainBreakLargeGap(t, numEmpty, numNotarizations, seqToDoubleFinalize)
			}
		}
	} 
}

func testChainBreakLargeGap(t *testing.T, numEmptyNotarizations uint64, numNotarizations uint64, seqToDoubleFinalize uint64) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
	ctx := context.Background()
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	initialBlock := createBlocks(t, nodes, 1)[0]
	recordingComm := &recordingComm{Communication: testutil.NewNoopComm(nodes), BroadcastMessages: make(chan *Message, 100), SentMessages: make(chan *Message, 100)}
	conf, _, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], recordingComm, bb)
	storage.Index(ctx, initialBlock.VerifiedBlock, initialBlock.Finalization)

	finalizeVoteSeqs := make(map[uint64]Digest)
	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())
	require.Equal(t, uint64(1), e.Metadata().Seq)

	notarizationsLeft := numNotarizations
	for i := uint64(0); i < numEmptyNotarizations; i++ {
		leader := LeaderForRound(e.Comm.Nodes(), e.Metadata().Round)
		if e.ID.Equals(leader) {
			require.NotZero(t, notarizationsLeft)
			seq, digest := advanceWithFinalizeCheck(t, e, recordingComm, bb)
			finalizeVoteSeqs[seq] = digest
			notarizationsLeft--
			i--
			continue
		}

		advanceRoundFromEmpty(t, e)
	}

	for range notarizationsLeft {
		seq, digest := advanceWithFinalizeCheck(t, e, recordingComm, bb)
		finalizeVoteSeqs[seq] = digest
	}

	require.Equal(t, 1+numEmptyNotarizations+numNotarizations, e.Metadata().Round)
	require.Equal(t, 1 + numNotarizations, e.Metadata().Seq)

	// clear the recorded messages
	for len(recordingComm.BroadcastMessages) > 0 {
		<-recordingComm.BroadcastMessages
	}

	advanceRoundWithMD(t, e, bb, true, true,  ProtocolMetadata{
		Round: e.Metadata().Round,
		Seq:   seqToDoubleFinalize,
		Prev:  finalizeVoteSeqs[seqToDoubleFinalize-1],
	})

	for {
		msg := <-recordingComm.BroadcastMessages
		if msg.FinalizeVote != nil {
			// we should not have sent two different finalize votes for the same seq
			// require.NotEqual(t, e, msg.FinalizeVote.Finalization.Round)
			require.NotEqual(t, seqToDoubleFinalize, msg.FinalizeVote.Finalization.Seq)
			break
		}

		if len(recordingComm.BroadcastMessages) == 0 {
			break
		}
	}
}

// garbageCollectSuspectedNodes progresses [e] to a new round. If [notarize] is set, the round will progress due to a notarization.
// If [finalize] is set, the round will advance and the block will be indexed to storage.
func advanceRoundWithMD(t *testing.T, e *simplex.Epoch, bb *testutil.TestBlockBuilder, notarize bool, finalize bool, md simplex.ProtocolMetadata) (simplex.VerifiedBlock, *simplex.Notarization) {
	require.True(t, notarize || finalize, "must either notarize or finalize a round to advance")
	nextSeqToCommit := e.Storage.NumBlocks()
	nodes := e.Comm.Nodes()
	quorum := simplex.Quorum(len(nodes))
	// leader is the proposer of the new block for the given round
	leader := simplex.LeaderForRound(nodes, md.Round)
	// only create blocks if we are not the node running the epoch
	isEpochNode := leader.Equals(e.ID)
	if !isEpochNode {
		_, ok := bb.BuildBlock(context.Background(), md, simplex.Blacklist{
			NodeCount: uint16(len(e.EpochConfig.Comm.Nodes())),
		})
		require.True(t, ok)
	}

	block := <-bb.Out

	if !isEpochNode {
		// send node a message from the leader
		vote, err := testutil.NewTestVote(block, leader)
		require.NoError(t, err)
		err = e.HandleMessage(&simplex.Message{
			BlockMessage: &simplex.BlockMessage{
				Vote:  *vote,
				Block: block,
			},
		}, leader)
		require.NoError(t, err)
	}

	var notarization *simplex.Notarization
	if notarize {
		// start at one since our node has already voted
		n, err := testutil.NewNotarization(e.Logger, e.SignatureAggregator, block, nodes[0:quorum])
		testutil.InjectTestNotarization(t, e, n, nodes[1])

		e.WAL.(*testutil.TestWAL).AssertNotarization(block.Metadata.Round)
		require.NoError(t, err)
		notarization = &n
	}

	if finalize {
		for i := 0; i <= quorum; i++ {
			if nodes[i].Equals(e.ID) {
				continue
			}
			testutil.InjectTestFinalizeVote(t, e, block, nodes[i])
		}

		if nextSeqToCommit != block.Metadata.Seq {
			testutil.WaitToEnterRound(t, e, block.Metadata.Round+1)
			return block, notarization
		}

		blockFromStorage := e.Storage.(*testutil.InMemStorage).WaitForBlockCommit(block.Metadata.Seq)
		require.Equal(t, block, blockFromStorage)
	}

	return block, notarization
}

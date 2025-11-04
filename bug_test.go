package simplex_test

import (
	"context"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	. "github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

// returns the seq and the digest we have sent a finalize vote for
func advanceWithFinalizeCheck(t *testing.T, e *Epoch, recordingComm *recordingComm, bb *testutil.TestBlockBuilder) *FinalizeVote {
	time.Sleep(100 * time.Millisecond)
	round := e.Metadata().Round
	seq := e.Metadata().Seq
	advanceRoundFromNotarization(t, e, bb)

	// wait for finalize votes for the round
	for {
		msg := <-recordingComm.BroadcastMessages
		if msg.FinalizeVote != nil {
			require.Equal(t, round, msg.FinalizeVote.Finalization.Round)
			require.Equal(t, seq, msg.FinalizeVote.Finalization.Seq)
			return msg.FinalizeVote
		}
	}
}

// TestFinalizeSameSequence tests that a node only verifies a block after it receives all the empty notarizations the block depends on.
// This means the node can(and should) send a finalize vote for the same sequence if and only if a valid empty notarization has been verified.
//
// The test does this
// Round 0 := finalized block of seq 0
// Round 1 := receive block of seq 1, round 1 advance from notarization & send finalize vote
// Round 2 := receive block of seq 1, round 2 and are unable to verify(because we don't have empty notarization yet)
//
// Once we receive the empty notarization, we can verify the block(seq 1, round 2) and send out a finalize vote
func TestFinalizeSameSequence(t *testing.T) {
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

	// give some time for block to be (not)verified
	time.Sleep(100 * time.Millisecond)
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
}

// func TestFinalizeSameSequenceGap(t *testing.T) {
// 	nodes := []NodeID{{1}, {2}, {3}, {4}}

// 	// for numEmpty := range uint64(5) {
// 	// 	for numNotarizations := range uint64(5) {
// 	// 		leader := LeaderForRound(nodes, 1 + numEmpty + numNotarizations)
// 	// 		if leader.Equals(nodes[0]) {
// 	// 			continue
// 	// 		}

// 	// 		for seqToDoubleFinalize := uint64(1); seqToDoubleFinalize <= numNotarizations; seqToDoubleFinalize++ {
// 	// 			t.Run(fmt.Sprintf("empty=%d notarizations=%d seq=%d", numEmpty, numNotarizations, seqToDoubleFinalize), func(t *testing.T) {
// 	// 				t.Parallel()
// 	// 				testFinalizeSameSequenceGap(t, nodes, numEmpty, numNotarizations, seqToDoubleFinalize)
// 	// 			})
// 	// 		}
// 	// 	}
// 	// }
// 	testFinalizeSameSequenceGap(t, nodes, 3, 2, 2)
// }

// func testFinalizeSameSequenceGap(t *testing.T, nodes []NodeID, numEmptyNotarizations uint64, numNotarizations uint64, seqToDoubleFinalize uint64) {
// 	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)}
// 	ctx := context.Background()
// 	initialBlock := createBlocks(t, nodes, 1)[0]
// 	recordingComm := &recordingComm{Communication: testutil.NewNoopComm(nodes), BroadcastMessages: make(chan *Message, 100), SentMessages: make(chan *Message, 100)}
// 	conf, _, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], recordingComm, bb)
// 	storage.Index(ctx, initialBlock.VerifiedBlock, initialBlock.Finalization)

// 	e, err := NewEpoch(conf)
// 	require.NoError(t, err)

// 	require.NoError(t, e.Start())
// 	require.Equal(t, uint64(1), e.Metadata().Seq)

// 	finalizeVoteSeqs := make(map[uint64]*FinalizeVote)
// 	finalizeVoteSeqs[0] = &FinalizeVote{
// 		Finalization: ToBeSignedFinalization{
// 			BlockHeader: initialBlock.VerifiedBlock.BlockHeader(),
// 		},
// 	}
// 	notarizationsLeft := numNotarizations

// 	for i := uint64(0); i < numEmptyNotarizations; i++ {
// 		leader := LeaderForRound(e.Comm.Nodes(), e.Metadata().Round)
// 		if e.ID.Equals(leader) {
// 			require.NotZero(t, notarizationsLeft)
// 			fVote := advanceWithFinalizeCheck(t, e, recordingComm, bb)
// 			finalizeVoteSeqs[fVote.Finalization.Seq] = fVote
// 			notarizationsLeft--
// 			i--
// 			continue
// 		}

// 		advanceRoundFromEmpty(t, e)
// 	}

// 	for range notarizationsLeft {
// 		fmt.Println("epoch round before notarization:", e.Metadata().Round)
// 		fmt.Println("epoch seq before notarization:", e.Metadata().Seq)
// 		fVote := advanceWithFinalizeCheck(t, e, recordingComm, bb)
// 		finalizeVoteSeqs[fVote.Finalization.Seq] = fVote
// 	}

// 	// require.Equal(t, 1+numEmptyNotarizations+numNotarizations, e.Metadata().Round)
// 	// require.Equal(t, 1 + numNotarizations, e.Metadata().Seq)

// 	// // clear the recorded messages
// 	// for len(recordingComm.BroadcastMessages) > 0 {
// 	// 	<-recordingComm.BroadcastMessages
// 	// }

// 	// md := ProtocolMetadata{
// 	// 	Round: 1 + numEmptyNotarizations + numNotarizations,
// 	// 	Seq:   seqToDoubleFinalize,
// 	// 	Prev:  finalizeVoteSeqs[seqToDoubleFinalize-1].Finalization.Digest,
// 	// }
// 	// _, ok := bb.BuildBlock(context.Background(), md, simplex.Blacklist{
// 	// 	NodeCount: uint16(len(e.EpochConfig.Comm.Nodes())),
// 	// })
// 	// require.True(t, ok)

// 	// block := <-bb.Out
// 	// block.OnVerify = func() {
// 	// 	require.Fail(t, "block should not be verified since we don't have empty notarization for round 1")
// 	// }

// 	// leader := LeaderForRound(e.Comm.Nodes(), 1 + numEmptyNotarizations + numNotarizations)
// 	// require.False(t, e.ID.Equals(leader), "leader should not be the epoch node running the test")
// 	// // send block from leader
// 	// vote, err := testutil.NewTestVote(block, leader)
// 	// require.NoError(t, err)
// 	// err = e.HandleMessage(&simplex.Message{
// 	// 	BlockMessage: &simplex.BlockMessage{
// 	// 		Vote:  *vote,
// 	// 		Block: block,
// 	// 	},
// 	// }, leader)
// 	// require.NoError(t, err)

// 	// // give some time for block to be (not)verified
// 	// time.Sleep(100 * time.Millisecond)
// 	// verified := make(chan struct{}, 1)
// 	// block.OnVerify = func() {
// 	// 	verified <- struct{}{}
// 	// }

// 	// // now lets send empty notarizations seqToDoubleFinalize.round - seqToDoubleFinalize-1.round
// 	// startMissingNotarizationRound := finalizeVoteSeqs[seqToDoubleFinalize-1].Finalization.Round + 1
// 	// for i := startMissingNotarizationRound; i < md.Round; i++ {
// 	// 	emptyNotarization := testutil.NewEmptyNotarization(nodes, i)
// 	// 	err = e.HandleMessage(&simplex.Message{
// 	// 		EmptyNotarization: emptyNotarization,
// 	// 	}, nodes[3])
// 	// 	require.NoError(t, err)
// 	// }

// 	// // // TODO: test too large of a gap(should not schedule blocks)
// 	// <-verified

// 	// // drain any finalize votes that were sent
// 	// for len(recordingComm.BroadcastMessages) > 0 {
// 	// 	<-recordingComm.BroadcastMessages
// 	// }

// 	// // create a notarization and now we should send a finalize vote for seqToDoubleFinalize again
// 	// notarization, err := testutil.NewNotarization(e.Logger, e.SignatureAggregator, block, nodes[1:])
// 	// require.NoError(t, err)
// 	// testutil.InjectTestNotarization(t, e, notarization, nodes[1])

// 	// wal.AssertNotarization(block.Metadata.Round)

// 	// // wait for finalize votes
// 	// for {
// 	// 	msg := <-recordingComm.BroadcastMessages
// 	// 	if msg.FinalizeVote != nil {
// 	// 		require.Equal(t, seqToDoubleFinalize, msg.FinalizeVote.Finalization.Seq)
// 	// 		break
// 	// 	}
// 	// }

// 	// require.Equal(t, 2 + numEmptyNotarizations + numNotarizations, e.Metadata().Round)
// }

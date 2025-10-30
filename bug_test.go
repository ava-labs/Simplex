package simplex_test

import (
	"context"
	"testing"

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
	conf, _, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], recordingComm, bb)
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

	advanceRoundWithMD(t, e, bb, true, true,  ProtocolMetadata{
		Round: 2,
		Seq:   1, // next seq is 1 not 2
		Prev:  initialBlock.VerifiedBlock.BlockHeader().Digest,
	})


	for {
		msg := <-recordingComm.BroadcastMessages
		if msg.FinalizeVote != nil {
			// we should not have sent two different finalize votes for the same seq
			require.NotEqual(t, uint64(2), msg.FinalizeVote.Finalization.Round)
			require.NotEqual(t, uint64(1), msg.FinalizeVote.Finalization.Seq)
			break
		}

		if len(recordingComm.BroadcastMessages) == 0 {
			break
		}
	}
}

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
package simplex_test

import (
	"context"
	"testing"

	. "github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

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
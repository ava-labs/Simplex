package nonvalidator

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

func TestSkipMessage(t *testing.T) {
	tests := []struct {
		name     string
		msg      *simplex.Message
		expected bool
	}{
		{
			name:     "EmptyNotarization",
			msg:      &simplex.Message{EmptyNotarization: &simplex.EmptyNotarization{}},
			expected: true,
		},
		{
			name:     "VoteMessage",
			msg:      &simplex.Message{VoteMessage: &simplex.Vote{}},
			expected: true,
		},
		{
			name:     "EmptyVoteMessage",
			msg:      &simplex.Message{EmptyVoteMessage: &simplex.EmptyVote{}},
			expected: true,
		},
		{
			name:     "Notarization",
			msg:      &simplex.Message{Notarization: &simplex.Notarization{}},
			expected: true,
		},
		{
			name:     "ReplicationResponse",
			msg:      &simplex.Message{ReplicationResponse: &simplex.ReplicationResponse{}},
			expected: true,
		},
		{
			name:     "ReplicationRequest",
			msg:      &simplex.Message{ReplicationRequest: &simplex.ReplicationRequest{}},
			expected: true,
		},
		{
			name:     "FinalizeVote",
			msg:      &simplex.Message{FinalizeVote: &simplex.FinalizeVote{}},
			expected: true,
		},
		{
			name:     "BlockDigestRequest",
			msg:      &simplex.Message{BlockDigestRequest: &simplex.BlockDigestRequest{}},
			expected: false,
		},
		{
			name:     "BlockMessage",
			msg:      &simplex.Message{BlockMessage: &simplex.BlockMessage{}},
			expected: false,
		},
		{
			name:     "Finalization",
			msg:      &simplex.Message{Finalization: &simplex.Finalization{}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, skipMessage(tt.msg))
		})
	}
}

func newTestNonValidator(t *testing.T, nodes []simplex.NodeID, lastVerifiedBlock simplex.Block) *NonValidator {
	config := Config{
		Storage:           testutil.NewNonValidatorInMemoryStorage(),
		Logger:            testutil.MakeLogger(t, 1),
		GenesisValidators: nodes,
	}

	return NewNonValidator(config, lastVerifiedBlock)
}

func blockMessage(t *testing.T, block simplex.Block, from simplex.NodeID) *simplex.Message {
	vote, err := testutil.NewTestVote(block, from)
	require.NoError(t, err)
	return &simplex.Message{
		BlockMessage: &simplex.BlockMessage{
			Block: block,
			Vote:  *vote,
		},
	}
}

func TestHandleFinalizationMessage(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}

	tests := []struct {
		name              string
		lastVerifiedBlock *testutil.TestBlock
		// sendBlock controls whether a block message is sent before the finalization.
		sendBlock         bool
		blockSender       simplex.NodeID
		expectVerified    bool
		expectedNumBlocks uint64
	}{
		{
			name: "Finalization Only No Block",
		},
		{
			name:        "Block From Non-Leader Then Finalization",
			sendBlock:   true,
			blockSender: nodes[1],
		},
		{
			name:              "Block From Leader Then Finalization",
			sendBlock:         true,
			blockSender:       nodes[0],
			expectVerified:    true,
			expectedNumBlocks: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lastVerified simplex.Block
			if tt.lastVerifiedBlock != nil {
				lastVerified = tt.lastVerifiedBlock
			}
			v := newTestNonValidator(t, nodes, lastVerified)

			var verified atomic.Bool
			blockToSend := testutil.NewTestBlock(simplex.ProtocolMetadata{
				Round: 0,
				Seq:   0,
				Epoch: 0,
			}, simplex.Blacklist{})
			blockToSend.OnVerify = func() {
				verified.Store(true)
			}

			if tt.sendBlock {
				err := v.HandleMessage(blockMessage(t, blockToSend, nodes[0]), tt.blockSender)
				require.NoError(t, err)
			}

			finalization, _ := testutil.NewFinalizationRecord(t, v.Logger, &testutil.TestSignatureAggregator{}, blockToSend, nodes)
			err := v.HandleMessage(&simplex.Message{Finalization: &finalization}, nodes[0])
			require.NoError(t, err)

			if tt.expectVerified {
				require.Eventually(t, verified.Load, 2*time.Second, 20*time.Millisecond)
			} else {
				require.Never(t, verified.Load, 2*time.Second, 20*time.Millisecond)
			}
			require.Equal(t, tt.expectedNumBlocks, v.Storage.NumBlocks())
		})
	}
}

func TestHandleBlockMessage(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}

	tests := []struct {
		name              string
		lastVerifiedBlock *testutil.TestBlock
		// finalizationBlock returns the block to finalize; nil means no finalization is sent.
		// lastVerified may be nil when lastVerifiedBlock is not set for the test case.
		finalizationBlock func(lastVerified, blockToSend *testutil.TestBlock) *testutil.TestBlock
		blockSender       simplex.NodeID
		expectVerified    bool
		expectedNumBlocks uint64
	}{
		{
			name:        "Next to Verify But No Finalization",
			blockSender: nodes[0],
		},
		{
			name:              "BlockMessage not sent from leader",
			finalizationBlock: func(_, blockToSend *testutil.TestBlock) *testutil.TestBlock { return blockToSend },
			blockSender:       nodes[1],
		},
		{
			name: "Already Verified",
			lastVerifiedBlock: testutil.NewTestBlock(simplex.ProtocolMetadata{
				Round: 0,
				Seq:   0,
				Epoch: 0,
			}, simplex.Blacklist{}),
			finalizationBlock: func(lastVerified, _ *testutil.TestBlock) *testutil.TestBlock { return lastVerified },
			blockSender:       nodes[1],
		},
		{
			name:              "Finalization Received",
			finalizationBlock: func(_, blockToSend *testutil.TestBlock) *testutil.TestBlock { return blockToSend },
			blockSender:       nodes[0],
			expectVerified:    true,
			expectedNumBlocks: 1,
		},
		{
			name:              "Finalization Received But Not Next To Verify",
			finalizationBlock: func(_, blockToSend *testutil.TestBlock) *testutil.TestBlock { return blockToSend },
			blockSender:       nodes[0],
			expectVerified:    true,
			expectedNumBlocks: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lastVerified simplex.Block
			if tt.lastVerifiedBlock != nil {
				lastVerified = tt.lastVerifiedBlock
			}
			v := newTestNonValidator(t, nodes, lastVerified)

			var verified atomic.Bool
			blockToSend := testutil.NewTestBlock(simplex.ProtocolMetadata{
				Round: 0,
				Seq:   0,
				Epoch: 0,
			}, simplex.Blacklist{})
			blockToSend.OnVerify = func() {
				verified.Store(true)
			}

			if tt.finalizationBlock != nil {
				finalizeBlock := tt.finalizationBlock(tt.lastVerifiedBlock, blockToSend)
				finalization, _ := testutil.NewFinalizationRecord(t, v.Logger, &testutil.TestSignatureAggregator{}, finalizeBlock, nodes)
				v.HandleMessage(&simplex.Message{Finalization: &finalization}, nodes[0])
			}

			err := v.HandleMessage(blockMessage(t, blockToSend, nodes[0]), tt.blockSender)
			require.NoError(t, err)

			if tt.expectVerified {
				require.Eventually(t, verified.Load, 2*time.Second, 20*time.Millisecond)
			} else {
				require.Never(t, verified.Load, 2*time.Second, 20*time.Millisecond)
			}
			require.Equal(t, tt.expectedNumBlocks, v.Storage.NumBlocks())
		})
	}
}

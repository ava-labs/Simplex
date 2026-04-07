package nonvalidator

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

type errQC struct{}

func (errQC) Signers() []simplex.NodeID { return nil }
func (errQC) Verify([]byte) error       { return errors.New("verification failed") }
func (errQC) Bytes() []byte             { return nil }

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

func TestHandleBlockDigestMismatch(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	v := newTestNonValidator(t, nodes, nil)

	metadata := simplex.ProtocolMetadata{Seq: 0, Epoch: 0, Round: 0}
	blockA := testutil.NewTestBlock(metadata, simplex.Blacklist{})
	blockB := testutil.NewTestBlock(metadata, simplex.Blacklist{})
	blockB.Data = []byte("different")
	blockB.ComputeDigest()

	// send finalization for blockB
	finalization, _ := testutil.NewFinalizationRecord(t, v.Logger, &testutil.TestSignatureAggregator{}, blockB, nodes)
	err := v.HandleMessage(&simplex.Message{Finalization: &finalization}, nodes[0])
	require.NoError(t, err)

	// send block message for blockA — digest differs from stored finalization
	err = v.HandleMessage(blockMessage(t, blockA, nodes[0]), nodes[0])
	require.NoError(t, err)

	require.Equal(t, uint64(0), v.Storage.NumBlocks())
}

func TestHandleFinalizationDigestMismatch(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	v := newTestNonValidator(t, nodes, nil)

	metadata := simplex.ProtocolMetadata{Seq: 0, Epoch: 0, Round: 0}
	blockA := testutil.NewTestBlock(metadata, simplex.Blacklist{})
	blockB := testutil.NewTestBlock(metadata, simplex.Blacklist{})
	blockB.Data = []byte("different")
	blockB.ComputeDigest()

	// send block message for blockA from leader
	err := v.HandleMessage(blockMessage(t, blockA, nodes[0]), nodes[0])
	require.NoError(t, err)

	// send finalization for blockB — digest differs from stored block
	finalization, _ := testutil.NewFinalizationRecord(t, v.Logger, &testutil.TestSignatureAggregator{}, blockB, nodes)
	err = v.HandleMessage(&simplex.Message{Finalization: &finalization}, nodes[0])
	require.NoError(t, err)

	require.Equal(t, uint64(0), v.Storage.NumBlocks())
}

func TestHandleFinalizationFailsVerification(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	v := newTestNonValidator(t, nodes, nil)

	var verified atomic.Bool
	block := testutil.NewTestBlock(simplex.ProtocolMetadata{
		Round: 0,
		Seq:   0,
		Epoch: 0,
	}, simplex.Blacklist{})
	block.OnVerify = func() {
		verified.Store(true)
	}

	// send block from leader
	err := v.HandleMessage(blockMessage(t, block, nodes[0]), nodes[0])
	require.NoError(t, err)

	// send a finalization that fails verification
	finalization := &simplex.Finalization{
		QC: errQC{},
	}
	err = v.HandleMessage(&simplex.Message{Finalization: finalization}, nodes[0])
	require.NoError(t, err)

	require.Never(t, verified.Load, 2*time.Second, 20*time.Millisecond)
	require.Equal(t, uint64(0), v.Storage.NumBlocks())
}

func TestBlockVerifyCalledOnce(t *testing.T) {
	nodes := []simplex.NodeID{{1}, {2}, {3}, {4}}
	v := newTestNonValidator(t, nodes, nil)

	verificationDelay := make(chan struct{})
	var verifyCount atomic.Int32

	block := testutil.NewTestBlock(simplex.ProtocolMetadata{Seq: 0, Round: 0, Epoch: 0}, simplex.Blacklist{})
	block.VerificationDelay = verificationDelay
	block.OnVerify = func() {
		verifyCount.Add(1)
		// Schedule a second verification while task 1 is still inside Verify.
		// The OneTimeVerifier should return the cached result without calling Verify again.
		_ = v.verifier.triggerVerify(block)
	}

	finalization, _ := testutil.NewFinalizationRecord(t, v.Logger, &testutil.TestSignatureAggregator{}, block, nodes)
	err := v.HandleMessage(&simplex.Message{Finalization: &finalization}, nodes[0])
	require.NoError(t, err)

	err = v.HandleMessage(blockMessage(t, block, nodes[0]), nodes[0])
	require.NoError(t, err)

	// Unblock the in-progress verification.
	close(verificationDelay)

	require.Eventually(t, func() bool { return verifyCount.Load() == 1 }, 2*time.Second, 20*time.Millisecond)
	require.Never(t, func() bool { return verifyCount.Load() > 1 }, 200*time.Millisecond, 20*time.Millisecond)
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
		blockSeq          uint64
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
			// seq 1 arrives with a finalization but seq 0 has not been verified yet,
			// so the block is indexed but verification is deferred.
			name:              "Finalization Received But Not Next To Verify",
			finalizationBlock: func(_, blockToSend *testutil.TestBlock) *testutil.TestBlock { return blockToSend },
			blockSender:       nodes[0],
			blockSeq:          1,
			expectVerified:    false,
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
				Seq:   tt.blockSeq,
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

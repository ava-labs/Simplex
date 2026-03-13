package nonvalidator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

func blockWithSeq(seq uint64) *testutil.TestBlock {
	return testutil.NewTestBlock(simplex.ProtocolMetadata{Seq: seq}, simplex.Blacklist{})
}

func TestAlreadyVerifiedSeq(t *testing.T) {
	logger := testutil.MakeLogger(t, 0)

	tests := []struct {
		name     string
		verifier *Verifiier
		seq      uint64
		expected bool
	}{
		{
			name: "No Verified Blocks",
			verifier: func() *Verifiier {
				storage := testutil.NewNonValidatorInMemoryStorage()
				return NewVerifier(logger, nil, storage)
			}(),
			seq:      5,
			expected: false,
		},
		{
			name: "Already verified",
			verifier: func() *Verifiier {
				storage := testutil.NewNonValidatorInMemoryStorage()
				return NewVerifier(logger, blockWithSeq(5), storage)
			}(),
			seq:      3,
			expected: true,
		},
		{
			name: "Not verified",
			verifier: func() *Verifiier {
				storage := testutil.NewNonValidatorInMemoryStorage()
				return NewVerifier(logger, blockWithSeq(5), storage)
			}(),
			seq:      6,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.verifier.alreadyVerifiedSeq(tt.seq)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestTriggerVerify(t *testing.T) {
	logger := testutil.MakeLogger(t, 0)

	tests := []struct {
		name                      string
		verifier                  *Verifiier
		block                     simplex.Block
		expectedErr               error
		expectedLatestVerifiedSeq uint64
	}{
		{
			name: "nothing to verify",
			verifier: func() *Verifiier {
				s := testutil.NewNonValidatorInMemoryStorage()
				return NewVerifier(logger, blockWithSeq(5), s)
			}(),
			block:                     blockWithSeq(9),
			expectedLatestVerifiedSeq: 5,
		},
		{
			name: "block is next to verify",
			verifier: func() *Verifiier {
				s := testutil.NewNonValidatorInMemoryStorage()
				return NewVerifier(logger, blockWithSeq(5), s)
			}(),
			block:                     blockWithSeq(6),
			expectedLatestVerifiedSeq: 6,
		},
		{
			name: "other block can be verified",
			verifier: func() *Verifiier {
				s := testutil.NewNonValidatorInMemoryStorage()
				require.NoError(t, s.Index(context.Background(), blockWithSeq(6), simplex.Finalization{}))
				return NewVerifier(logger, blockWithSeq(5), s)
			}(),
			block:                     blockWithSeq(9),
			expectedLatestVerifiedSeq: 6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.verifier.triggerVerify(tt.block)
			require.ErrorIs(t, err, tt.expectedErr)

			require.Eventually(t, func() bool {
				lv := tt.verifier.latestVerifiedBlock
				if lv == nil {
					return false
				}
				return lv.BlockHeader().Seq == tt.expectedLatestVerifiedSeq
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
}

func TestTriggerVerifyWhileVerifying(t *testing.T) {
	logger := testutil.MakeLogger(t, 0)

	storage := testutil.NewNonValidatorInMemoryStorage()
	block7 := blockWithSeq(7)
	block6 := blockWithSeq(6)
	block6.OnVerify = func() {
		storage.Index(context.Background(), block7, simplex.Finalization{})
	}

	v := NewVerifier(logger, blockWithSeq(5), storage)
	require.NoError(t, v.triggerVerify(block6))

	require.Eventually(t, func() bool {
		lv := v.latestVerifiedBlock
		return lv != nil && lv.BlockHeader().Seq == 7
	}, 5*time.Second, 10*time.Millisecond)
}

func TestTriggerVerifyDBError(t *testing.T) {
	logger := testutil.MakeLogger(t, 0)

	dbErr := errors.New("db error")
	storage := testutil.NewNonValidatorInMemoryStorage()
	storage.RetrieveF = func(_ uint64) (simplex.FullBlock, simplex.Finalization, error) {
		return nil, simplex.Finalization{}, dbErr
	}

	v := NewVerifier(logger, blockWithSeq(5), storage)
	require.ErrorIs(t, v.triggerVerify(blockWithSeq(9)), dbErr)
	require.Equal(t, uint64(5), v.latestVerifiedBlock.BlockHeader().Seq)
}

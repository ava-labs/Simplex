// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"testing"

	"github.com/ava-labs/simplex/common"
	"github.com/ava-labs/simplex/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

func newSealingQuorumRound(epoch uint64, numValidators int) *common.QuorumRound {
	validatorSet := make(common.Nodes, numValidators)
	for i := range validatorSet {
		validatorSet[i] = common.Node{Id: common.NodeID{byte(i + 1)}, Weight: 1}
	}

	return &common.QuorumRound{
		Block: newSealingTestBlock(1, epoch, common.Digest{}, &common.SealingBlockInfo{
			Epoch:        epoch,
			ValidatorSet: validatorSet,
		}),
	}
}

type testValidatorSetRetriever struct {
	nodes common.Nodes
}

func (v *testValidatorSetRetriever) Nodes() common.Nodes {
	return v.nodes
}

// TestCollectedQuorumRound feeds an epochReplicator a sealing-block quorum round
// for an unknown epoch and asserts collectedQuorumRound only confirms the epoch
// once a threshold of distinct validators have voted for the same digest.
func TestCollectedQuorumRound(t *testing.T) {
	tests := []struct {
		name string
		qr   *common.QuorumRound
	}{
		{
			name: "4 validator nodes",
			qr:   newSealingQuorumRound(1, 4),
		},
		{
			name: "16 validator nodes",
			qr:   newSealingQuorumRound(1, 16),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			voters := tt.qr.Block.SealingBlockInfo().ValidatorSet
			// votes required to confirm the epoch.
			threshold := simplex.F(len(voters)) + 1
			require.GreaterOrEqual(t, len(voters), threshold, "need at least threshold validators to vote with")
			e := newEpochReplicator(testutil.MakeLogger(t, 1), &testValidatorSetRetriever{
				nodes: voters,
			})

			// Each distinct vote below the threshold leaves the epoch unconfirmed.
			for i := 0; i < threshold-1; i++ {
				require.False(t, e.collectedQuorumRound(tt.qr, voters[i].Id))
			}

			// The threshold-th distinct vote for the same digest confirms it.
			require.True(t, e.collectedQuorumRound(tt.qr, voters[threshold-1].Id))
		})
	}
}

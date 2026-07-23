// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package common_test

import (
	"testing"

	"github.com/ava-labs/simplex/common"
	"github.com/ava-labs/simplex/testutil"

	"github.com/stretchr/testify/require"
)

func TestIsReplicationMessage(t *testing.T) {
	tests := []struct {
		name     string
		msg      common.Message
		expected bool
	}{
		{
			name:     "empty message",
			msg:      common.Message{},
			expected: false,
		},
		{
			name:     "replication request",
			msg:      common.Message{ReplicationRequest: &common.ReplicationRequest{}},
			expected: true,
		},
		{
			name:     "replication response",
			msg:      common.Message{ReplicationResponse: &common.ReplicationResponse{}},
			expected: true,
		},
		{
			name:     "verified replication response",
			msg:      common.Message{VerifiedReplicationResponse: &common.VerifiedReplicationResponse{}},
			expected: true,
		},
		{
			name:     "block message",
			msg:      common.Message{BlockMessage: &common.BlockMessage{}},
			expected: false,
		},
		{
			name:     "verified block message",
			msg:      common.Message{VerifiedBlockMessage: &common.VerifiedBlockMessage{}},
			expected: false,
		},
		{
			name:     "empty notarization",
			msg:      common.Message{EmptyNotarization: &common.EmptyNotarization{}},
			expected: false,
		},
		{
			name:     "vote",
			msg:      common.Message{VoteMessage: &common.Vote{}},
			expected: false,
		},
		{
			name:     "empty vote",
			msg:      common.Message{EmptyVoteMessage: &common.EmptyVote{}},
			expected: false,
		},
		{
			name:     "notarization",
			msg:      common.Message{Notarization: &common.Notarization{}},
			expected: false,
		},
		{
			name:     "finalize vote",
			msg:      common.Message{FinalizeVote: &common.FinalizeVote{}},
			expected: false,
		},
		{
			name:     "finalization",
			msg:      common.Message{Finalization: &common.Finalization{}},
			expected: false,
		},
		{
			// A block digest request is a request but not a replication message.
			name:     "block digest request",
			msg:      common.Message{BlockDigestRequest: &common.BlockDigestRequest{}},
			expected: true,
		},
		{
			// When several fields are set, any replication field makes it a replication message.
			name: "block message and replication request",
			msg: common.Message{
				BlockMessage:       &common.BlockMessage{},
				ReplicationRequest: &common.ReplicationRequest{},
			},
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.msg.IsReplicationMessage())
		})
	}
}

func TestQuorumRoundMalformed(t *testing.T) {
	tests := []struct {
		name        string
		qr          common.QuorumRound
		expectedErr bool
	}{
		{
			name: "empty notarization",
			qr: common.QuorumRound{
				EmptyNotarization: &common.EmptyNotarization{},
			},
			expectedErr: false,
		}, {
			name: "all nil",
			qr: common.QuorumRound{
				EmptyNotarization: nil,
				Block:             nil,
				Notarization:      nil,
				Finalization:      nil,
			},
			expectedErr: true,
		}, {
			name: "block and notarization",
			qr: common.QuorumRound{
				Block:        &testutil.TestBlock{},
				Notarization: &common.Notarization{},
			},
			expectedErr: false,
		}, {
			name: "block and finalization",
			qr: common.QuorumRound{
				Block:        &testutil.TestBlock{},
				Finalization: &common.Finalization{},
			},
			expectedErr: false,
		}, {
			name: "block and empty notarization",
			qr: common.QuorumRound{
				Block:             &testutil.TestBlock{},
				EmptyNotarization: &common.EmptyNotarization{},
			},
			expectedErr: true,
		},
		{
			name: "block and notarization and finalization",
			qr: common.QuorumRound{
				Block:        &testutil.TestBlock{},
				Notarization: &common.Notarization{},
				Finalization: &common.Finalization{},
			},
			expectedErr: false,
		},
		{
			name: "notarization and no block",
			qr: common.QuorumRound{
				Notarization: &common.Notarization{},
			},
			expectedErr: true,
		},
		{
			name: "finalization and no block",
			qr: common.QuorumRound{
				Finalization: &common.Finalization{},
			},
			expectedErr: true,
		},
		{
			name: "just block",
			qr: common.QuorumRound{
				Block: &testutil.TestBlock{},
			},
			expectedErr: true,
		},
		{
			name: "block and notarization and empty notarization",
			qr: common.QuorumRound{
				Block:             &testutil.TestBlock{},
				Notarization:      &common.Notarization{},
				EmptyNotarization: &common.EmptyNotarization{},
			},
			expectedErr: false,
		},
		{
			name: "block and finalization and empty notarization",
			qr: common.QuorumRound{
				Block:             &testutil.TestBlock{},
				Finalization:      &common.Finalization{},
				EmptyNotarization: &common.EmptyNotarization{},
			},
			expectedErr: false,
		},
		{
			name: "empty notarization and finalization, no block",
			qr: common.QuorumRound{
				EmptyNotarization: &common.EmptyNotarization{},
				Finalization:      &common.Finalization{},
			},
			expectedErr: true,
		},
		{
			name: "empty notarization and notarization, no block",
			qr: common.QuorumRound{
				EmptyNotarization: &common.EmptyNotarization{},
				Notarization:      &common.Notarization{},
			},
			expectedErr: true,
		},
		{
			name: "empty notarization and notarization and finalization, no block",
			qr: common.QuorumRound{
				EmptyNotarization: &common.EmptyNotarization{},
				Notarization:      &common.Notarization{},
				Finalization:      &common.Finalization{},
			},
			expectedErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.qr.IsWellFormed()
			if err != nil {
				require.True(t, test.expectedErr)
				return
			}
			require.False(t, test.expectedErr)
		})
	}

}

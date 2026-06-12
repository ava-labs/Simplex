package common_test

import (
	"testing"

	"github.com/ava-labs/simplex/common"
	"github.com/ava-labs/simplex/testutil"

	"github.com/stretchr/testify/require"
)

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

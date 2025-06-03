package simplex_test

import (
	"simplex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuorumRoundMalformed(t *testing.T) {
	tests := []struct {
		name        string
		qr          simplex.QuorumRound
		expectedErr bool
	}{
		{
			name: "empty notarization",
			qr: simplex.QuorumRound{
				EmptyNotarization: &simplex.EmptyNotarization{},
			},
			expectedErr: false,
		}, {
			name: "all nil",
			qr: simplex.QuorumRound{
				EmptyNotarization: nil,
				Block:             nil,
				Notarization:      nil,
				Finalization:      nil,
			},
			expectedErr: true,
		}, {
			name: "block and notarization",
			qr: simplex.QuorumRound{
				Block:        &testBlock{},
				Notarization: &simplex.Notarization{},
			},
			expectedErr: false,
		}, {
			name: "block and finalization",
			qr: simplex.QuorumRound{
				Block:        &testBlock{},
				Finalization: &simplex.Finalization{},
			},
			expectedErr: false,
		}, {
			name: "block and empty notarization",
			qr: simplex.QuorumRound{
				Block:             &testBlock{},
				EmptyNotarization: &simplex.EmptyNotarization{},
			},
			expectedErr: true,
		},
		{
			name: "block and notarization and finalization",
			qr: simplex.QuorumRound{
				Block:        &testBlock{},
				Notarization: &simplex.Notarization{},
				Finalization: &simplex.Finalization{},
			},
			expectedErr: false,
		},
		{
			name: "notarization and no block",
			qr: simplex.QuorumRound{
				Notarization: &simplex.Notarization{},
			},
			expectedErr: true,
		},
		{
			name: "finalization and no block",
			qr: simplex.QuorumRound{
				Finalization: &simplex.Finalization{},
			},
			expectedErr: true,
		},
		{
			name: "just block",
			qr: simplex.QuorumRound{
				Block: &testBlock{},
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

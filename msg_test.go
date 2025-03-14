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
				FCert:             nil,
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
			name: "block and fcert",
			qr: simplex.QuorumRound{
				Block: &testBlock{},
				FCert: &simplex.FinalizationCertificate{},
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
			name: "block and notarization and fcert",
			qr: simplex.QuorumRound{
				Block:        &testBlock{},
				Notarization: &simplex.Notarization{},
				FCert:        &simplex.FinalizationCertificate{},
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
			name: "fcert and no block",
			qr: simplex.QuorumRound{
				FCert: &simplex.FinalizationCertificate{},
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

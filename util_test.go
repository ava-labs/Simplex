// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"errors"
	"simplex"
	. "simplex"
	"simplex/testutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetrieveFromStorage(t *testing.T) {
	brokenStorage := newInMemStorage()
	brokenStorage.data[41] = struct {
		Block
		FinalizationCertificate
	}{Block: newTestBlock(ProtocolMetadata{Seq: 41})}

	block := newTestBlock(ProtocolMetadata{Seq: 0})
	fCert := FinalizationCertificate{
		Finalization: ToBeSignedFinalization{
			BlockHeader: block.BlockHeader(),
		},
	}
	normalStorage := newInMemStorage()
	normalStorage.data[0] = struct {
		Block
		FinalizationCertificate
	}{Block: block, FinalizationCertificate: fCert}

	for _, testCase := range []struct {
		description   string
		storage       Storage
		expectedErr   error
		expectedBlock Block
		expectedFCert *FinalizationCertificate
	}{
		{
			description: "no blocks in storage",
			storage:     newInMemStorage(),
		},
		{
			description: "broken storage",
			storage:     brokenStorage,
			expectedErr: errors.New("failed retrieving last block from storage with seq 0"),
		},
		{
			description:   "normal storage",
			storage:       normalStorage,
			expectedBlock: block,
			expectedFCert: &fCert,
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			block, fCert, err := RetrieveLastIndexFromStorage(testCase.storage)
			require.Equal(t, testCase.expectedErr, err)
			require.Equal(t, testCase.expectedBlock, block)
			require.Equal(t, testCase.expectedFCert, fCert)
		})
	}
}

func TestFinalizationCertificateValidation(t *testing.T) {
	l := testutil.MakeLogger(t, 0)
	nodes := []NodeID{{1}, {2}, {3}, {4}, {5}}
	quorumSize := Quorum(len(nodes))
	signatureAggregator := &testSignatureAggregator{}
	// Test
	tests := []struct {
		name       string
		fCert      FinalizationCertificate
		quorumSize int
		valid      bool
	}{
		{
			name: "valid finalization certificate",
			fCert: func() FinalizationCertificate {
				block := newTestBlock(ProtocolMetadata{})
				fCert, _ := newFinalizationRecord(t, l, signatureAggregator, block, nodes[:quorumSize])
				return fCert
			}(),
			quorumSize: quorumSize,
			valid:      true,
		}, {
			name: "not enough signers",
			fCert: func() FinalizationCertificate {
				block := newTestBlock(ProtocolMetadata{})
				fCert, _ := newFinalizationRecord(t, l, signatureAggregator, block, nodes[:quorumSize-1])
				return fCert
			}(),
			quorumSize: quorumSize,
			valid:      false,
		},
		{
			name: "signer signed twice",
			fCert: func() FinalizationCertificate {
				block := newTestBlock(ProtocolMetadata{})
				doubleNodes := []NodeID{{1}, {2}, {3}, {4}, {4}}
				fCert, _ := newFinalizationRecord(t, l, signatureAggregator, block, doubleNodes)
				return fCert
			}(),
			quorumSize: quorumSize,
			valid:      false,
		},
		{
			name:       "quorum certificate not in finalization certificate",
			fCert:      FinalizationCertificate{Finalization: ToBeSignedFinalization{}},
			quorumSize: quorumSize,
			valid:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid, err := simplex.IsFinalizationCertificateValid(&tt.fCert, tt.quorumSize, l)
			require.NoError(t, err)
			require.Equal(t, tt.valid, valid)
		})
	}
}

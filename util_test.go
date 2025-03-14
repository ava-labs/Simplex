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
		VerifiedBlock
		FinalizationCertificate
	}{VerifiedBlock: newTestBlock(ProtocolMetadata{Seq: 41})}

	block := newTestBlock(ProtocolMetadata{Seq: 0})
	fCert := FinalizationCertificate{
		Finalization: ToBeSignedFinalization{
			BlockHeader: block.BlockHeader(),
		},
	}
	normalStorage := newInMemStorage()
	normalStorage.data[0] = struct {
		VerifiedBlock
		FinalizationCertificate
	}{VerifiedBlock: block, FinalizationCertificate: fCert}

	for _, testCase := range []struct {
		description           string
		storage               Storage
		expectedErr           error
		expectedVerifiedBlock *VerifiedFinalizedBlock
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
			description: "normal storage",
			storage:     normalStorage,
			expectedVerifiedBlock: &VerifiedFinalizedBlock{
				VerifiedBlock: block,
				FCert:         fCert,
			},
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			lastBlock, err := RetrieveLastIndexFromStorage(testCase.storage)
			require.Equal(t, testCase.expectedErr, err)

			require.Equal(t, testCase.expectedVerifiedBlock, lastBlock)
		})
	}
}

func TestFinalizationCertificateValidation(t *testing.T) {
	l := testutil.MakeLogger(t, 0)
	nodes := []NodeID{{1}, {2}, {3}, {4}, {5}}
	eligibleSigners := make(map[string]struct{})
	for _, n := range nodes {
		eligibleSigners[string(n)] = struct{}{}
	}
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
		{
			name: "nodes are not eligible signers",
			fCert: func() FinalizationCertificate {
				block := newTestBlock(ProtocolMetadata{})
				signers := []NodeID{{1}, {2}, {3}, {4}, {6}}
				fCert, _ := newFinalizationRecord(t, l, signatureAggregator, block, signers)
				return fCert
			}(), quorumSize: quorumSize,
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := simplex.IsFinalizationCertificateValid(eligibleSigners, &tt.fCert, tt.quorumSize, l)
			require.Equal(t, tt.valid, valid)
		})
	}
}

func TestGetHighestQuorumRound(t *testing.T) {
	// Test
	nodes := []NodeID{{1}, {2}, {3}, {4}, {5}}
	l := testutil.MakeLogger(t, 0)
	signatureAggregator := &testSignatureAggregator{}

	// seq 1
	block1 := newTestBlock(ProtocolMetadata{
		Seq:   1,
		Round: 1,
	})
	notarization1, err := newNotarization(l, signatureAggregator, block1, nodes)
	require.NoError(t, err)
	fCert1, _ := newFinalizationRecord(t, l, signatureAggregator, block1, nodes)

	// seq 10
	block10 := newTestBlock(ProtocolMetadata{Seq: 10, Round: 10})
	notarization10, err := newNotarization(l, signatureAggregator, block10, nodes)
	require.NoError(t, err)
	fCert10, _ := newFinalizationRecord(t, l, signatureAggregator, block10, nodes)

	tests := []struct {
		name       string
		round      *Round
		eNote      *EmptyNotarization
		lastBlock  *VerifiedFinalizedBlock
		expectedQr *VerifiedQuorumRound
	}{
		{
			name:  "only mpty notarization",
			eNote: newEmptyNotarization(nodes, 1, 1),
			expectedQr: &VerifiedQuorumRound{
				EmptyNotarization: newEmptyNotarization(nodes, 1, 1),
			},
		},
		{
			name: "only last block",
			lastBlock: &VerifiedFinalizedBlock{
				VerifiedBlock: block1,
				FCert:         fCert1,
			},
			expectedQr: &VerifiedQuorumRound{
				VerifiedBlock: block1,
				FCert:         &fCert1,
			},
		},
		{
			name:  "round",
			round: SetRound(block1, nil, &fCert1),
			expectedQr: &VerifiedQuorumRound{
				VerifiedBlock: block1,
				FCert:         &fCert1,
			},
		},
		{
			name:  "round with notarization",
			round: SetRound(block1, &notarization1, nil),
			expectedQr: &VerifiedQuorumRound{
				VerifiedBlock: block1,
				Notarization:  &notarization1,
			},
		},
		{
			name:  "higher notarized round than indexed",
			round: SetRound(block10, &notarization10, nil),
			lastBlock: &VerifiedFinalizedBlock{
				VerifiedBlock: block1,
				FCert:         fCert1,
			},
			expectedQr: &VerifiedQuorumRound{
				VerifiedBlock: block10,
				Notarization:  &notarization10,
			},
		},
		{
			name:  "higher indexed than in round",
			round: SetRound(block1, &notarization1, nil),
			lastBlock: &VerifiedFinalizedBlock{
				VerifiedBlock: block10,
				FCert:         fCert10,
			},
			expectedQr: &VerifiedQuorumRound{
				VerifiedBlock: block10,
				FCert:         &fCert10,
			},
		},
		{
			name:  "higher empty notarization",
			eNote: newEmptyNotarization(nodes, 100, 100),
			lastBlock: &VerifiedFinalizedBlock{
				VerifiedBlock: block1,
				FCert:         fCert1,
			},
			round: SetRound(block10, &notarization10, nil),
			expectedQr: &VerifiedQuorumRound{
				EmptyNotarization: newEmptyNotarization(nodes, 100, 100),
			},
		},
		{
			name:  "higher empty notarization with same sequence",
			eNote: newEmptyNotarization(nodes, 11, 10),
			lastBlock: &VerifiedFinalizedBlock{
				VerifiedBlock: block10,
				FCert:         fCert10,
			},
			expectedQr: &VerifiedQuorumRound{
				EmptyNotarization: newEmptyNotarization(nodes, 11, 10),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qr := simplex.GetLatestVerifiedQuorumRound(tt.round, tt.eNote, tt.lastBlock)
			require.Equal(t, tt.expectedQr, qr)
		})
	}
}

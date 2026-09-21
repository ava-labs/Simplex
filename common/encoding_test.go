// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package common

import (
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuorumRecord(t *testing.T) {
	var qr QuorumRecord
	qr.QC = []byte{1, 2, 3, 4, 5}
	qr.Vote = []byte{6, 7, 8, 9, 10}

	bytes := qr.Bytes()

	var qr2 QuorumRecord
	err := qr2.FromBytes(bytes)
	require.NoError(t, err)

	require.Equal(t, qr.QC, qr2.QC)
	require.Equal(t, qr.Vote, qr2.Vote)
}

func TestQuorumRecordFromBytes(t *testing.T) {
	for _, tc := range []struct {
		name          string
		input         []byte
		expectedError string
	}{
		{
			name:          "too short",
			input:         []byte{1, 2, 3},
			expectedError: "buffer too small to contain vote length",
		},
		{
			name:          "buffer too small",
			input:         []byte{0, 0, 0, 10, 1, 2, 3},
			expectedError: "buffer too small to contain vote",
		},
		{
			name:          "vote length exceeds buffer size",
			input:         []byte{0xff, 0xff, 0xff, 0xff},
			expectedError: "vote length exceeds buffer size",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var qr QuorumRecord
			err := qr.FromBytes(tc.input)
			if tc.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.expectedError)
			}
		})
	}
}

func TestBlockRecord(t *testing.T) {
	bh := BlockHeader{
		ProtocolMetadata: ProtocolMetadata{
			Version: 1,
			Round:   666,
			Seq:     3,
			Epoch:   4,
		},
	}

	_, err := rand.Read(bh.Prev[:])
	require.NoError(t, err)

	_, err = rand.Read(bh.Digest[:])
	require.NoError(t, err)

	payload := []byte{11, 12, 13, 14, 15, 16}
	record, err := BlockRecord(bh, payload)
	require.NoError(t, err)

	recordRound, err := BlockRecordRound(record)
	require.NoError(t, err)
	require.Equal(t, uint64(666), recordRound)

	md2, payload2, err := ParseBlockRecord(record)
	require.NoError(t, err)

	require.True(t, bh.Equals(&md2))
	require.Equal(t, payload, payload2)
}

func FuzzBlockRecord(f *testing.F) {
	f.Fuzz(func(t *testing.T, version uint8, round, seq, epoch uint64, prevPreimage, digestPreimage []byte, payload []byte) {
		prev := sha256.Sum256(prevPreimage)
		digest := sha256.Sum256(digestPreimage)
		bh := BlockHeader{
			ProtocolMetadata: ProtocolMetadata{
				Version: version,
				Round:   round,
				Seq:     seq,
				Epoch:   epoch,
				Prev:    prev,
			},
			Digest: digest,
		}
		record, err := BlockRecord(bh, payload)
		require.NoError(t, err)
		recordRound, err := BlockRecordRound(record)
		require.NoError(t, err)
		require.Equal(t, round, recordRound)

		md2, payload2, err := ParseBlockRecord(record)
		require.NoError(t, err)

		require.Equal(t, bh, md2)
		require.Equal(t, payload, payload2)
	})
}

func TestNotarizationRecord(t *testing.T) {
	sig := make([]byte, 64)
	_, err := rand.Read(sig)
	require.NoError(t, err)

	vote := ToBeSignedVote{
		BlockHeader{
			ProtocolMetadata: ProtocolMetadata{
				Version: 1,
				Round:   666,
				Seq:     3,
				Epoch:   4,
			},
		},
	}

	_, err = rand.Read(vote.Prev[:])
	require.NoError(t, err)

	_, err = rand.Read(vote.Prev[:])
	require.NoError(t, err)

	record := NewQuorumRecord([]byte{1, 2, 3}, vote.Bytes(), NotarizationRecordType)
	recordRound, err := toBeSignedVoteQuorumRecordRound(record)
	require.NoError(t, err)
	require.Equal(t, uint64(666), recordRound)

	qc, vote2, err := ParseNotarizationRecord(record)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, qc)
	require.True(t, vote.Equals(&vote2.BlockHeader))
}

func FuzzNotarizationRecord(f *testing.F) {
	f.Fuzz(func(t *testing.T, version uint8, round uint64, seq uint64, epoch uint64, prevPreimage, digestPreimage []byte, sig []byte, signer1, signer2 []byte) {
		prev := sha256.Sum256(prevPreimage)
		digest := sha256.Sum256(digestPreimage)

		vote := ToBeSignedVote{
			BlockHeader{
				ProtocolMetadata: ProtocolMetadata{
					Version: version,
					Round:   round,
					Seq:     seq,
					Epoch:   epoch,
					Prev:    prev,
				},
				Digest: digest,
			},
		}

		record := NewQuorumRecord([]byte{1, 2, 3}, vote.Bytes(), NotarizationRecordType)
		recordRound, err := toBeSignedVoteQuorumRecordRound(record)
		require.NoError(t, err)
		require.Equal(t, round, recordRound)

		qc, vote2, err := ParseNotarizationRecord(record)
		require.NoError(t, err)
		require.Equal(t, []byte{1, 2, 3}, qc)
		require.True(t, vote.Equals(&vote2.BlockHeader))
	})
}

func TestEmptyVote(t *testing.T) {
	record := NewEmptyVoteRecord(ToBeSignedEmptyVote{
		EmptyVoteMetadata{
			Round: 666,
			Epoch: 4,
		},
	})

	recordRound, err := EmptyVoteRecordRound(record)
	require.NoError(t, err)
	require.Equal(t, uint64(666), recordRound)

	_, err = EmptyVoteRecordRound(record[2:])
	require.ErrorContains(t, err, "record too short to extract round")
}

func TestRecordRound(t *testing.T) {
	emptyVoteRecord := NewEmptyVoteRecord(ToBeSignedEmptyVote{
		EmptyVoteMetadata{
			Round: 669,
			Epoch: 4,
		},
	})

	vote := ToBeSignedVote{
		BlockHeader{
			ProtocolMetadata: ProtocolMetadata{
				Version: 1,
				Round:   666,
				Seq:     3,
				Epoch:   4,
			},
		},
	}

	en := &EmptyNotarization{
		Vote: ToBeSignedEmptyVote{EmptyVoteMetadata: EmptyVoteMetadata{
			Round: 667,
		}},
	}
	emptyNotarizationRecord := NewQuorumRecord([]byte{1, 2, 3}, en.Vote.Bytes(), EmptyNotarizationRecordType)
	notarizationRecord := NewQuorumRecord([]byte{1, 2, 3}, vote.Bytes(), NotarizationRecordType)
	blockRecord, err := BlockRecord(BlockHeader{
		ProtocolMetadata: ProtocolMetadata{
			Version: 1,
			Round:   668,
			Seq:     3,
			Epoch:   4,
		},
	}, []byte{11, 12, 13, 14, 15, 16})
	require.NoError(t, err)

	finalization := ToBeSignedFinalization{
		BlockHeader{
			ProtocolMetadata: ProtocolMetadata{
				Version: 1,
				Round:   670,
				Seq:     3,
				Epoch:   4,
			},
		},
	}
	finalizationRecord := NewQuorumRecord([]byte{1, 2, 3}, finalization.Bytes(), FinalizationRecordType)

	// A record whose type is not any of the known WAL record types.
	unknownRecord := NewQuorumRecord([]byte{1, 2, 3}, en.Vote.Bytes(), 9999)

	for _, tc := range []struct {
		name          string
		record        []byte
		expectedRound uint64
		expectedError string
	}{
		{
			name:          "not enough bytes",
			record:        []byte{1},
			expectedError: "entry too short to extract record",
		},
		{
			name:          "wrong record type",
			record:        unknownRecord,
			expectedError: "unknown record type",
		},
		{
			name:          "FinalizationRecord",
			record:        finalizationRecord,
			expectedRound: 670,
		},
		{
			name:          "EmptyVoteRecord",
			record:        emptyVoteRecord,
			expectedRound: 669,
		},
		{
			name:          "NotarizationRecord",
			record:        notarizationRecord,
			expectedRound: 666,
		},
		{
			name:          "EmptyNotarizationRecord",
			record:        emptyNotarizationRecord,
			expectedRound: 667,
		},
		{
			name:          "BlockRecord",
			record:        blockRecord,
			expectedRound: 668,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recordRound, err := RecordRound(tc.record)
			if tc.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expectedRound, recordRound)
			} else {
				require.ErrorContains(t, err, tc.expectedError)
			}
		})
	}
}

// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockRecord(t *testing.T) {
	bh := BlockHeader{
		ProtocolMetadata: ProtocolMetadata{
			Round: 2,
			Seq:   3,
			Epoch: 4,
		},
	}

	_, err := rand.Read(bh.Prev[:])
	require.NoError(t, err)

	_, err = rand.Read(bh.Digest[:])
	require.NoError(t, err)

	payload := []byte{11, 12, 13, 14, 15, 16}

	record := Record{
		Block: &BlockRecord{
			Header:  bh,
			Payload: payload,
		},
	}
	recordBytes := record.MarshalCanoto()

	var parsedRecord Record
	require.NoError(t, parsedRecord.UnmarshalCanoto(recordBytes))
	require.True(t, bh.Equals(&parsedRecord.Block.Header))
	require.Equal(t, payload, parsedRecord.Block.Payload)
}

func FuzzBlockRecord(f *testing.F) {
	f.Fuzz(func(t *testing.T, version uint8, round, seq, epoch uint64, prevPreimage, digestPreimage []byte, payload []byte) {
		prev := sha256.Sum256(prevPreimage)
		digest := sha256.Sum256(digestPreimage)
		bh := BlockHeader{
			ProtocolMetadata: ProtocolMetadata{
				Round: round,
				Seq:   seq,
				Epoch: epoch,
				Prev:  prev,
			},
			Digest: digest,
		}

		record := Record{
			Block: &BlockRecord{
				Header:  bh,
				Payload: payload,
			},
		}
		recordBytes := record.MarshalCanoto()

		var parsedRecord Record
		require.NoError(t, parsedRecord.UnmarshalCanoto(recordBytes))
		require.True(t, bh.Equals(&parsedRecord.Block.Header))
		require.Equal(t, payload, parsedRecord.Block.Payload)
	})
}

func TestNotarizationRecord(t *testing.T) {
	sig := make([]byte, 64)
	_, err := rand.Read(sig)
	require.NoError(t, err)

	vote := ToBeSignedVote{
		BlockHeader{
			ProtocolMetadata: ProtocolMetadata{
				Round: 2,
				Seq:   3,
				Epoch: 4,
			},
		},
	}

	_, err = rand.Read(vote.Prev[:])
	require.NoError(t, err)

	_, err = rand.Read(vote.Digest[:])
	require.NoError(t, err)

	record := Record{
		Notarization: &QuorumRecord{
			Header: vote.BlockHeader,
			QC:     []byte{1, 2, 3},
		},
	}
	recordBytes := record.MarshalCanoto()

	var parsedRecord Record
	require.NoError(t, parsedRecord.UnmarshalCanoto(recordBytes))
	require.True(t, vote.BlockHeader.Equals(&parsedRecord.Notarization.Header))
	require.Equal(t, []byte{1, 2, 3}, parsedRecord.Notarization.QC)
}

func FuzzNotarizationRecord(f *testing.F) {
	f.Fuzz(func(t *testing.T, round uint64, seq uint64, epoch uint64, prevPreimage, digestPreimage []byte, sig []byte, signer1, signer2 []byte) {
		prev := sha256.Sum256(prevPreimage)
		digest := sha256.Sum256(digestPreimage)

		vote := ToBeSignedVote{
			BlockHeader{
				ProtocolMetadata: ProtocolMetadata{
					Round: round,
					Seq:   seq,
					Epoch: epoch,
					Prev:  prev,
				},
				Digest: digest,
			},
		}

		record := Record{
			Notarization: &QuorumRecord{
				Header: vote.BlockHeader,
				QC:     []byte{1, 2, 3},
			},
		}
		recordBytes := record.MarshalCanoto()

		var parsedRecord Record
		require.NoError(t, parsedRecord.UnmarshalCanoto(recordBytes))
		require.True(t, vote.BlockHeader.Equals(&parsedRecord.Notarization.Header))
		require.Equal(t, []byte{1, 2, 3}, parsedRecord.Notarization.QC)
	})
}

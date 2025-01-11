// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNotarizationRecord(t *testing.T) {
	sig := make([]byte, 64)
	_, err := rand.Read(sig)
	require.NoError(t, err)

	vote := BlockHeader{
		ProtocolMetadata: ProtocolMetadata{
			Round: 2,
			Seq:   3,
			Epoch: 4,
		},
	}

	_, err = rand.Read(vote.Prev[:])
	require.NoError(t, err)

	_, err = rand.Read(vote.Digest[:])
	require.NoError(t, err)

	record := Record{
		Notarization: &Quorum{
			Header:      vote,
			Certificate: []byte{1, 2, 3},
		},
	}
	recordBytes := record.MarshalCanoto()

	var parsedRecord Record
	require.NoError(t, parsedRecord.UnmarshalCanoto(recordBytes))
	require.True(t, vote.Equals(&parsedRecord.Notarization.Header))
	require.Equal(t, []byte{1, 2, 3}, parsedRecord.Notarization.Certificate)
}

func FuzzNotarizationRecord(f *testing.F) {
	f.Fuzz(func(t *testing.T, round uint64, seq uint64, epoch uint64, prevPreimage, digestPreimage []byte, sig []byte, signer1, signer2 []byte) {
		prev := sha256.Sum256(prevPreimage)
		digest := sha256.Sum256(digestPreimage)

		vote := BlockHeader{
			ProtocolMetadata: ProtocolMetadata{
				Round: round,
				Seq:   seq,
				Epoch: epoch,
				Prev:  prev,
			},
			Digest: digest,
		}

		record := Record{
			Notarization: &Quorum{
				Header:      vote,
				Certificate: []byte{1, 2, 3},
			},
		}
		recordBytes := record.MarshalCanoto()

		var parsedRecord Record
		require.NoError(t, parsedRecord.UnmarshalCanoto(recordBytes))
		require.True(t, vote.Equals(&parsedRecord.Notarization.Header))
		require.Equal(t, []byte{1, 2, 3}, parsedRecord.Notarization.Certificate)
	})
}

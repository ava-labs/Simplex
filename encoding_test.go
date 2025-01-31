// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"crypto/rand"
	"crypto/sha256"
	"simplex/record"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockRecord(t *testing.T) {
	bh := BlockHeader{
		ProtocolMetadata: ProtocolMetadata{
			Version: 1,
			Round:   2,
			Seq:     3,
			Epoch:   4,
		},
	}

	_, err := rand.Read(bh.Prev[:])
	require.NoError(t, err)

	_, err = rand.Read(bh.Digest[:])
	require.NoError(t, err)

	payload := []byte{11, 12, 13, 14, 15, 16}
	record := BlockRecord(bh, payload)

	md2, payload2, err := ParseBlockRecord(record)
	require.NoError(t, err)

	require.Equal(t, bh, md2)
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
		record := BlockRecord(bh, payload)

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
				Round:   2,
				Seq:     3,
				Epoch:   4,
			},
		},
	}

	_, err = rand.Read(vote.Prev[:])
	require.NoError(t, err)

	_, err = rand.Read(vote.Prev[:])
	require.NoError(t, err)

	record := NewQuorumRecord([]byte{1, 2, 3}, vote.Bytes(), record.NotarizationRecordType)
	qc, vote2, err := ParseNotarizationRecord(record)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, qc)
	require.Equal(t, vote, vote2)
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

		var signers []NodeID
		for _, signer := range [][]byte{signer1, signer2} {
			signers = append(signers, signer)
		}

		record := NewQuorumRecord([]byte{1, 2, 3}, vote.Bytes(), record.NotarizationRecordType)
		qc, vote2, err := ParseNotarizationRecord(record)
		require.NoError(t, err)
		require.Equal(t, []byte{1, 2, 3}, qc)
		require.Equal(t, vote, vote2)
	})
}

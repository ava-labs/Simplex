// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
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
	md := Metadata{
		ProtocolMetadata: ProtocolMetadata{
			Version: 1,
			Round:   2,
			Seq:     3,
			Epoch:   4,
			Prev:    make([]byte, 32),
		},
		Digest: make([]byte, 32),
	}

	_, err := rand.Read(md.Prev)
	require.NoError(t, err)

	_, err = rand.Read(md.Digest)
	require.NoError(t, err)

	payload := []byte{11, 12, 13, 14, 15, 16}

	record := BlockRecord(md, payload)

	md2, payload2, err := blockFromRecord(record)
	require.NoError(t, err)

	require.Equal(t, md, md2)
	require.Equal(t, payload, payload2)
}

func FuzzBlockRecord(f *testing.F) {
	f.Fuzz(func(t *testing.T, version uint8, round, seq, epoch uint64, prevPreimage, digestPreimage []byte, payload []byte) {
		prev := sha256.Sum256(prevPreimage)
		digest := sha256.Sum256(digestPreimage)
		md := Metadata{
			ProtocolMetadata: ProtocolMetadata{
				Version: version,
				Round:   round,
				Seq:     seq,
				Epoch:   epoch,
				Prev:    prev[:],
			},
			Digest: digest[:],
		}

		record := BlockRecord(md, payload)

		md2, payload2, err := blockFromRecord(record)
		require.NoError(t, err)

		require.Equal(t, md, md2)
		require.Equal(t, payload, payload2)
	})
}

func TestNotarizationRecord(t *testing.T) {
	sig := make([]byte, 64)
	_, err := rand.Read(sig)
	require.NoError(t, err)

	sigs := [][]byte{sig}

	vote := Vote{
		Metadata{
			ProtocolMetadata: ProtocolMetadata{
				Version: 1,
				Round:   2,
				Seq:     3,
				Epoch:   4,
				Prev:    make([]byte, 32),
			},
			Digest: make([]byte, 32),
		},
	}

	_, err = rand.Read(vote.Prev)
	require.NoError(t, err)

	_, err = rand.Read(vote.Prev)
	require.NoError(t, err)

	var signers []NodeID
	for range 4 {
		signer := make([]byte, 32)
		_, err = rand.Read(signer)
		require.NoError(t, err)

		signers = append(signers, signer)
	}

	record := NewQuorumRecord(sigs, signers, vote.Bytes(), record.NotarizationRecordType)
	notarization, err := NotarizationFromRecord(record)
	require.NoError(t, err)
	require.Equal(t, vote, notarization.Vote)
	require.Equal(t, sigs, [][]byte{notarization.AggregatedSignedVote.Signature})
	require.Equal(t, signers, notarization.AggregatedSignedVote.Signers)
}

func FuzzNotarizationRecord(f *testing.F) {
	f.Fuzz(func(t *testing.T, version uint8, round uint64, seq uint64, epoch uint64, prevPreimage, digestPreimage []byte, sig []byte, signer1, signer2 []byte) {
		prev := sha256.Sum256(prevPreimage)
		digest := sha256.Sum256(digestPreimage)

		vote := Vote{
			Metadata{
				ProtocolMetadata: ProtocolMetadata{
					Version: version,
					Round:   round,
					Seq:     seq,
					Epoch:   epoch,
					Prev:    prev[:],
				},
				Digest: digest[:],
			},
		}

		var signers []NodeID
		for _, signer := range [][]byte{signer1, signer2} {
			signers = append(signers, signer)
		}

		record := NewQuorumRecord([][]byte{sig}, signers, vote.Bytes(), record.NotarizationRecordType)
		notarization, err := NotarizationFromRecord(record)
		require.NoError(t, err)
		require.Equal(t, vote, notarization.Vote)
		require.Equal(t, [][]byte{sig}, [][]byte{notarization.AggregatedSignedVote.Signature})
		require.Equal(t, signers, notarization.AggregatedSignedVote.Signers)
	})
}

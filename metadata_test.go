// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetadata(t *testing.T) {
	var (
		prev   Digest
		digest Digest
	)

	_, err := rand.Read(prev[:])
	require.NoError(t, err)

	_, err = rand.Read(digest[:])
	require.NoError(t, err)

	bh := BlockHeader{
		ProtocolMetadata: ProtocolMetadata{
			Round: 2,
			Seq:   3,
			Epoch: 4,
			Prev:  prev,
		},
		Digest: digest,
	}

	var bh2 BlockHeader
	require.NoError(t, bh2.UnmarshalCanoto(bh.MarshalCanoto()))
	require.True(t, bh.Equals(&bh2))
}

func FuzzMetadata(f *testing.F) {
	f.Fuzz(func(t *testing.T, version uint8, round uint64, seq uint64, epoch uint64, prevPreimage []byte, digestPreimage []byte) {

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

		var bh2 BlockHeader
		require.NoError(t, bh2.UnmarshalCanoto(bh.MarshalCanoto()))
		require.True(t, bh.Equals(&bh2))
	})
}

// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"crypto/rand"
	"crypto/sha256"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMetadata(t *testing.T) {
	prev := make([]byte, metadataPrevLen)
	digest := make([]byte, metadataDigestLen)

	_, err := rand.Read(prev)
	require.NoError(t, err)

	_, err = rand.Read(digest)
	require.NoError(t, err)

	md := Metadata{
		Version: 1,
		Round:   2,
		Seq:     3,
		Epoch:   4,
		Prev:    prev,
		Digest:  digest,
	}

	var md2 Metadata
	require.NoError(t, md2.FromBytes(md.Bytes()))
	require.Equal(t, md, md2)
}

func FuzzMetadata(f *testing.F) {
	f.Fuzz(func(t *testing.T, version uint8, round uint64, seq uint64, epoch uint64, prevPreimage []byte, digestPreimage []byte) {
		t.Fail()
		prev := sha256.Sum256(prevPreimage)
		digest := sha256.Sum256(digestPreimage)

		md := Metadata{
			Version: version,
			Round:   round,
			Seq:     seq,
			Epoch:   epoch,
			Prev:    prev[:],
			Digest:  digest[:],
		}

		var md2 Metadata
		require.NoError(t, md2.FromBytes(md.Bytes()))
		require.Equal(t, md, md2)
	})
}

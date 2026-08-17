// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"testing"
	"time"

	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"

	"github.com/stretchr/testify/require"
)

// newTestParsedBlock builds a ParsedBlock with round and seq set to num.
func newTestParsedBlock(num uint64, payload string) *ParsedBlock {
	return &ParsedBlock{
		StateMachineBlock: metadata.StateMachineBlock{
			Metadata: metadata.StateMachineMetadata{
				SimplexProtocolMetadata: common.ProtocolMetadata{
					Round: num,
					Seq:   num,
				},
			},
			InnerBlock: &testInnerBlock{
				Height_: num,
				TS:      time.UnixMilli(1),
				Payload: []byte(payload),
			},
		},
	}
}

// TestCachedStorageRetrieveBySeq asserts that Retrieve serves a verified but
// not yet indexed block when the caller passes a zero digest or a digest that
// misses the cache, by matching on seq alone.
func TestCachedStorageRetrieveBySeq(t *testing.T) {
	cs := NewCachedStorage(NewMockStorage(t))
	indexedBlock := newTestParsedBlock(0, "indexed")

	require.NoError(t, cs.Index(t.Context(), indexedBlock, common.Finalization{}))

	block := newTestParsedBlock(5, "cached")
	cachedBlock := &cachedBlock{
		ParsedBlock: block,
		cache:       cs,
	}

	_, err := cachedBlock.Verify(t.Context(), common.OnlyVMVerifyOpt)
	require.NoError(t, err)

	// Seq-only lookup with a zero digest.
	got, fin, err := cs.Retrieve(5, common.Digest{})
	require.NoError(t, err)
	require.Nil(t, fin)
	require.Equal(t, cachedBlock.ParsedBlock, got)

	// Requesting a different digest for that sequence, returns error not found
	_, _, err = cs.Retrieve(5, common.Digest{1})
	require.ErrorIs(t, err, common.ErrBlockNotFound)

	// A seq that is not cached falls through to storage.
	indexed, _, err := cs.Retrieve(0, common.Digest{})
	require.NoError(t, err, common.ErrBlockNotFound)
	require.Equal(t, indexedBlock, indexed)

	// Not in storage should error
	_, _, err = cs.Retrieve(7, common.Digest{})
	require.ErrorIs(t, err, common.ErrBlockNotFound)
}

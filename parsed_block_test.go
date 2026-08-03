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

func TestParsedBlockLegacyBlock(t *testing.T) {
	inner := &testInnerBlock{Height_: 3, TS: time.Now(), Payload: []byte("pre-simplex")}
	smb := metadata.StateMachineBlock{
		InnerBlock: inner,
		Metadata: metadata.StateMachineMetadata{
			SimplexProtocolMetadata: common.ProtocolMetadata{Seq: inner.Height()},
		},
	}

	legacyBlock := &ParsedBlock{StateMachineBlock: smb, legacyBlock: true}
	simplexBlock := &ParsedBlock{StateMachineBlock: smb}

	// A block predating Simplex is its inner block, both on the wire and in its digest.
	require.Equal(t, inner.Bytes(), legacyBlock.Bytes())
	require.Equal(t, len(inner.Bytes()), legacyBlock.Size())
	require.Equal(t, common.Digest(inner.Digest()), legacyBlock.BlockHeader().Digest)

	// A Simplex block is the inner block wrapped together with its metadata, so both its
	// encoding and its digest differ from the block predating Simplex.
	require.Equal(t, common.Digest(smb.Digest()), simplexBlock.BlockHeader().Digest)
	require.NotEqual(t, legacyBlock.Bytes(), simplexBlock.Bytes())
	require.NotEqual(t, legacyBlock.BlockHeader().Digest, simplexBlock.BlockHeader().Digest)
}

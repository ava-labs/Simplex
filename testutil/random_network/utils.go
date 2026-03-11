// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package random_network

import (
	"encoding/binary"
	"math/rand"

	"github.com/ava-labs/simplex"
)

func NewProtocolMetadata(round, seq uint64, prev simplex.Digest) simplex.ProtocolMetadata {
	return simplex.ProtocolMetadata{
		Round: round,
		Seq:   seq,
		Prev:  prev,
	}
}

func GenerateNodeIDFromRand(r *rand.Rand) simplex.NodeID {
	b := make([]byte, 32)

	for i := 0; i < len(b); i += 8 {
		binary.LittleEndian.PutUint64(b[i:], r.Uint64())
	}

	return simplex.NodeID(b)
}

var emptyBlacklist = simplex.Blacklist{
	NodeCount:      4,
	SuspectedNodes: simplex.SuspectedNodes{},
	Updates:        []simplex.BlacklistUpdate{},
}

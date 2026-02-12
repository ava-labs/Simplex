// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package random_network

import "github.com/ava-labs/simplex"

func NewProtocolMetadata(round, seq uint64, prev simplex.Digest) simplex.ProtocolMetadata {
	return simplex.ProtocolMetadata{
		Round: round,
		Seq:   seq,
		Prev:  prev,
	}
}

func AdvanceRoundAndSeq(md simplex.ProtocolMetadata, digest simplex.Digest) simplex.ProtocolMetadata {
	return NewProtocolMetadata(md.Round+1, md.Seq+1, digest)
}

var emptyBlacklist = simplex.Blacklist{
	NodeCount:      4,
	SuspectedNodes: simplex.SuspectedNodes{},
	Updates:        []simplex.BlacklistUpdate{},
}

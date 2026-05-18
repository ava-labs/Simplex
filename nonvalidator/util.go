// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"fmt"

	"github.com/ava-labs/simplex"
)

type finalizedSeq struct {
	block        simplex.Block
	finalization *simplex.Finalization
}

func (f *finalizedSeq) String() string {
	seq := uint64(0)
	digest := simplex.Digest{}
	if f.block != nil {
		seq = f.block.BlockHeader().Seq
		digest = f.block.BlockHeader().Digest
	}
	if f.finalization != nil {
		seq = f.finalization.Finalization.Seq
		digest = f.finalization.Finalization.Digest
	}

	return fmt.Sprintf("FinalizedSeq {BlockDigest: %s, Seq: %d, BlockExists %t, FinalizationExists %t}", digest, seq, f.block != nil, f.finalization != nil)
}

func (f *finalizedSeq) GetSeq() uint64 {
	if f.block != nil {
		return f.block.BlockHeader().Seq
	}
	if f.finalization != nil {
		return f.finalization.Finalization.Seq
	}

	return 0
}

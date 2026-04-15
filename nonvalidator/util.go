package nonvalidator

import (
	"fmt"

	"github.com/ava-labs/simplex"
)

type finalizedSeq struct {
	block        simplex.FullBlock
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

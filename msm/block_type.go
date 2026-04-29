// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import "fmt"

// BlockType classifies a Simplex block by the role it plays in its epoch:
// a normal block in the middle of an epoch, a Telock (a pre-sealing block
// that carries no inner VM block), a sealing block that finalizes the
// current epoch, or the first block of a new epoch.
type BlockType uint8

const (
	BlockTypeNormal BlockType = iota + 1
	BlockTypeTelock
	BlockTypeSealing
	BlockTypeNewEpoch
)

func (t BlockType) String() string {
	switch t {
	case BlockTypeNormal:
		return "Normal"
	case BlockTypeTelock:
		return "Telock"
	case BlockTypeSealing:
		return "Sealing"
	case BlockTypeNewEpoch:
		return "NewEpoch"
	default:
		return fmt.Sprintf("UnknownBlockType(%d)", t)
	}
}

// IdentifyBlockType classifies a proposed block relative to its parent by
// inspecting the epoch information and the parent's sequence number.
func IdentifyBlockType(nextBlockMD StateMachineMetadata, prevBlockMD StateMachineMetadata, prevSeq uint64) BlockType {
	simplexEpochInfo := nextBlockMD.SimplexEpochInfo
	prevSimplexEpochInfo := prevBlockMD.SimplexEpochInfo

	// Only sealing blocks carry block validation descriptors
	if nextBlockMD.SimplexEpochInfo.BlockValidationDescriptor != nil {
		return BlockTypeSealing
	}

	// This block could be in the edges of an epoch, either at the end or at the beginning.

	// If the new block comes after a sealing block, it could be a Telock or the first block of the next epoch.
	// [ Sealing Block ] <-- [ New Block ]
	if prevSimplexEpochInfo.BlockValidationDescriptor != nil {
		// The zero-epoch block has BlockValidationDescriptor but epoch number 1 and next P-chain reference height of 0,
		// so the block following it is a normal block, not a Telock.
		if prevSimplexEpochInfo.EpochNumber == 1 && prevSimplexEpochInfo.NextPChainReferenceHeight == 0 {
			return BlockTypeNormal
		}

		if simplexEpochInfo.EpochNumber == prevSeq {
			// If the epoch number of the new block is the same as the previous block's sequence number,
			// it means we have just transitioned to a new epoch as the previous block was a sealing block.
			return BlockTypeNewEpoch
		}

		// Otherwise, we haven't transitioned to a new epoch yet, so this block has to be a Telock,
		// as after a sealing block we either have a Telock or the first block of the new epoch,
		// and we have already ruled out the first block of the new epoch in the previous condition.
		return BlockTypeTelock
	}

	// Else, if the previous block has a sealing block sequence and is in the same epoch as this block,
	// then this block has to be a Telock, as the sealing block sequence indicates that the sealing block has been created.
	// [ Sealing Block ] <-- [ Prev block ] <-- [ New Block ]
	if simplexEpochInfo.EpochNumber == prevSimplexEpochInfo.EpochNumber && prevSimplexEpochInfo.SealingBlockSeq != 0 {
		return BlockTypeTelock
	}

	// This block is the first block of its epoch if the epoch number is the sealing block sequence of the previous epoch
	if simplexEpochInfo.EpochNumber == prevSimplexEpochInfo.SealingBlockSeq {
		return BlockTypeNewEpoch
	}

	// Otherwise, we do not fall into any of these cases, so it's a block in the middle of the epoch,
	// not in the edges.
	return BlockTypeNormal
}

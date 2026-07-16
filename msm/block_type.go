// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"crypto/sha256"
	"fmt"
)

type BlockType uint8

// A StateMachineBlock is a representation of a parsed OuterBlock, containing the inner block and the metadata.
type StateMachineBlock struct {
	// InnerBlock is the VM-level block, or nil if this is a block without an inner block (e.g., a Telock block).
	InnerBlock VMBlock
	// Metadata contains the state machine metadata associated with this block.
	Metadata StateMachineMetadata
}

// Digest returns the SHA-256 hash of the combined inner block digest and metadata digest.
func (smb *StateMachineBlock) Digest() [32]byte {
	var blockDigest [32]byte
	if smb.InnerBlock != nil {
		blockDigest = smb.InnerBlock.Digest()
	} else {
		blockDigest = [32]byte{}
	}
	mdDigest := sha256.Sum256(smb.Metadata.MarshalCanoto())
	combined := make([]byte, 64)
	copy(combined[:32], blockDigest[:])
	copy(combined[32:], mdDigest[:])
	return sha256.Sum256(combined)
}

const (
	BlockTypeNormal        BlockType = iota + 1 // In-epoch block with no epoch transition in progress
	BlockTypeZero                               // The first ever Simplex block; establishes the first epoch
	BlockTypeTelock                             // Built after the sealing block to extend its epoch until the sealing block finalizes
	BlockTypeSealing                            // Seals its epoch; only Telocks may follow it
	BlockTypeTransitioning                      // An epoch transition is in progress (collecting aux info and approvals) but the epoch is not yet sealed
)

func (bt BlockType) String() string {
	switch bt {
	case BlockTypeNormal:
		return "Normal"
	case BlockTypeTelock:
		return "Telock"
	case BlockTypeSealing:
		return "Sealing"
	default:
		return fmt.Sprintf("UnknownBlockType(%d)", bt)
	}
}

var emptyHash [32]byte

func (smb *StateMachineBlock) Type() BlockType {
	sei := smb.Metadata.SimplexEpochInfo

	// Only sealing blocks carry block validation descriptors
	if sei.BlockValidationDescriptor != nil {
		// The zeroth block has a descriptor but points to an empty digest
		if sei.PrevSealingBlockHash == emptyHash {
			return BlockTypeZero
		}
		return BlockTypeSealing
	}

	if sei.SealingBlockSeq != 0 {
		return BlockTypeTelock
	}

	if sei.NextPChainReferenceHeight > 0 {
		return BlockTypeTransitioning
	}

	// Otherwise, we do not fall into any of these cases, so it's a normal block
	return BlockTypeNormal
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"bytes"
	"slices"
)

// go:generate go run github.com/StephenButtolph/canoto/canoto encoding.go

// OuterBlock is the top-level encoding of a Simplex block.
// It contains the inner block (the block built by the VM),
// as well as metadata created by the StateMachine.
type OuterBlock struct {
	// InnerBlock is the block created by the VM, encoded as bytes and opaque to the StateMachine.
	InnerBlock []byte `canoto:"bytes,1"`
	// Metadata is created by the StateMachine.
	Metadata StateMachineMetadata `canoto:"value,2"`

	canotoData canotoData_OuterBlock
}

// StateMachineMetadata defines the metadata that the StateMachine uses to transition between epochs,
// and maintain ICM epoch information.
// TODO: change SimplexProtocolMetadata and SimplexBlacklist to be non-opaque types.
// TODO: This requires to encode the protocol metadata and blacklist using canoto.
type StateMachineMetadata struct {
	// SimplexEpochInfo is the metadata that the StateMachine uses for its own epoching.
	SimplexEpochInfo SimplexEpochInfo `canoto:"value,1"`
	// SimplexProtocolMetadata is the metadata that Simplex uses for its protocol, such as sequence and round number.
	SimplexProtocolMetadata []byte `canoto:"bytes,2"`
	// SimplexBlacklist is the metadata that Simplex uses to keep track of blacklisted nodes.
	// Blacklisted nodes do not become leaders.
	SimplexBlacklist []byte `canoto:"bytes,3"`
	// PChainHeight is the P-Chain height that the StateMachine sampled at the time of building the block.
	// It's used for ICM epoching, not for Simplex epoching.
	// For Simplex epoching, the P-Chain height that matters is the PChainReferenceHeight in the SimplexEpochInfo.
	PChainHeight uint64 `canoto:"uint,4"`
	// Timestamp is the time when the block is being built, in milliseconds since Unix epoch.
	Timestamp uint64 `canoto:"uint,5"`

	canotoData canotoData_StateMachineMetadata
}

// SimplexEpochInfo is metadata used by the StateMachine.
type SimplexEpochInfo struct {
	// PChainReferenceHeight is the P-Chain height that the StateMachine uses as a reference for the current epoch.
	// The validator set is determined based on the validators on the P-Chain at the PChainReferenceHeight.
	PChainReferenceHeight uint64 `canoto:"uint,1"`
	// EpochNumber is the current epoch number.
	// The first epoch is numbered 1, and each successive epoch is numbered according to the block sequence
	// of the sealing block of the previous epoch.
	EpochNumber uint64 `canoto:"uint,2"`
	// PrevSealingBlockHash is the hash of the sealing block of the previous epoch.
	// It is empty for the first epoch, and the second epoch has the PrevSealingBlockHash set to be
	// the hash of the first ever block built by the StateMachine.
	PrevSealingBlockHash [32]byte `canoto:"fixed bytes,3"`
	// NextPChainReferenceHeight is the P-Chain height that the StateMachine uses as a reference for the next epoch.
	// When the NextPChainReferenceHeight is > 0, it means the StateMachine is on its way to transition to a new epoch
	// in which the validator set will be based on the given P-chain height.
	NextPChainReferenceHeight uint64 `canoto:"uint,4"`
	// PrevVMBlockSeq is the block sequence of the previous block that has a VM block (inner block).
	// This is used to know on which VM block to build the next block.
	PrevVMBlockSeq uint64 `canoto:"uint,5"`
	// BlockValidationDescriptor is the metadata that describes the validator set of the next epoch.
	// It is only set in the sealing block, and nil in all other blocks.
	BlockValidationDescriptor *BlockValidationDescriptor `canoto:"pointer,6"`
	// NextEpochApprovals is the metadata that contains the approvals from validators for the next epoch.
	// It is set only in the sealing block and the blocks preceding it starting from a block that has a NextPChainReferenceHeight set.
	NextEpochApprovals *NextEpochApprovals `canoto:"pointer,7"`
	// SealingBlockSeq is the block sequence of the sealing block of the current epoch.
	// It defines the validator set of the next epoch.
	SealingBlockSeq uint64 `canoto:"uint,8"`

	canotoData canotoData_SimplexEpochInfo
}

func (sei *SimplexEpochInfo) IsZero() bool {
	var zero SimplexEpochInfo
	return sei.Equal(&zero)
}

func (sei *SimplexEpochInfo) Equal(other *SimplexEpochInfo) bool {
	if sei == nil {
		return other == nil
	}
	if other == nil {
		return false
	}
	if sei.BlockValidationDescriptor == nil && other.BlockValidationDescriptor != nil {
		return false
	}
	if sei.BlockValidationDescriptor != nil && other.BlockValidationDescriptor == nil {
		return false
	}
	if sei.NextEpochApprovals == nil && other.NextEpochApprovals != nil {
		return false
	}
	if sei.NextEpochApprovals != nil && other.NextEpochApprovals == nil {
		return false
	}

	if sei.PChainReferenceHeight != other.PChainReferenceHeight || sei.EpochNumber != other.EpochNumber ||
		sei.NextPChainReferenceHeight != other.NextPChainReferenceHeight ||
		sei.PrevVMBlockSeq != other.PrevVMBlockSeq || sei.SealingBlockSeq != other.SealingBlockSeq {
		return false
	}
	if !bytes.Equal(sei.PrevSealingBlockHash[:], other.PrevSealingBlockHash[:]) {
		return false
	}
	if sei.BlockValidationDescriptor != nil && !sei.BlockValidationDescriptor.Equals(other.BlockValidationDescriptor) {
		return false
	}
	if sei.NextEpochApprovals != nil && !sei.NextEpochApprovals.Equals(other.NextEpochApprovals) {
		return false
	}
	return true
}

type NodeBLSMapping struct {
	NodeID nodeID `canoto:"fixed bytes,1"`
	BLSKey []byte `canoto:"bytes,2"`
	Weight uint64 `canoto:"uint,3"`

	canotoData canotoData_NodeBLSMapping
}

func (nbm *NodeBLSMapping) Clone() NodeBLSMapping {
	var cloned NodeBLSMapping
	copy(cloned.NodeID[:], nbm.NodeID[:])
	cloned.BLSKey = make([]byte, len(nbm.BLSKey))
	copy(cloned.BLSKey, nbm.BLSKey)
	cloned.Weight = nbm.Weight
	return cloned
}

func (nbm *NodeBLSMapping) Equals(other *NodeBLSMapping) bool {
	if !slices.Equal(nbm.NodeID[:], other.NodeID[:]) {
		return false
	}
	if !slices.Equal(nbm.BLSKey, other.BLSKey) {
		return false
	}
	if nbm.Weight != other.Weight {
		return false
	}
	return true
}

type BlockValidationDescriptor struct {
	AggregatedMembership AggregatedMembership `canoto:"value,1"`

	canotoData canotoData_BlockValidationDescriptor
}

func (bvd *BlockValidationDescriptor) Equals(other *BlockValidationDescriptor) bool {
	if bvd == nil && other == nil {
		return true
	}
	if bvd == nil || other == nil {
		return false
	}
	return bvd.AggregatedMembership.Equals(other.AggregatedMembership.Members)
}

type AggregatedMembership struct {
	Members []NodeBLSMapping `canoto:"repeated value,1"`

	canotoData canotoData_AggregatedMembership
}

func (c *AggregatedMembership) Equals(members []NodeBLSMapping) bool {
	if len(c.Members) != len(members) {
		return false
	}

	for i := range c.Members {
		if !c.Members[i].Equals(&members[i]) {
			return false
		}
	}
	return true
}

type NextEpochApprovals struct {
	NodeIDs   []byte `canoto:"bytes,1"`
	Signature []byte `canoto:"bytes,2"`

	canotoData canotoData_NextEpochApprovals
}

func (nea *NextEpochApprovals) Equals(other *NextEpochApprovals) bool {
	if nea == nil && other == nil {
		return true
	}
	if nea == nil || other == nil {
		return false
	}
	if !bytes.Equal(nea.NodeIDs, other.NodeIDs) {
		return false
	}
	if !bytes.Equal(nea.Signature, other.Signature) {
		return false
	}
	return true
}

type NodeBLSMappings []NodeBLSMapping

func (nbms NodeBLSMappings) Clone() NodeBLSMappings {
	cloned := make(NodeBLSMappings, len(nbms))
	for i, nbm := range nbms {
		cloned[i] = nbm.Clone()
	}
	return cloned
}

func (nbms NodeBLSMappings) TotalWeight() (uint64, error) {
	var total uint64
	for _, nbm := range nbms {
		sum, err := safeAdd(total, nbm.Weight)
		if err != nil {
			return 0, err
		}
		total = sum
	}
	return total, nil
}

func (nbms NodeBLSMappings) Equal(other NodeBLSMappings) bool {
	if len(nbms) != len(other) {
		return false
	}

	sortByNodeID := func(a, b NodeBLSMapping) int {
		return slices.Compare(a.NodeID[:], b.NodeID[:])
	}

	nbmsClone := nbms.Clone()
	otherClone := other.Clone()
	slices.SortFunc(nbmsClone, sortByNodeID)
	slices.SortFunc(otherClone, sortByNodeID)

	return slices.EqualFunc(nbmsClone, otherClone, func(a, b NodeBLSMapping) bool {
		return a.Equals(&b)
	})
}

type ValidatorSetApproval struct {
	NodeID           nodeID   `canoto:"fixed bytes,1"`
	AuxInfoSeqDigest [32]byte `canoto:"fixed bytes,2"`
	PChainHeight     uint64   `canoto:"uint,3"`
	Signature        []byte   `canoto:"bytes,4"`

	canotoData canotoData_ValidatorSetApproval
}

type ValidatorSetApprovals []ValidatorSetApproval

func (vsa ValidatorSetApprovals) Filter(keep func(ValidatorSetApproval) bool) ValidatorSetApprovals {
	result := make(ValidatorSetApprovals, 0, len(vsa))
	for _, v := range vsa {
		if keep(v) {
			result = append(result, v)
		}
	}
	return result
}

func (vsa ValidatorSetApprovals) UniqueByNodeID() ValidatorSetApprovals {
	seen := make(map[nodeID]struct{})
	result := make(ValidatorSetApprovals, 0, len(vsa))
	for _, v := range vsa {
		if _, exists := seen[v.NodeID]; exists {
			continue
		}
		seen[v.NodeID] = struct{}{}
		result = append(result, v)
	}
	return result
}

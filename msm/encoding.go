// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"bytes"
	"slices"
)

// go:generate go tool canoto encoding.go

type OuterBlock struct {
	InnerBlock []byte               `canoto:"bytes,1"`
	Metadata   StateMachineMetadata `canoto:"value,2"`

	canotoData canotoData_OuterBlock
}

type StateMachineMetadata struct {
	ICMEpochInfo            ICMEpochInfo     `canoto:"value,1"`
	SimplexEpochInfo        SimplexEpochInfo `canoto:"value,2"`
	SimplexProtocolMetadata []byte           `canoto:"bytes,3"`
	SimplexBlacklist        []byte           `canoto:"bytes,4"`
	AuxiliaryInfo           *AuxiliaryInfo   `canoto:"pointer,5"`
	PChainHeight            uint64           `canoto:"uint,6"`
	Timestamp               uint64           `canoto:"uint,7"`

	canotoData canotoData_OuterBlock
}

type ICMEpochInfo struct {
	EpochStartTime    uint64 `canoto:"uint,1"`
	EpochNumber       uint64 `canoto:"uint,2"`
	PChainEpochHeight uint64 `canoto:"uint,3"`

	canotoData canotoData_ICMEpochInfo
}

func (ei *ICMEpochInfo) Equal(other *ICMEpochInfo) bool {
	if other == nil {
		return ei == nil
	}
	return ei.EpochStartTime == other.EpochStartTime && ei.EpochNumber == other.EpochNumber && ei.PChainEpochHeight == other.PChainEpochHeight
}

type AuxiliaryInfo struct {
	Info           []byte `canoto:"bytes,1"`
	PrevAuxInfoSeq uint64 `canoto:"uint,2"`
	ApplicationID  uint32 `canoto:"uint,3"`

	canotoData canotoData_AuxiliaryInfo
}

type SimplexEpochInfo struct {
	PChainReferenceHeight     uint64                     `canoto:"uint,1"`
	EpochNumber               uint64                     `canoto:"uint,2"`
	PrevSealingBlockHash      [32]byte                   `canoto:"fixed bytes,3"`
	NextPChainReferenceHeight uint64                     `canoto:"uint,4"`
	PrevVMBlockSeq            uint64                     `canoto:"uint,5"`
	BlockValidationDescriptor *BlockValidationDescriptor `canoto:"pointer,6"`
	NextEpochApprovals        *NextEpochApprovals        `canoto:"pointer,7"`
	SealingBlockSeq           uint64                     `canoto:"uint,8"`

	canotoData canotoData_SimplexEpochInfo
}

func (sei *SimplexEpochInfo) Equal(other *SimplexEpochInfo) bool {
	if other == nil {
		return sei == nil
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

func (nbms NodeBLSMappings) TotalWeight() (uint64, error) {
	return nbms.SumWeights(func(int, NodeBLSMapping) bool {
		return true
	})
}

func (nbms NodeBLSMappings) ForEach(selector func(int, NodeBLSMapping)) {
	for i, nbm := range nbms {
		selector(i, nbm)
	}
}

func (nbms NodeBLSMappings) SumWeights(selector func(int, NodeBLSMapping) bool) (uint64, error) {
	var total uint64
	var err error
	nbms.ForEach(func(i int, nbm NodeBLSMapping) {
		if err != nil {
			return
		}
		if selector(i, nbm) {
			total, err = safeAdd(total, nbm.Weight)
			total += nbm.Weight
		}
	})
	return total, err
}

func (nbms NodeBLSMappings) Compare(other NodeBLSMappings) bool {
	if len(nbms) != len(other) {
		return false
	}

	slices.SortFunc(nbms, func(a, b NodeBLSMapping) int {
		return slices.Compare(a.NodeID[:], b.NodeID[:])
	})

	slices.SortFunc(other, func(a, b NodeBLSMapping) int {
		return slices.Compare(a.NodeID[:], b.NodeID[:])
	})

	for i := range nbms {
		if !nbms[i].Equals(&other[i]) {
			return false
		}
	}
	return true
}

type ValidatorSetApproval struct {
	NodeID           nodeID   `canoto:"fixed bytes,1"`
	AuxInfoSeqDigest [32]byte `canoto:"fixed bytes,2"`
	PChainHeight     uint64   `canoto:"uint,3"`
	Signature        []byte   `canoto:"bytes,4"`

	canotoData canotoData_ValidatorSetApproval
}

type ValidatorSetApprovals []ValidatorSetApproval

func (vsa ValidatorSetApprovals) ForEach(f func(int, ValidatorSetApproval)) {
	for i, v := range vsa {
		f(i, v)
	}
}

func (vsa ValidatorSetApprovals) Filter(f func(int, ValidatorSetApproval) bool) ValidatorSetApprovals {
	result := make(ValidatorSetApprovals, 0, len(vsa))
	vsa.ForEach(func(i int, v ValidatorSetApproval) {
		if f(i, v) {
			result = append(result, v)
		}
	})
	return result
}

type StateMachineBlockPreImage struct {
	InnerBlockHash []byte               `canoto:"bytes,1"`
	Metadata       StateMachineMetadata `canoto:"value,2"`

	canotoData canotoData_StateMachineBlockPreImage
}

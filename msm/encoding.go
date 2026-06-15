// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"bytes"
	"slices"

	"github.com/ava-labs/simplex/common"
)

//go:generate go run github.com/StephenButtolph/canoto/canoto encoding.go

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
	// ICMEpochInfo is the metadata that the StateMachine uses for ICM epoching.
	ICMEpochInfo ICMEpochInfo `canoto:"value,6"`

	canotoData canotoData_StateMachineMetadata
}

// ICMEpochInfo is the ICM epoch information that is maintained by the StateMachine and used for the ICM protocol.
// The StateMachine maintains this information identically to how the proposerVM maintains it, and it does so by
// building the ICMEpochInput and then passing it into the StateMachine's ComputeICMEpoch function.
type ICMEpochInfo struct {
	// EpochStartTime is the Unix timestamp when this ICM epoch started.
	EpochStartTime uint64 `canoto:"uint,1"`
	// EpochNumber is the sequential identifier of this ICM epoch.
	EpochNumber uint64 `canoto:"uint,2"`
	// PChainEpochHeight is the P-chain height associated with this ICM epoch.
	PChainEpochHeight uint64 `canoto:"uint,3"`

	canotoData canotoData_ICMEpochInfo
}

func (ei *ICMEpochInfo) Equal(other *ICMEpochInfo) bool {
	if ei == nil {
		return other == nil
	}
	if other == nil {
		return ei == nil
	}
	return ei.EpochStartTime == other.EpochStartTime && ei.EpochNumber == other.EpochNumber && ei.PChainEpochHeight == other.PChainEpochHeight
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
	// It is set to the hash of the zero block in the first epoch, and in subsequent epochs it is set to be
	// the hash of the sealing block of the previous epoch.
	// This is used to be able to quickly fetch and verify the sealing blocks without having to retrieve the interleaving blocks,
	// which allows to bootstrap the BLS keys of the validator set for each epoch before fully syncing the interleaving blocks.
	PrevSealingBlockHash [32]byte `canoto:"fixed bytes,3"`
	// NextPChainReferenceHeight is the P-Chain height that the StateMachine uses as a reference for the next epoch.
	// When the NextPChainReferenceHeight is > 0, it means the StateMachine is on its way to transition to a new epoch
	// in which the validator set will be based on the given P-chain height.
	// It sets the PChainReferenceHeight for the next epoch.
	NextPChainReferenceHeight uint64 `canoto:"uint,4"`
	// PrevVMBlockSeq is the block sequence of the previous block that has a VM block (inner block).
	// This is used to know on which VM block to build the next block.
	PrevVMBlockSeq uint64 `canoto:"uint,5"`
	// BlockValidationDescriptor is the metadata that describes the validator set of the next epoch.
	// It is only set in the sealing block and zero block, and nil in all other blocks.
	BlockValidationDescriptor *BlockValidationDescriptor `canoto:"pointer,6"`
	// NextEpochApprovals is the metadata that contains the approvals from validators for the next epoch.
	// It is set only in the sealing block and the blocks preceding it starting from a block that has a NextPChainReferenceHeight set.
	NextEpochApprovals *NextEpochApprovals `canoto:"pointer,7"`
	// SealingBlockSeq is the block sequence of the sealing block of the current epoch.
	// It defines the validator set of the next epoch.
	// It is set once the first Telock is built and is copied over to subsequent Telocks.
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

// NextState returns the state used to build (or verify) the block that follows the one
// described by the SimplexEpochInfo.
func (sei *SimplexEpochInfo) NextState() state {
	// No Simplex epoch has started yet: the next block is the first one built by Simplex.
	if sei.EpochNumber == 0 {
		return stateFirstSimplexBlock
	}

	// No epoch transition in progress, so the next block is a normal in-epoch block.
	if sei.NextPChainReferenceHeight == 0 {
		return stateBuildBlockNormalOp
	}

	// If NextPChainReferenceHeight > 0, then an epoch transition is in progress.
	// An epoch is sealed if
	//   - SealingBlockSeq > 0: SimplexEpochInfo describes a Telock.
	//   - BlockValidationDescriptor != nil: SimplexEpochInfo describes the sealing block itself.
	if sei.SealingBlockSeq > 0 || sei.BlockValidationDescriptor != nil {
		return stateBuildBlockEpochSealed
	}

	// NextPChainReferenceHeight > 0 but the epoch is not yet sealed: we are still
	// collecting validator approvals for the next epoch.
	return stateBuildCollectingApprovals
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

func (nbms NodeBLSMappings) NodeWeights() common.Nodes {
	nodeWeights := make(common.Nodes, len(nbms))
	for i, nbm := range nbms {
		nodeWeights[i] = common.Node{
			Id:     nbm.NodeID[:],
			Weight: nbm.Weight,
		}
	}
	return nodeWeights
}

// IndexByNodeID returns a mapping from NodeID to the validator's index in the set,
// which is the position used by approval bitmasks.
func (nbms NodeBLSMappings) IndexByNodeID() map[nodeID]int {
	result := make(map[nodeID]int, len(nbms))
	for i, nbm := range nbms {
		result[nbm.NodeID] = i
	}
	return result
}

func (nbms NodeBLSMappings) SelectSubset(bitmask bitmask) []common.NodeID {
	nodeIDs := make([]common.NodeID, 0, len(nbms))
	for i, nbm := range nbms {
		if !bitmask.Contains(i) {
			continue
		}
		nodeIDs = append(nodeIDs, nbm.NodeID[:])
	}

	return nodeIDs
}

func (nbms NodeBLSMappings) Clone() NodeBLSMappings {
	cloned := make(NodeBLSMappings, len(nbms))
	for i, nbm := range nbms {
		cloned[i] = nbm.Clone()
	}
	return cloned
}

func (nbms NodeBLSMappings) Equal(other NodeBLSMappings) bool {
	if len(nbms) != len(other) {
		return false
	}

	nbmsClone := nbms.Clone()
	otherClone := other.Clone()

	slices.SortFunc(nbmsClone, func(a, b NodeBLSMapping) int {
		return slices.Compare(a.NodeID[:], b.NodeID[:])
	})

	slices.SortFunc(otherClone, func(a, b NodeBLSMapping) int {
		return slices.Compare(a.NodeID[:], b.NodeID[:])
	})

	for i := range nbmsClone {
		if !nbmsClone[i].Equals(&otherClone[i]) {
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

func (vsa ValidatorSetApprovals) Filter(f func(ValidatorSetApproval, common.Logger) bool, logger common.Logger) ValidatorSetApprovals {
	result := make(ValidatorSetApprovals, 0, len(vsa))
	for _, v := range vsa {
		if f(v, logger) {
			result = append(result, v)
		}
	}
	return result
}

func (vsa ValidatorSetApprovals) UniqueByNodeID() ValidatorSetApprovals {
	seen := make(map[nodeID]struct{})
	result := make(ValidatorSetApprovals, 0, len(vsa))
	for _, v := range vsa {
		if _, exists := seen[v.NodeID]; !exists {
			seen[v.NodeID] = struct{}{}
			result = append(result, v)
		}
	}
	return result
}

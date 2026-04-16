// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/ava-labs/simplex"
)

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

// ApprovalsRetriever retrieves the approvals from validators of the next epoch for the epoch change.
type ApprovalsRetriever interface {
	RetrieveApprovals() ValidatorSetApprovals
}

// SignatureVerifier verifies a cryptographic signature against a message and public key.
// Used to verify Approvals from validators for epoch transitions.
type SignatureVerifier interface {
	VerifySignature(signature []byte, message []byte, publicKey []byte) error
}

// SignatureAggregator combines multiple cryptographic signatures into a single aggregated signature.
// Used to aggregate validator signatures for epoch transitions.
type SignatureAggregator interface {
	AggregateSignatures(signatures ...[]byte) ([]byte, error)
}

// KeyAggregator combines multiple public keys into a single aggregated public key.
type KeyAggregator interface {
	AggregateKeys(keys ...[]byte) ([]byte, error)
}

// ValidatorSetRetriever retrieves the validator set at a given P-chain height.
type ValidatorSetRetriever func(pChainHeight uint64) (NodeBLSMappings, error)

// RetrievingOpts specifies the options for retrieving a block by height and/or digest.
type RetrievingOpts struct {
	// Height is the sequence number of the block to retrieve.
	Height uint64
	// Digest is the expected hash of the block, used for validation.
	Digest [32]byte
}

// BlockRetriever retrieves a block and its finalization status given the retrieval options.
// If the block cannot be found it returns ErrBlockNotFound.
// If an error occurs during retrieval, it returns a non-nil error.
type BlockRetriever func(RetrievingOpts) (StateMachineBlock, *simplex.Finalization, error)

type state uint8

const (
	stateFirstSimplexBlock state = iota
	stateBuildBlockNormalOp
	stateBuildCollectingApprovals
	stateBuildBlockEpochSealed
)

type BlockType uint8

const (
	BlockTypeNormal BlockType = iota
	BlockTypeTelock
	BlockTypeSealing
	BlockTypeNewEpoch
)

func (state BlockType) String() string {
	switch state {
	case BlockTypeNormal:
		return "Normal"
	case BlockTypeTelock:
		return "Telock"
	case BlockTypeSealing:
		return "Sealing"
	case BlockTypeNewEpoch:
		return "NewEpoch"
	default:
		return fmt.Sprintf("UnknownBlockType(%d)", state)
	}
}

func identifyCurrentState(prevBlockSimplexEpochInfo SimplexEpochInfo) (state, error) {
	// If this is the first ever epoch, then this is also the first ever block to be built by Simplex.
	if prevBlockSimplexEpochInfo.EpochNumber == 0 {
		return stateFirstSimplexBlock, nil
	}

	// If we don't have a next P-chain preference height, it means we are not transitioning to a new epoch just yet.
	if prevBlockSimplexEpochInfo.NextPChainReferenceHeight == 0 {
		return stateBuildBlockNormalOp, nil
	}

	// If the previous block has a sealing block sequence, it's a Telock.
	// If it has a block validation descriptor, it's a sealing block.
	// Eithe way, the epoch has been sealed.
	if prevBlockSimplexEpochInfo.SealingBlockSeq > 0 || prevBlockSimplexEpochInfo.BlockValidationDescriptor != nil {
		return stateBuildBlockEpochSealed, nil
	}

	// In any other case, NextPChainReferenceHeight > 0 but the previous block is not a Telock or sealing block,
	// it means we are in the process of collecting approvals for the next epoch.
	return stateBuildCollectingApprovals, nil
}

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
		// The zero-epoch block has BlockValidationDescriptor but epoch number 1 and next P-chain reference height of 0.,
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

// computeNewApproverSignaturesAndSigners computes the signatures of the nodes that approve the next epoch including the previous aggregated signature,
// and bitmask of nodes that correspond to those signatures, and aggregates all signatures together.
func computeNewApproverSignaturesAndSigners(nextEpochApprovals *NextEpochApprovals, approvalsFromPeers ValidatorSetApprovals, oldApprovingNodes bitmask, nodeID2ValidatorIndex map[nodeID]int, aggregator SignatureAggregator) ([]byte, bitmask, error) {
	// Prepare the new signatures from the new approvals that haven't approved yet and that agree with our candidate auxiliary info digest and P-Chain height.
	newSignatures := make([][]byte, 0, len(approvalsFromPeers)+1)

	// We will overwrite the old approving nodes with the new approving nodes, by turning on the bits for the new approvers.
	newApprovingNodes := oldApprovingNodes.Clone()

	approvalsFromPeers.ForEach(func(i int, approval ValidatorSetApproval) {
		approvingNodeIndexOfNewApprover, exists := nodeID2ValidatorIndex[approval.NodeID]
		if !exists {
			// This should not happen, because we have already filtered approvals that are not in the validator set, but we check just in case.
			return
		}
		// Turn on the bit for the new approver
		newApprovingNodes.Add(approvingNodeIndexOfNewApprover)
		newSignatures = append(newSignatures, approval.Signature)
	})

	// Add the existing signature into the list of signatures to aggregate
	existingSignature := nextEpochApprovals.Signature
	if existingSignature != nil {
		newSignatures = append(newSignatures, existingSignature)
	}

	// Finally, we aggregate all signatures together, to compute the new aggregated signature.
	aggregatedSignature, err := aggregator.AggregateSignatures(newSignatures...)
	if err != nil {
		return nil, bitmask{}, fmt.Errorf("failed to aggregate signatures: %w", err)
	}

	return aggregatedSignature, *newApprovingNodes, nil
}

// sanitizeApprovals filters out approvals that are not valid by checking if they agree with our candidate auxiliary info digest and P-Chain height,
// and if they are from the validator set and haven't already been approved.
func sanitizeApprovals(approvals ValidatorSetApprovals, pChainHeight uint64, nodeID2ValidatorIndex map[nodeID]int, oldApprovingNodes bitmask) ValidatorSetApprovals {
	filter1 := approvalsThatAgreeWithAuxInfoAndPChainHeight(pChainHeight)
	filter2 := approvalsThatAreInValidatorSetAndHaveNotAlreadyApproved(oldApprovingNodes.Clone(), nodeID2ValidatorIndex)
	return approvals.Filter(filter1).Filter(filter2).UniqueByNodeID()
}

func approvalsThatAgreeWithAuxInfoAndPChainHeight(pChainHeight uint64) func(i int, approval ValidatorSetApproval) bool {
	return func(i int, approval ValidatorSetApproval) bool {
		// Pick only approvals that agree with our candidate auxiliary info digest and P-Chain height
		return approval.PChainHeight == pChainHeight
	}
}

func approvalsThatAreInValidatorSetAndHaveNotAlreadyApproved(oldApprovingNodes *bitmask, nodeID2ValidatorIndex map[nodeID]int) func(i int, approval ValidatorSetApproval) bool {
	return func(i int, approval ValidatorSetApproval) bool {
		approvingNodeIndexOfNewApprover, exists := nodeID2ValidatorIndex[approval.NodeID]
		if !exists {
			// If the approving node is not in the validator set, we ignore this approval.
			return false
		}
		// Only pick approvals from nodes that haven't already approved
		return !oldApprovingNodes.Contains(approvingNodeIndexOfNewApprover)
	}
}

func computeApprovingWeight(validators NodeBLSMappings, approvingNodes *bitmask) (int64, error) {
	var approvingWeight uint64
	var err error
	validators.ForEach(func(i int, nbm NodeBLSMapping) {
		if err != nil {
			return
		}
		if !approvingNodes.Contains(i) {
			return
		}
		approvingWeight, err = safeAdd(approvingWeight, nbm.Weight)
	})

	if err != nil {
		return 0, fmt.Errorf("failed to compute approving weights: %w", err)
	}

	if approvingWeight > math.MaxInt64 {
		return 0, fmt.Errorf("approving weight of validators is too big, overflows int64: %d", approvingWeight)
	}

	return int64(approvingWeight), nil
}

func computeTotalWeight(validators NodeBLSMappings) (int64, error) {
	totalWeight, err := validators.TotalWeight()
	if err != nil {
		return 0, fmt.Errorf("failed to sum weights of all nodes: %w", err)
	}

	if totalWeight == 0 {
		return 0, fmt.Errorf("total weight of validators is 0")
	}

	if totalWeight > math.MaxInt64 {
		return 0, fmt.Errorf("total weight of validators is too big, overflows int64: %d", totalWeight)
	}
	return int64(totalWeight), nil
}
func findFirstSimplexBlock(getBlock BlockRetriever, endHeight uint64) (uint64, error) {
	var haltError error

	// Make sure the bound passed to sort.Search is valid, to avoid illegal input caused by overflow.
	if endHeight >= math.MaxInt {
		endHeight = math.MaxInt - 1
	}

	firstSimplexBlock := sort.Search(int(endHeight+1), func(i int) bool {
		if haltError != nil {
			return true
		}
		block, _, err := getBlock(RetrievingOpts{Height: uint64(i)})
		if errors.Is(err, simplex.ErrBlockNotFound) {
			return false
		}
		if err != nil {
			haltError = fmt.Errorf("error retrieving block at height %d: %w", i, err)
			return false
		}
		// The first Simplex block is such that its epoch info isn't the zero value.
		return !block.Metadata.SimplexEpochInfo.IsZero()
	})
	if haltError != nil {
		return 0, haltError
	}

	if uint64(firstSimplexBlock) > endHeight {
		return 0, fmt.Errorf("no simplex blocks found in range [%d, %d]", 0, endHeight)
	}

	return uint64(firstSimplexBlock), nil
}

func computePrevVMBlockSeq(parentBlock StateMachineBlock, prevBlockSeq uint64) uint64 {
	// Either our parent block has no inner block, in which case we just inherit its previous VM block sequence,
	if parentBlock.InnerBlock == nil {
		return parentBlock.Metadata.SimplexEpochInfo.PrevVMBlockSeq
	}
	// or it has an inner block, in which case it is the previous block sequence.
	return prevBlockSeq
}

type approvals struct {
	canSeal   bool
	nodeIDs   []byte
	signature []byte
}

func ensureNextEpochApprovalsSignersSupersetOfApprovalsOfPrevBlock(prev SimplexEpochInfo, next SimplexEpochInfo) error {
	if prev.NextEpochApprovals == nil {
		// Condition satisifed vacously.
		return nil
	}
	// Else, prev.NextEpochApprovals is not nil.
	// If next.NextEpochApprovals is nil, condition is not satisfied.
	if next.NextEpochApprovals == nil {
		return fmt.Errorf("previous block has next epoch approvals but proposed block doesn't have next epoch approvals")
	}

	// Make sure that previous signers are still there.
	prevSigners := bitmaskFromBytes(prev.NextEpochApprovals.NodeIDs)
	nextSigners := bitmaskFromBytes(next.NextEpochApprovals.NodeIDs)
	// Remove all bits in nextSigners from prevSigners
	prevSigners.Difference(&nextSigners)
	// If we have some bits left, it means there was a bit in prevSigners that wasn't in nextSigners
	if prevSigners.Len() > 0 {
		return fmt.Errorf("some signers from parent block are missing from next epoch approvals of proposed block")
	}
	return nil
}

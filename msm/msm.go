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

// Used to aggregate validator signatures for epoch transitions.
type SignatureAggregator interface {
	AggregateSignatures(signatures ...[]byte) ([]byte, error)
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

func identifyCurrentState(prevBlockSimplexEpochInfo SimplexEpochInfo) state {
	// If this is the first ever epoch, then this is also the first ever block to be built by Simplex.
	if prevBlockSimplexEpochInfo.EpochNumber == 0 {
		return stateFirstSimplexBlock
	}

	// If we don't have a next P-chain preference height, it means we are not transitioning to a new epoch just yet.
	if prevBlockSimplexEpochInfo.NextPChainReferenceHeight == 0 {
		return stateBuildBlockNormalOp
	}

	// If the previous block has a sealing block sequence, it's a Telock.
	// If it has a block validation descriptor, it's a sealing block.
	// Either way, the epoch has been sealed.
	if prevBlockSimplexEpochInfo.SealingBlockSeq > 0 || prevBlockSimplexEpochInfo.BlockValidationDescriptor != nil {
		return stateBuildBlockEpochSealed
	}

	// In any other case, NextPChainReferenceHeight > 0 but the previous block is not a Telock or sealing block,
	// it means we are in the process of collecting approvals for the next epoch.
	return stateBuildCollectingApprovals
}

// computeNewApproverSignaturesAndSigners computes the signatures of the nodes that approve the next epoch including the previous aggregated signature,
// and bitmask of nodes that correspond to those signatures, and aggregates all signatures together.
func computeNewApproverSignaturesAndSigners(nextEpochApprovals *NextEpochApprovals, approvalsFromPeers ValidatorSetApprovals, oldApprovingNodes bitmask, nodeID2ValidatorIndex map[nodeID]int, aggregator SignatureAggregator) ([]byte, bitmask, error) {
	if nextEpochApprovals == nil {
		return nil, bitmask{}, fmt.Errorf("next epoch approvals is nil")
	}
	// Prepare the new signatures from the new approvals that haven't approved yet and that agree with our candidate auxiliary info digest and P-Chain height.
	newSignatures := make([][]byte, 0, len(approvalsFromPeers)+1)

	// We will overwrite the old approving nodes with the new approving nodes, by turning on the bits for the new approvers.
	newApprovingNodes := oldApprovingNodes.Clone()

	for _, approval := range approvalsFromPeers {
		approvingNodeIndexOfNewApprover, exists := nodeID2ValidatorIndex[approval.NodeID]
		if !exists {
			// This should not happen, because we have already filtered approvals that are not in the validator set, but we check just in case.
			continue
		}

		// Check if the node has already approved in the past.
		if newApprovingNodes.Contains(approvingNodeIndexOfNewApprover) {
			continue
		}

		// Turn on the bit for the new approver
		newApprovingNodes.Add(approvingNodeIndexOfNewApprover)
		newSignatures = append(newSignatures, approval.Signature)
	}

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

	return aggregatedSignature, newApprovingNodes, nil
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

func approvalsThatAreInValidatorSetAndHaveNotAlreadyApproved(oldApprovingNodes bitmask, nodeID2ValidatorIndex map[nodeID]int) func(i int, approval ValidatorSetApproval) bool {
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

func findFirstSimplexBlock(getBlock BlockRetriever, endHeight uint64) (uint64, error) {
	var haltError error

	if endHeight > math.MaxInt-1 {
		return 0, fmt.Errorf("endHeight %d is too big, must be at most %d", endHeight, math.MaxInt-1)
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

var (
	errSignerSetShrinked          = fmt.Errorf("some signers from parent block are missing from next epoch approvals of proposed block")
	errNextEpochApprovalsShrinked = fmt.Errorf("previous block has next epoch approvals but proposed block doesn't have next epoch approvals")
)

func ensureNextEpochApprovalsSignersSupersetOfApprovalsOfPrevBlock(prev SimplexEpochInfo, next SimplexEpochInfo) error {
	if prev.NextEpochApprovals == nil {
		// Condition satisifed vacuously.
		return nil
	}
	// Else, prev.NextEpochApprovals is not nil.
	// If next.NextEpochApprovals is nil, condition is not satisfied.
	if next.NextEpochApprovals == nil {
		return errNextEpochApprovalsShrinked
	}

	// Make sure that previous signers are still there.
	prevSigners := bitmaskFromBytes(prev.NextEpochApprovals.NodeIDs)
	nextSigners := bitmaskFromBytes(next.NextEpochApprovals.NodeIDs)
	// Remove all bits in nextSigners from prevSigners
	prevSigners.Difference(&nextSigners)
	// If we have some bits left, it means there was a bit in prevSigners that wasn't in nextSigners
	if prevSigners.Len() > 0 {
		return errSignerSetShrinked
	}
	return nil
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"math/big"
	"sort"
	"time"

	"github.com/ava-labs/simplex"
)

type ICMEpochInput struct {
	ParentPChainHeight uint64
	ParentTimestamp    time.Time
	ChildTimestamp     time.Time
	ParentEpoch        ICMEpoch
}

type ICMEpoch struct {
	EpochStartTime    uint64
	EpochNumber       uint64
	PChainEpochHeight uint64
}

type StateMachineBlock struct {
	InnerBlock VMBlock
	Metadata   StateMachineMetadata
}

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

type ApprovalsRetriever interface {
	RetrieveApprovals() ValidatorSetApprovals
}

type SignatureVerifier interface {
	VerifySignature(signature []byte, message []byte, publicKey []byte) error
}

type SignatureAggregator interface {
	AggregateSignatures(signatures ...[]byte) ([]byte, error)
}

type KeyAggregator interface {
	AggregateKeys(keys ...[]byte) ([]byte, error)
}

type Block interface {
	Height() uint64

	Hash() [32]byte

	Metadata() StateMachineMetadata

	HasInnerBlock() bool
}

type ICMEpochTransition func(upgradeConfig, ICMEpochInput) ICMEpoch

type ValidatorSetRetriever func(pChainHeight uint64) (NodeBLSMappings, error)

type BlockRetriever func(height uint64) (StateMachineBlock, *simplex.Finalization, error)

type BlockBuilder interface {
	BuildBlock(ctx context.Context, pChainHeight uint64) (VMBlock, error)
}

type StateMachine struct {
	LatestPersistedHeight    uint64
	MaxBlockBuildingWaitTime time.Duration
	ComputeICMEpoch          ICMEpochTransition
	GetPChainHeight          func() uint64
	GetUpgrades              func() upgradeConfig
	BlockBuilder             BlockBuilder
	Logger                   Logger
	GetValidatorSet          ValidatorSetRetriever
	GetBlock                 BlockRetriever
	ApprovalsRetriever       ApprovalsRetriever
	SignatureAggregator      SignatureAggregator
	KeyAggregator            KeyAggregator
	SignatureVerifier        SignatureVerifier

	initialized bool
	verifiers   []verifier
}

type state uint8

const (
	stateFirstSimplexBlock state = iota
	stateBuildBlockNormalOp
	stateBuildCollectingApprovals
	stateBuildBlockEpochSealed
)

type blockType uint8

const (
	blockTypeNormal blockType = iota
	blockTypeTelock
	blockTypeSealing
	blockTypeNewEpoch
)

func (sm *StateMachine) BuildBlock(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata simplex.ProtocolMetadata, simplexBlacklist *simplex.Blacklist) (*StateMachineBlock, error) {
	sm.maybeInit()

	simplexMetadataBytes := simplexMetadata.Bytes()
	var simplexBlacklistBytes []byte
	if simplexBlacklist != nil {
		simplexBlacklistBytes = simplexBlacklist.Bytes()
	}

	currentState, err := sm.identifyCurrentState(parentBlock.Metadata.SimplexEpochInfo)
	if err != nil {
		return nil, err
	}

	if simplexMetadata.Seq == 0 {
		return nil, fmt.Errorf("invalid ProtocolMetadata sequence number: should be > 0, got %d", simplexMetadata.Seq)
	}

	prevBlockSeq := simplexMetadata.Seq - 1

	switch currentState {
	case stateFirstSimplexBlock:
		return sm.buildBlockZeroEpoch(ctx, parentBlock, simplexMetadataBytes, simplexBlacklistBytes, prevBlockSeq)
	case stateBuildBlockNormalOp:
		return sm.buildBlockNormalOp(ctx, parentBlock, simplexMetadataBytes, simplexBlacklistBytes, prevBlockSeq)
	case stateBuildCollectingApprovals:
		return sm.buildBlockCollectingApprovals(ctx, parentBlock, simplexMetadataBytes, simplexBlacklistBytes, prevBlockSeq)
	case stateBuildBlockEpochSealed:
		return sm.buildBlockEpochSealed(ctx, parentBlock, simplexMetadataBytes, simplexBlacklistBytes, prevBlockSeq)
	default:
		return nil, fmt.Errorf("unknown state %d", currentState)
	}
}

func (sm *StateMachine) VerifyBlock(ctx context.Context, block *StateMachineBlock) error {
	sm.maybeInit()

	if block == nil {
		return fmt.Errorf("InnerBlock is nil")
	}

	pmd, err := simplex.ProtocolMetadataFromBytes(block.Metadata.SimplexProtocolMetadata)
	if err != nil {
		return fmt.Errorf("failed to parse ProtocolMetadata: %w", err)
	}

	seq := pmd.Seq

	if seq == 0 {
		return fmt.Errorf("attempted to build a genesis inner block")
	}

	prevBlock, _, err := sm.GetBlock(seq - 1)
	if err != nil {
		return fmt.Errorf("failed to retrieve previous (%d) inner block: %w", seq - 1, err)
	}

	prevMD := prevBlock.Metadata
	currentState, err := sm.identifyCurrentState(prevMD.SimplexEpochInfo)
	if err != nil {
		return fmt.Errorf("failed to identify previous state: %w", err)
	}

	switch currentState {
	case stateFirstSimplexBlock:
		err = sm.verifyBlockZero(ctx, block, prevBlock)
	default:
		err = sm.verifyNonZeroBlock(block, prevBlock.Metadata, prevMD, currentState, seq-1)
	}
	return err
}

func (sm *StateMachine) maybeInit() {
	if sm.initialized {
		return
	}
	sm.init()
	sm.initialized = true
}

func (sm *StateMachine) init() {
	sm.verifiers = []verifier{
		&icmEpochInfoVerifier{
			computeICMEpoch: sm.ComputeICMEpoch,
			getUpdates:      sm.GetUpgrades,
		},
		&pChainHeightVerifier{
			getPChainHeight: sm.GetPChainHeight,
		},
		&timestampVerifier{},
		&pChainReferenceHeightVerifier{},
		&epochNumberVerifier{},
		&prevSealingBlockHashVerifier{
			getBlock:               sm.GetBlock,
			latestPersistedHeight:  sm.LatestPersistedHeight,
		},
		&nextPChainReferenceHeightVerifier{
			getPChainHeight: sm.GetPChainHeight,
			getValidatorSet: sm.GetValidatorSet,
		},
		&vmBlockSeqVerifier{
			getBlock: sm.GetBlock,
		},
		&validationDescriptorVerifier{
			getValidatorSet: sm.GetValidatorSet,
		},
		&nextEpochApprovalsVerifier{
			getValidatorSet: sm.GetValidatorSet,
			keyAggregator:   sm.KeyAggregator,
			sigVerifier:     sm.SignatureVerifier,
		},
		&sealingBlockSeqVerifier{},
	}
}

func (sm *StateMachine) verifyNonZeroBlock(block *StateMachineBlock, prevBlockMD StateMachineMetadata, prevMD StateMachineMetadata, state state, prevSeq uint64) error {
	blockType := sm.identifyBlockType(block, prevBlockMD)
	timestamp := time.Unix(int64(prevMD.Timestamp), 0)

	if block.InnerBlock != nil {
		timestamp = block.InnerBlock.Timestamp()
	}

	for _, verifier := range sm.verifiers {
		if err := verifier.Verify(verificationInput{
			proposedBlockMD:        block.Metadata,
			nextBlockType:          blockType,
			prevMD:                 prevMD,
			state:                  state,
			prevBlockSeq:           prevSeq,
			hasInnerBlock:          block.InnerBlock != nil,
			proposedBlockTimestamp: timestamp,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (sm *StateMachine) identifyCurrentState(simplexEpochInfo SimplexEpochInfo) (state, error) {
	// If this is the first ever epoch, then this is also the first ever block to be built by Simplex.
	if simplexEpochInfo.EpochNumber == 0 {
		return stateFirstSimplexBlock, nil
	}

	if simplexEpochInfo.NextPChainReferenceHeight == 0 {
		return stateBuildBlockNormalOp, nil
	}

	// Else, NextPChainReferenceHeight > 0, so we're either in stateBuildCollectingApprovals or stateBuildBlockEpochSealed
	if simplexEpochInfo.SealingBlockSeq == 0 {
		// If we don't have a sealing block sequence yet, we're still collecting approvals for the validator set change.
		return stateBuildCollectingApprovals, nil
	}

	// Otherwise, we do have a sealing block sequence, so the epoch has been sealed.
	return stateBuildBlockEpochSealed, nil
}

func (sm *StateMachine) buildBlockNormalOp(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata, simplexBlacklist []byte, prevBlockSeq uint64) (*StateMachineBlock, error) {
	pChainHeight := sm.GetPChainHeight()

	currentValidatorSet, err := sm.GetValidatorSet(parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight)
	if err != nil {
		return nil, err
	}

	newValidatorSet, err := sm.GetValidatorSet(pChainHeight)
	if err != nil {
		return nil, err
	}

	newSimplexEpochInfo := SimplexEpochInfo{
		PChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight,
		EpochNumber:           parentBlock.Metadata.SimplexEpochInfo.EpochNumber,
	}

	// If the validator set has changed, it's time to move to a new epoch.
	// We do this by setting NextPChainReferenceHeight to the new P-chain height
	// and building a block without waiting indefinitely.
	if !currentValidatorSet.Compare(newValidatorSet) {
		newSimplexEpochInfo.NextPChainReferenceHeight = pChainHeight
		return sm.buildBlockImpatiently(ctx, parentBlock, simplexMetadata, simplexBlacklist, newSimplexEpochInfo, pChainHeight, prevBlockSeq)
	}

	childBlock, err := sm.BlockBuilder.BuildBlock(ctx, parentBlock.Metadata.ICMEpochInfo.PChainEpochHeight)
	if err != nil {
		return nil, err
	}

	return sm.wrapBlock(parentBlock, childBlock, newSimplexEpochInfo, pChainHeight, simplexMetadata, simplexBlacklist, prevBlockSeq), nil
}

func (sm *StateMachine) identifyBlockType(block *StateMachineBlock, prevBlockMD StateMachineMetadata) blockType {
	simplexEpochInfo := block.Metadata.SimplexEpochInfo

	prevSimplexEpochInfo := prevBlockMD.SimplexEpochInfo

	// Only sealing blocks carry block validation descriptors
	if block.Metadata.SimplexEpochInfo.BlockValidationDescriptor != nil {
		return blockTypeSealing
	}

	// This block could be in the edges of an epoch, either at the end or at the beginning.

	// If the new block comes after a sealing block, then this block is a Telock.
	// [ Sealing Block ] <-- [ New Block ]
	if prevSimplexEpochInfo.BlockValidationDescriptor != nil {
		return blockTypeTelock
	}

	// Else, if the previous block has a sealing block sequence and is in the same epoch as this block,
	// then this block has to be a Telock.
	// [ Sealing Block ] <-- [ Prev block ] <-- [ New Block ]
	if simplexEpochInfo.EpochNumber == prevSimplexEpochInfo.EpochNumber && prevSimplexEpochInfo.SealingBlockSeq != 0 {
		return blockTypeTelock
	}

	// This block is the first block of its epoch if the epoch number is the sealing block sequence of the previous epoch
	if simplexEpochInfo.EpochNumber == prevSimplexEpochInfo.SealingBlockSeq {
		return blockTypeNewEpoch
	}

	// Otherwise, we do not fall into any of these cases, so it's probably a block in the middle of the epoch,
	// not in the edges.
	return blockTypeNormal
}

func (sm *StateMachine) buildBlockZeroEpoch(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata, simplexBlacklist []byte, prevBlockSeq uint64) (*StateMachineBlock, error) {
	pChainHeight := sm.GetPChainHeight()

	newValidatorSet, err := sm.GetValidatorSet(pChainHeight)
	if err != nil {
		return nil, err
	}

	var prevVMBlockSeq uint64
	if parentBlock.InnerBlock != nil {
		prevVMBlockSeq = parentBlock.InnerBlock.Height()
	}
	simplexEpochInfo := constructSimplexEpochInfoForZeroEpoch(pChainHeight, newValidatorSet, prevVMBlockSeq)

	return sm.buildBlockImpatiently(ctx, parentBlock, simplexMetadata, simplexBlacklist, simplexEpochInfo, pChainHeight, prevBlockSeq)
}

func (sm *StateMachine) verifyBlockZero(ctx context.Context, block *StateMachineBlock, prevBlock StateMachineBlock) error {
	if block == nil {
		return fmt.Errorf("block is nil")
	}

	simplexEpochInfo := block.Metadata.SimplexEpochInfo

	if simplexEpochInfo.EpochNumber != 1 {
		return fmt.Errorf("invalid epoch number (%d), should be 1", simplexEpochInfo.EpochNumber)
	}

	if prevBlock.InnerBlock == nil  {
		return fmt.Errorf("parent inner block (%s) has no inner block", prevBlock.Digest())
	}

	prevVMBlockSeq := prevBlock.InnerBlock.Height()

	currentPChainHeight := sm.GetPChainHeight()

	if block.Metadata.PChainHeight > currentPChainHeight {
		return fmt.Errorf("invalid P-chain height (%d) is too big, expected to be ≤ %d",
			block.Metadata.PChainHeight, currentPChainHeight)
	}

	if prevBlock.Metadata.PChainHeight > block.Metadata.PChainHeight {
		return fmt.Errorf("invalid P-chain height (%d) is smaller than parent InnerBlock's P-chain height (%d)",
			block.Metadata.PChainHeight, prevBlock.Metadata.PChainHeight)
	}

	expectedValidatorSet, err := sm.GetValidatorSet(simplexEpochInfo.PChainReferenceHeight)
	if err != nil {
		return fmt.Errorf("failed to retrieve validator set at height %d: %w", simplexEpochInfo.PChainReferenceHeight, err)
	}

	if simplexEpochInfo.BlockValidationDescriptor == nil {
		return fmt.Errorf("invalid BlockValidationDescriptor: should not be nil")
	}

	membership := simplexEpochInfo.BlockValidationDescriptor.AggregatedMembership.Members
	if !NodeBLSMappings(membership).Compare(expectedValidatorSet) {
		return fmt.Errorf("invalid BlockValidationDescriptor: should match validator set at P-chain height %d", simplexEpochInfo.PChainReferenceHeight)
	}

	// If we have compared all fields so far, the rest of the fields we compare by constructing an explicit expected SimplexEpochInfo
	expectedSimplexEpochInfo := constructSimplexEpochInfoForZeroEpoch(simplexEpochInfo.PChainReferenceHeight, expectedValidatorSet, prevVMBlockSeq)

	if ! expectedSimplexEpochInfo.Equal(&simplexEpochInfo) {
		return fmt.Errorf("invalid SimplexEpochInfo: expected %v, got %v", expectedSimplexEpochInfo, simplexEpochInfo)
	}

	if block.InnerBlock == nil {
		return nil
	}

	return block.InnerBlock.Verify(ctx)
}

func constructSimplexEpochInfoForZeroEpoch(pChainHeight uint64, newValidatorSet NodeBLSMappings, prevVMBlockSeq uint64) SimplexEpochInfo {
	newSimplexEpochInfo := SimplexEpochInfo{
		PChainReferenceHeight: pChainHeight,
		EpochNumber:           1,
		BlockValidationDescriptor: &BlockValidationDescriptor{
			AggregatedMembership: AggregatedMembership{
				Members: newValidatorSet,
			},
		},
		NextEpochApprovals:        nil, // We don't need to collect approvals to seal the zero epoch.
		PrevVMBlockSeq:            prevVMBlockSeq,
		SealingBlockSeq:           0,          // We don't have a sealing block in the zero epoch.
		PrevSealingBlockHash:      [32]byte{}, // The zero epoch has no previous sealing block.
		NextPChainReferenceHeight: 0,
	}
	return newSimplexEpochInfo
}

func (sm *StateMachine) buildBlockCollectingApprovals(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata, simplexBlacklist []byte, prevBlockSeq uint64) (*StateMachineBlock, error) {
	newSimplexEpochInfo := SimplexEpochInfo{
		PChainReferenceHeight:     parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight,
		EpochNumber:               parentBlock.Metadata.SimplexEpochInfo.EpochNumber,
		NextPChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight,
	}

	validators, err := sm.GetValidatorSet(parentBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight)
	if err != nil {
		return nil, err
	}
	approvalsFromPeers := sm.ApprovalsRetriever.RetrieveApprovals()
	auxInfo := parentBlock.Metadata.AuxiliaryInfo
	nextPChainHeight := parentBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight
	prevNextEpochApprovals := parentBlock.Metadata.SimplexEpochInfo.NextEpochApprovals
	newApprovals, err := computeNewApprovals(prevNextEpochApprovals, auxInfo, approvalsFromPeers, nextPChainHeight, sm.SignatureAggregator, validators)
	if err != nil {
		return nil, err
	}

	if !newApprovals.canSeal {
		pChainHeight := parentBlock.Metadata.PChainHeight
		return sm.buildBlockImpatiently(ctx, parentBlock, simplexMetadata, simplexBlacklist, newSimplexEpochInfo, pChainHeight, prevBlockSeq)
	}

	// Else, we create the sealing block.
	return sm.createSealingBlock(ctx, parentBlock, simplexMetadata, simplexBlacklist, newSimplexEpochInfo, newApprovals, nextPChainHeight, prevBlockSeq)
}

func (sm *StateMachine) buildBlockImpatiently(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata []byte, simplexBlacklist []byte, simplexEpochInfo SimplexEpochInfo, pChainHeight uint64, prevBlockSeq uint64) (*StateMachineBlock, error) {
	impatientContext, cancel := context.WithTimeout(ctx, sm.MaxBlockBuildingWaitTime)
	defer cancel()

	ctx = impatientContext

	childBlock, err := sm.BlockBuilder.BuildBlock(ctx, parentBlock.Metadata.ICMEpochInfo.PChainEpochHeight)
	if err != nil && ctx.Err() == nil {
		// If we got an error building the block, and we didn't time out, return the error.
		// We failed to build the block.
		return nil, err
	}
	// Else, either err == nil, and we've built the block,
	// or err != nil but ctx.Err() != nil and we have waited MaxBlockBuildingWaitTime,
	// so we need to build a block regardless of whether the inner VM wants to build a block.
	return sm.wrapBlock(parentBlock, childBlock, simplexEpochInfo, pChainHeight, simplexMetadata, simplexBlacklist, prevBlockSeq), nil
}

func (sm *StateMachine) createSealingBlock(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata []byte, simplexBlacklist []byte, simplexEpochInfo SimplexEpochInfo, newApprovals *approvals, pChainHeight uint64, prevBlockSeq uint64) (*StateMachineBlock, error) {
	// Update the approvals and signature in the simplex epoch info for the next block
	simplexEpochInfo.NextEpochApprovals.NodeIDs = newApprovals.nodeIDs
	simplexEpochInfo.NextEpochApprovals.Signature = newApprovals.signature

	// If this is the sealing block, set the sealing block sequence.
	md, err := simplex.ProtocolMetadataFromBytes(parentBlock.Metadata.SimplexProtocolMetadata)
	if err != nil {
		return nil, err
	}
	simplexEpochInfo.SealingBlockSeq = md.Seq + 1
	validators, err := sm.GetValidatorSet(simplexEpochInfo.NextPChainReferenceHeight)
	if err != nil {
		return nil, err
	}
	simplexEpochInfo.BlockValidationDescriptor.AggregatedMembership.Members = validators

	// If this is not the first epoch, and this is the sealing block, we set the hash of the previous sealing block.
	if newApprovals.canSeal && simplexEpochInfo.EpochNumber > 0 {
		prevSealingBlock, _, err := sm.GetBlock(simplexEpochInfo.EpochNumber)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve previous sealing InnerBlock at epoch %d: %w", simplexEpochInfo.EpochNumber-1, err)
		}
		simplexEpochInfo.PrevSealingBlockHash = prevSealingBlock.Digest()
	}

	return sm.buildBlockImpatiently(ctx, parentBlock, simplexMetadata, simplexBlacklist, simplexEpochInfo, pChainHeight, prevBlockSeq)
}

func computeNewApprovals(
	nextEpochApprovals *NextEpochApprovals,
	auxInfo *AuxiliaryInfo,
	newApprovals ValidatorSetApprovals,
	pChainHeight uint64,
	aggregator SignatureAggregator,
	validators NodeBLSMappings,
) (*approvals, error) {
	nodeID2ValidatorIndex := make(map[nodeID]int)
	validators.ForEach(func(i int, nbm NodeBLSMapping) {
		nodeID2ValidatorIndex[nbm.NodeID] = i
	})

	var candidateAuxInfoDigest [32]byte
	if auxInfo != nil {
		candidateAuxInfoDigest = sha256.Sum256(auxInfo.Info)
	}

	newApprovals = newApprovals.Filter(func(i int, approval ValidatorSetApproval) bool {
		// Pick only approvals that agree with our candidate auxiliary info digest and P-Chain height
		return approval.PChainHeight == pChainHeight && approval.AuxInfoSeqDigest == candidateAuxInfoDigest
	})

	if nextEpochApprovals == nil {
		nextEpochApprovals = &NextEpochApprovals{}
	}
	existingApprovingNodes := bitmaskFromBytes(nextEpochApprovals.NodeIDs)

	newApprovals = newApprovals.Filter(func(i int, approval ValidatorSetApproval) bool {
		approvingNodeIndexOfNewApprover := nodeID2ValidatorIndex[approval.NodeID]
		// Only pick approvals from nodes that haven't already approved
		return !existingApprovingNodes.Contains(approvingNodeIndexOfNewApprover)
	})

	newApprovingNodes := existingApprovingNodes

	// Prepare the new signatures from the new approvals that haven't approved yet and that agree with our candidate auxiliary info digest and P-Chain height.
	newSignatures := make([][]byte, 0, len(newApprovals)+1)

	newApprovals.ForEach(func(i int, approval ValidatorSetApproval) {
		approvingNodeIndexOfNewApprover := nodeID2ValidatorIndex[approval.NodeID]
		// Turn on the bit for the new approver
		newApprovingNodes.Add(approvingNodeIndexOfNewApprover)
		newSignatures = append(newSignatures, approval.Signature)
	})

	// Add the existing signature into the list of signatures to aggregate
	existingSignature := nextEpochApprovals.Signature
	newSignatures = append(newSignatures, existingSignature)

	aggregatedSignature, err := aggregator.AggregateSignatures(newSignatures...)
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate signatures: %w", err)
	}

	approvingWeight, err := computeApprovingWeight(validators, &newApprovingNodes)
	if err != nil {
		return nil, err
	}

	totalWeight, err := computeTotalWeight(validators)
	if err != nil {
		return nil, err
	}

	threshold := big.NewRat(2, 3)

	approvingRatio := big.NewRat(approvingWeight, totalWeight)

	canSeal := approvingRatio.Cmp(threshold) > 0

	return &approvals{
		canSeal:   canSeal,
		signature: aggregatedSignature,
		nodeIDs:   newApprovingNodes.Bytes(),
	}, nil
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

func (sm *StateMachine) buildBlockEpochSealed(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata, simplexBlacklist []byte, prevBlockSeq uint64) (*StateMachineBlock, error) {
	// We check if the sealing block has already been finalized.
	// If not, we build a Telock block.

	newSimplexEpochInfo := SimplexEpochInfo{
		PChainReferenceHeight:     parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight,
		EpochNumber:               parentBlock.Metadata.SimplexEpochInfo.EpochNumber,
		NextPChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight,
		SealingBlockSeq:           parentBlock.Metadata.SimplexEpochInfo.SealingBlockSeq,
	}

	// First, we find the sequence of the sealing block.
	seq := parentBlock.Metadata.SimplexEpochInfo.SealingBlockSeq

	// Do a sanity check just in case, make sure it's defined
	if seq == 0 {
		return nil, fmt.Errorf("cannot build epoch sealed inner block: sealing inner block sequence is 0 or undefined")
	}

	_, finalization, err := sm.GetBlock(seq)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve sealing inner block at sequence %d: %w", seq, err)
	}

	isSealingBlockFinalized := finalization != nil

	if !isSealingBlockFinalized {
		pChainHeight := parentBlock.Metadata.PChainHeight
		return sm.wrapBlock(parentBlock, nil, newSimplexEpochInfo, pChainHeight, simplexMetadata, simplexBlacklist, prevBlockSeq), nil
	}

	// Else, we build a block for the new epoch.
	newSimplexEpochInfo = SimplexEpochInfo{
		// P-chain reference height is previous block's NextPChainReferenceHeight.
		PChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight,
		// The epoch number is the sequence of the sealing block.
		EpochNumber: parentBlock.Metadata.SimplexEpochInfo.SealingBlockSeq,
	}

	childBlock, err := sm.BlockBuilder.BuildBlock(ctx, parentBlock.Metadata.ICMEpochInfo.PChainEpochHeight)
	if err != nil {
		return nil, err
	}

	return sm.wrapBlock(parentBlock, childBlock, newSimplexEpochInfo, parentBlock.Metadata.PChainHeight, simplexMetadata, simplexBlacklist, prevBlockSeq), nil
}

func computeICMEpochInfo(getUpgrades func() upgradeConfig, icmEpochTransition ICMEpochTransition, parentMetadata StateMachineMetadata, parentTimestamp, childTimestamp time.Time) ICMEpoch {
	upgrades := getUpgrades()

	icmEpoch := icmEpochTransition(upgrades, ICMEpochInput{
		ParentPChainHeight: parentMetadata.PChainHeight,
		ParentTimestamp:    parentTimestamp,
		ChildTimestamp:     childTimestamp,
		ParentEpoch: ICMEpoch{
			EpochStartTime:    parentMetadata.ICMEpochInfo.EpochStartTime,
			EpochNumber:       parentMetadata.ICMEpochInfo.EpochNumber,
			PChainEpochHeight: parentMetadata.ICMEpochInfo.PChainEpochHeight,
		},
	})
	return icmEpoch
}

func (sm *StateMachine) wrapBlock(parentBlock StateMachineBlock, childBlock VMBlock, newSimplexEpochInfo SimplexEpochInfo, pChainHeight uint64, simplexMetadata, simplexBlacklist []byte, prevBlockSeq uint64) *StateMachineBlock {
	parentMetadata := parentBlock.Metadata
	timestamp := parentMetadata.Timestamp

	hasChildBlock := childBlock != nil
	getUpgrades := sm.GetUpgrades
	icmEpochTransition := sm.ComputeICMEpoch

	var newTimestamp time.Time
	// nextICMEpoch returns the parent's ICM epoch info if hasChildBlock is false.
	if hasChildBlock {
		newTimestamp = childBlock.Timestamp()
		timestamp = uint64(newTimestamp.Unix())
	}

	icmEpochInfo := nextICMEpochInfo(parentMetadata, hasChildBlock, getUpgrades, icmEpochTransition, newTimestamp)

	if parentBlock.InnerBlock == nil {
		newSimplexEpochInfo.PrevVMBlockSeq = parentMetadata.SimplexEpochInfo.PrevVMBlockSeq
	} else {
		newSimplexEpochInfo.PrevVMBlockSeq = prevBlockSeq
	}

	return &StateMachineBlock{
		InnerBlock: childBlock,
		Metadata: StateMachineMetadata{
			Timestamp:               timestamp,
			SimplexProtocolMetadata: simplexMetadata,
			SimplexBlacklist:        simplexBlacklist,
			SimplexEpochInfo:        newSimplexEpochInfo,
			PChainHeight:            pChainHeight,
			ICMEpochInfo:            icmEpochInfo,
		},
	}
}

func nextICMEpochInfo(parentMetadata StateMachineMetadata, hasChildBlock bool, getUpgrades func() upgradeConfig, icmEpochTransition ICMEpochTransition, newTimestamp time.Time) ICMEpochInfo {
	icmEpochInfo := parentMetadata.ICMEpochInfo

	if hasChildBlock {
		parentTimestamp := time.Unix(int64(parentMetadata.Timestamp), 0)
		icmEpoch := computeICMEpochInfo(getUpgrades, icmEpochTransition, parentMetadata, parentTimestamp, newTimestamp)
		icmEpochInfo = ICMEpochInfo{
			EpochStartTime:    icmEpoch.EpochStartTime,
			EpochNumber:       icmEpoch.EpochNumber,
			PChainEpochHeight: icmEpoch.PChainEpochHeight,
		}
	}
	return icmEpochInfo
}

func findFirstSimplexBlock(getBlock BlockRetriever, endHeight uint64) (uint64, error) {
	firstSimplexBlock := sort.Search(int(endHeight+1), func(i int) bool {
		block, _, err := getBlock(uint64(i))
		if err != nil {
			return false
		}
		return block.Metadata.SimplexEpochInfo.EpochNumber > 0
	})

	if uint64(firstSimplexBlock) > endHeight {
		return 0, fmt.Errorf("no simplex blocks found in range [%d, %d]", 0, endHeight)
	}

	return uint64(firstSimplexBlock), nil
}

type approvals struct {
	canSeal   bool
	nodeIDs   []byte
	signature []byte
}

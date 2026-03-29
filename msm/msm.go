// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"time"

	"github.com/ava-labs/simplex"
)

// ICMEpochInput defines the input for computing the ICM Epoch information for the next block.
type ICMEpochInput struct {
	// ParentPChainHeight is the P-chain height recorded in the parent block.
	ParentPChainHeight uint64
	// ParentTimestamp is the timestamp of the parent block.
	ParentTimestamp time.Time
	// ChildTimestamp is the timestamp of the block being built.
	ChildTimestamp time.Time
	// ParentEpoch is the ICM epoch information from the parent block.
	ParentEpoch ICMEpoch
}

// ICMEpoch defines the ICM epoch information that is maintained by the StateMachine and used for the ICM protocol.
// The Statemachine maintains this information identically to how the proposerVM maintains this information,
// and it does so by building the ICMEpochInput and then passing it into the StateMachine's ComputeICMEpoch function.
type ICMEpoch struct {
	// EpochStartTime is the Unix timestamp when this ICM epoch started.
	EpochStartTime uint64
	// EpochNumber is the sequential identifier of this ICM epoch.
	EpochNumber uint64
	// PChainEpochHeight is the P-chain height associated with this ICM epoch.
	PChainEpochHeight uint64
}

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

// ICMEpochTransition computes the next ICM epoch given the current upgrade configuration and epoch input.
type ICMEpochTransition func(UpgradeConfig, ICMEpochInput) ICMEpoch

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

// BlockBuilder builds a new VM block with the given observed P-chain height.
type BlockBuilder interface {
	BuildBlock(ctx context.Context, pChainHeight uint64) (VMBlock, error)

	// WaitForPendingBlock returns when either the given context is cancelled,
	// or when the VM signals that a block should be built.
	WaitForPendingBlock(ctx context.Context)
}

// StateMachine manages block building and verification across epoch transitions.
type StateMachine struct {
	// LatestPersistedHeight is the height of the most recently persisted block.
	LatestPersistedHeight uint64
	// MaxBlockBuildingWaitTime is the maximum duration to wait for the VM to build a block
	// before producing a block without an inner block.
	MaxBlockBuildingWaitTime time.Duration
	// TimeSkewLimit is the maximum allowed time difference between a block's timestamp and the current time.
	TimeSkewLimit time.Duration
	// GetTime returns the current time.
	GetTime func() time.Time
	// ComputeICMEpoch computes the ICM epoch for the next block.
	ComputeICMEpoch ICMEpochTransition
	// GetPChainHeight returns the latest known P-chain height.
	GetPChainHeight func() uint64
	// GetUpgrades returns the current upgrade configuration.
	GetUpgrades func() UpgradeConfig
	// BlockBuilder builds new VM blocks.
	BlockBuilder BlockBuilder
	// Logger is used for logging state machine operations.
	Logger Logger
	// GetValidatorSet retrieves the validator set at a given P-chain height.
	GetValidatorSet ValidatorSetRetriever
	// GetBlock retrieves a previously built or finalized block.
	GetBlock BlockRetriever
	// ApprovalsRetriever retrieves validator approvals for epoch transitions.
	ApprovalsRetriever ApprovalsRetriever
	// SignatureAggregator aggregates signatures from validators.
	SignatureAggregator SignatureAggregator
	// KeyAggregator aggregates public keys from validators.
	KeyAggregator KeyAggregator
	// SignatureVerifier verifies signatures from validators.
	SignatureVerifier SignatureVerifier
	// PChainProgressListener listens for changes in the P-chain height to trigger block building or epoch transitions.
	PChainProgressListener PChainProgressListener

	// initialized tracks whether the state machine has been initialized.
	// This is used to lazily initialize the verifiers.
	initialized bool

	// verifiers is the list of verifiers used to verify proposed blocks.
	// Each verifier is responsible for verifying a specific aspect of the block's metadata.
	verifiers   []verifier
}

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

// BuildBlock constructs the next block on top of the given parent block, and passes in the provided simplex metadata and blacklist.
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
		return sm.buildBlockZeroEpoch(ctx, parentBlock, simplexMetadataBytes, simplexBlacklistBytes)
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

// VerifyBlock validates a proposed block by checking its metadata, epoch info,
// and inner block against the previous block and the current state.
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

	prevBlock, _, err := sm.GetBlock(RetrievingOpts{Digest: pmd.Prev, Height: seq - 1})
	if err != nil {
		return fmt.Errorf("failed to retrieve previous (%d) inner block: %w", seq-1, err)
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
		err = sm.verifyNonZeroBlock(ctx, block, prevBlock.Metadata, prevMD, currentState, seq-1)
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
		&timestampVerifier{
			timeSkewLimit: sm.TimeSkewLimit,
			getTime:       sm.GetTime,
		},
		&pChainReferenceHeightVerifier{},
		&epochNumberVerifier{},
		&prevSealingBlockHashVerifier{
			getBlock:              sm.GetBlock,
			latestPersistedHeight: &sm.LatestPersistedHeight,
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

func (sm *StateMachine) verifyNonZeroBlock(ctx context.Context, block *StateMachineBlock, prevBlockMD StateMachineMetadata, prevMD StateMachineMetadata, state state, prevSeq uint64) error {
	blockType := IdentifyBlockType(block.Metadata, prevBlockMD, prevSeq)
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

	if block.InnerBlock == nil {
		return nil
	}

	return block.InnerBlock.Verify(ctx)
}

func (sm *StateMachine) identifyCurrentState(simplexEpochInfo SimplexEpochInfo) (state, error) {
	// If this is the first ever epoch, then this is also the first ever block to be built by Simplex.
	if simplexEpochInfo.EpochNumber == 0 {
		return stateFirstSimplexBlock, nil
	}

	if simplexEpochInfo.NextPChainReferenceHeight == 0 {
		return stateBuildBlockNormalOp, nil
	}

	if simplexEpochInfo.SealingBlockSeq > 0 || simplexEpochInfo.BlockValidationDescriptor != nil {
		return stateBuildBlockEpochSealed, nil
	}

	return stateBuildCollectingApprovals, nil
}

func computePrevVMBlockSeq(parentBlock StateMachineBlock, prevBlockSeq uint64) uint64 {
	if parentBlock.InnerBlock == nil {
		return parentBlock.Metadata.SimplexEpochInfo.PrevVMBlockSeq
	}
	return prevBlockSeq
}

func (sm *StateMachine) buildBlockNormalOp(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata, simplexBlacklist []byte, prevBlockSeq uint64) (*StateMachineBlock, error) {
	newSimplexEpochInfo := SimplexEpochInfo{
		PChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight,
		EpochNumber:           parentBlock.Metadata.SimplexEpochInfo.EpochNumber,
		PrevVMBlockSeq:        computePrevVMBlockSeq(parentBlock, prevBlockSeq),
	}

	blockBuildingDecider := blockBuildingDecider{
		maxBlockBuildingWaitTime: sm.MaxBlockBuildingWaitTime,
		pChainlistener:           sm.PChainProgressListener,
		getPChainHeight:          sm.GetPChainHeight,
		waitForPendingBlock:      sm.BlockBuilder.WaitForPendingBlock,
		shouldTransitionEpoch: func() (bool, error) {
			pChainHeight := sm.GetPChainHeight()

			currentValidatorSet, err := sm.GetValidatorSet(parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight)
			if err != nil {
				return false, err
			}

			newValidatorSet, err := sm.GetValidatorSet(pChainHeight)
			if err != nil {
				return false, err
			}

			if !currentValidatorSet.Compare(newValidatorSet) {
				return true, nil
			}
			return false, nil
		},
	}

	decisionToBuildBlock, pChainHeight, err := blockBuildingDecider.shouldBuildBlock(ctx)
	if err != nil {
		return nil, err
	}

	var childBlock VMBlock

	switch decisionToBuildBlock {
	case blockBuildingDecisionBuildBlock, blockBuildingDecisionBuildBlockAndTransitionEpoch:
		return sm.buildBlockAndMaybeTransitionEpoch(ctx, parentBlock, simplexMetadata, simplexBlacklist, childBlock, decisionToBuildBlock, newSimplexEpochInfo, pChainHeight)
	case blockBuildingDecisionTransitionEpoch:
		newSimplexEpochInfo.NextPChainReferenceHeight = pChainHeight
		return sm.wrapBlock(parentBlock, nil, newSimplexEpochInfo, pChainHeight, simplexMetadata, simplexBlacklist), nil
	case blockBuildingDecisionContextCanceled:
		return nil, ctx.Err()
	default:
		return nil, fmt.Errorf("unknown block building decision %d", decisionToBuildBlock)
	}
}

func (sm *StateMachine) buildBlockAndMaybeTransitionEpoch(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata []byte, simplexBlacklist []byte, childBlock VMBlock, decisionToBuildBlock blockBuildingDecision, newSimplexEpochInfo SimplexEpochInfo, pChainHeight uint64) (*StateMachineBlock, error) {
	childBlock, err := sm.BlockBuilder.BuildBlock(ctx, parentBlock.Metadata.ICMEpochInfo.PChainEpochHeight)
	if err != nil {
		return nil, err
	}

	if decisionToBuildBlock == blockBuildingDecisionBuildBlockAndTransitionEpoch {
		newSimplexEpochInfo.NextPChainReferenceHeight = pChainHeight
	}

	return sm.wrapBlock(parentBlock, childBlock, newSimplexEpochInfo, pChainHeight, simplexMetadata, simplexBlacklist), nil
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
			return BlockTypeNewEpoch
		}
		return BlockTypeTelock
	}

	// Else, if the previous block has a sealing block sequence and is in the same epoch as this block,
	// then this block has to be a Telock.
	// [ Sealing Block ] <-- [ Prev block ] <-- [ New Block ]
	if simplexEpochInfo.EpochNumber == prevSimplexEpochInfo.EpochNumber && prevSimplexEpochInfo.SealingBlockSeq != 0 {
		return BlockTypeTelock
	}

	// This block is the first block of its epoch if the epoch number is the sealing block sequence of the previous epoch
	if simplexEpochInfo.EpochNumber == prevSimplexEpochInfo.SealingBlockSeq {
		return BlockTypeNewEpoch
	}

	// Otherwise, we do not fall into any of these cases, so it's probably a block in the middle of the epoch,
	// not in the edges.
	return BlockTypeNormal
}

func (sm *StateMachine) buildBlockZeroEpoch(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata, simplexBlacklist []byte) (*StateMachineBlock, error) {
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

	return sm.buildBlockImpatiently(ctx, parentBlock, simplexMetadata, simplexBlacklist, simplexEpochInfo, pChainHeight)
}

func (sm *StateMachine) verifyBlockZero(ctx context.Context, block *StateMachineBlock, prevBlock StateMachineBlock) error {
	if block == nil {
		return fmt.Errorf("block is nil")
	}

	simplexEpochInfo := block.Metadata.SimplexEpochInfo

	if simplexEpochInfo.EpochNumber != 1 {
		return fmt.Errorf("invalid epoch number (%d), should be 1", simplexEpochInfo.EpochNumber)
	}

	if prevBlock.InnerBlock == nil {
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

	if !expectedSimplexEpochInfo.Equal(&simplexEpochInfo) {
		return fmt.Errorf("invalid SimplexEpochInfo: expected %v, got %v", expectedSimplexEpochInfo, simplexEpochInfo)
	}

	proposedTime, err := sm.verifyZeroBlockTimestamp(block, prevBlock)
	if err != nil {
		return err
	}

	// Verify ICM epoch info
	expectedICMInfo := nextICMEpochInfo(prevBlock.Metadata, block.InnerBlock != nil, sm.GetUpgrades, sm.ComputeICMEpoch, proposedTime)
	if !expectedICMInfo.Equal(&block.Metadata.ICMEpochInfo) {
		return fmt.Errorf("expected ICM epoch info to be %v but got %v", expectedICMInfo, block.Metadata.ICMEpochInfo)
	}

	if block.InnerBlock == nil {
		return nil
	}

	return block.InnerBlock.Verify(ctx)
}

func (sm *StateMachine) verifyZeroBlockTimestamp(block *StateMachineBlock, prevBlock StateMachineBlock) (time.Time, error) {
	var proposedTime time.Time
	if block.InnerBlock != nil {
		proposedTime = block.InnerBlock.Timestamp()
	} else {
		proposedTime = time.Unix(int64(prevBlock.Metadata.Timestamp), 0)
	}

	expectedTimestamp := proposedTime.Unix()
	if expectedTimestamp != int64(block.Metadata.Timestamp) {
		return time.Time{}, fmt.Errorf("expected timestamp to be %d but got %d", expectedTimestamp, int64(block.Metadata.Timestamp))
	}
	currentTime := sm.GetTime()
	if currentTime.Add(sm.TimeSkewLimit).Before(proposedTime) {
		return time.Time{}, fmt.Errorf("proposed block timestamp is too far in the future, current time is %s but got %s", currentTime.String(), proposedTime.String())
	}
	if prevBlock.Metadata.Timestamp > block.Metadata.Timestamp {
		return time.Time{}, fmt.Errorf("proposed block timestamp is older than parent block's timestamp, parent timestamp is %d but got %d", prevBlock.Metadata.Timestamp, block.Metadata.Timestamp)
	}
	return proposedTime, nil
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
		PrevVMBlockSeq:            computePrevVMBlockSeq(parentBlock, prevBlockSeq),
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
		if newSimplexEpochInfo.NextEpochApprovals == nil {
			newSimplexEpochInfo.NextEpochApprovals = &NextEpochApprovals{}
		}
		newSimplexEpochInfo.NextEpochApprovals.NodeIDs = newApprovals.nodeIDs
		newSimplexEpochInfo.NextEpochApprovals.Signature = newApprovals.signature
		pChainHeight := parentBlock.Metadata.PChainHeight
		return sm.buildBlockImpatiently(ctx, parentBlock, simplexMetadata, simplexBlacklist, newSimplexEpochInfo, pChainHeight)
	}

	// Else, we create the sealing block.
	return sm.createSealingBlock(ctx, parentBlock, simplexMetadata, simplexBlacklist, newSimplexEpochInfo, newApprovals, nextPChainHeight)
}

func (sm *StateMachine) buildBlockImpatiently(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata []byte, simplexBlacklist []byte, simplexEpochInfo SimplexEpochInfo, pChainHeight uint64) (*StateMachineBlock, error) {
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
	return sm.wrapBlock(parentBlock, childBlock, simplexEpochInfo, pChainHeight, simplexMetadata, simplexBlacklist), nil
}

func (sm *StateMachine) createSealingBlock(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata []byte, simplexBlacklist []byte, simplexEpochInfo SimplexEpochInfo, newApprovals *approvals, pChainHeight uint64) (*StateMachineBlock, error) {
	// Update the approvals and signature in the simplex epoch info for the next block
	if simplexEpochInfo.NextEpochApprovals == nil {
		simplexEpochInfo.NextEpochApprovals = &NextEpochApprovals{}
	}
	simplexEpochInfo.NextEpochApprovals.NodeIDs = newApprovals.nodeIDs
	simplexEpochInfo.NextEpochApprovals.Signature = newApprovals.signature

	validators, err := sm.GetValidatorSet(simplexEpochInfo.NextPChainReferenceHeight)
	if err != nil {
		return nil, err
	}
	if simplexEpochInfo.BlockValidationDescriptor == nil {
		simplexEpochInfo.BlockValidationDescriptor = &BlockValidationDescriptor{}
	}
	simplexEpochInfo.BlockValidationDescriptor.AggregatedMembership.Members = validators

	// If this is not the first epoch, and this is the sealing block, we set the hash of the previous sealing block.
	if newApprovals.canSeal && simplexEpochInfo.EpochNumber > 1 {
		prevSealingBlock, _, err := sm.GetBlock(RetrievingOpts{Height: simplexEpochInfo.EpochNumber})
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve previous sealing InnerBlock at epoch %d: %w", simplexEpochInfo.EpochNumber-1, err)
		}
		simplexEpochInfo.PrevSealingBlockHash = prevSealingBlock.Digest()
	} else { // Else, this is the first epoch, so we use the hash of the first ever Simplex block.
		firstSimplexBlock, err := findFirstSimplexBlock(sm.GetBlock, sm.LatestPersistedHeight)
		if err != nil {
			return nil, fmt.Errorf("failed to find first simplex block: %w", err)
		}
		firstSimplexBlockRetrieved, _, err := sm.GetBlock(RetrievingOpts{Height: firstSimplexBlock})
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve first simplex block at height %d: %w", firstSimplexBlock, err)
		}
		simplexEpochInfo.PrevSealingBlockHash = firstSimplexBlockRetrieved.Digest()
	}

	return sm.buildBlockImpatiently(ctx, parentBlock, simplexMetadata, simplexBlacklist, simplexEpochInfo, pChainHeight)
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

	// If there are multiple approvals from the same node, we only keep one of them.
	newApprovals = newApprovals.UniqueByNodeID()

	if nextEpochApprovals == nil {
		nextEpochApprovals = &NextEpochApprovals{}
	}
	existingApprovingNodes := bitmaskFromBytes(nextEpochApprovals.NodeIDs)

	newApprovals = newApprovals.Filter(func(i int, approval ValidatorSetApproval) bool {
		approvingNodeIndexOfNewApprover, exists := nodeID2ValidatorIndex[approval.NodeID]
		if !exists {
			// If the approving node is not in the validator set, we ignore this approval.
			return false
		}
		// Only pick approvals from nodes that haven't already approved
		return !existingApprovingNodes.Contains(approvingNodeIndexOfNewApprover)
	})

	newApprovingNodes := existingApprovingNodes

	// Prepare the new signatures from the new approvals that haven't approved yet and that agree with our candidate auxiliary info digest and P-Chain height.
	newSignatures := make([][]byte, 0, len(newApprovals)+1)

	newApprovals.ForEach(func(i int, approval ValidatorSetApproval) {
		approvingNodeIndexOfNewApprover, exits := nodeID2ValidatorIndex[approval.NodeID]
		if !exits {
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

	sealingBlockSeq := parentBlock.Metadata.SimplexEpochInfo.SealingBlockSeq

	// If the sealing block sequence is still 0, it means previous block was the sealing block.
	if sealingBlockSeq == 0 {
		sealingBlockSeq = prevBlockSeq
	}

	if sealingBlockSeq == 0 {
		return nil, fmt.Errorf("cannot build epoch sealed block: sealing block sequence is 0 or undefined")
	}

	newSimplexEpochInfo := SimplexEpochInfo{
		PChainReferenceHeight:     parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight,
		EpochNumber:               parentBlock.Metadata.SimplexEpochInfo.EpochNumber,
		NextPChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight,
		SealingBlockSeq:          sealingBlockSeq,
		PrevVMBlockSeq:            computePrevVMBlockSeq(parentBlock, prevBlockSeq),
	}

	_, finalization, err := sm.GetBlock(RetrievingOpts{Height: sealingBlockSeq})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve sealing block at sequence %d: %w", sealingBlockSeq, err)
	}

	isSealingBlockFinalized := finalization != nil

	if !isSealingBlockFinalized {
		pChainHeight := parentBlock.Metadata.PChainHeight
		return sm.wrapBlock(parentBlock, nil, newSimplexEpochInfo, pChainHeight, simplexMetadata, simplexBlacklist), nil
	}

	// Else, we build a block for the new epoch.
	newSimplexEpochInfo = SimplexEpochInfo{
		// P-chain reference height is previous block's NextPChainReferenceHeight.
		PChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight,
		// The epoch number is the sequence of the sealing block.
		EpochNumber:    sealingBlockSeq,
		PrevVMBlockSeq: computePrevVMBlockSeq(parentBlock, prevBlockSeq),
	}

	childBlock, err := sm.BlockBuilder.BuildBlock(ctx, parentBlock.Metadata.ICMEpochInfo.PChainEpochHeight)
	if err != nil {
		return nil, err
	}

	return sm.wrapBlock(parentBlock, childBlock, newSimplexEpochInfo, parentBlock.Metadata.PChainHeight, simplexMetadata, simplexBlacklist), nil
}

func computeICMEpochInfo(getUpgrades func() UpgradeConfig, icmEpochTransition ICMEpochTransition, parentMetadata StateMachineMetadata, parentTimestamp, childTimestamp time.Time) ICMEpoch {
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

func (sm *StateMachine) wrapBlock(parentBlock StateMachineBlock, childBlock VMBlock, newSimplexEpochInfo SimplexEpochInfo, pChainHeight uint64, simplexMetadata, simplexBlacklist []byte) *StateMachineBlock {
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

func nextICMEpochInfo(parentMetadata StateMachineMetadata, hasChildBlock bool, getUpgrades func() UpgradeConfig, icmEpochTransition ICMEpochTransition, newTimestamp time.Time) ICMEpochInfo {
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
	var haltError error
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

type approvals struct {
	canSeal   bool
	nodeIDs   []byte
	signature []byte
}

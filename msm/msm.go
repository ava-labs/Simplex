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
	"go.uber.org/zap"
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
	// ShouldBeFinalized indicates whether the block being retrieved is expected to be finalized.
	// This optimizes retrieval by allowing the retriever to go directly to persisted storage.
	ShouldBeFinalized bool
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

	// The zero sequence number is reserved for the genesis block, which should never be built.
	if simplexMetadata.Seq == 0 {
		return nil, fmt.Errorf("invalid ProtocolMetadata sequence number: should be > 0, got %d", simplexMetadata.Seq)
	}

	start := time.Now()

	sm.Logger.Debug("Building block",
		zap.Uint64("seq", simplexMetadata.Seq),
		zap.Uint64("epoch", simplexMetadata.Epoch),
		zap.Stringer("prevHash", simplexMetadata.Prev))

	defer func() {
		elapsed := time.Since(start)
		sm.Logger.Debug("Built block",
			zap.Uint64("seq", simplexMetadata.Seq),
			zap.Uint64("epoch", simplexMetadata.Epoch),
			zap.Stringer("prevHash", simplexMetadata.Prev),
			zap.Duration("elapsed", elapsed),
		)
	}()

	var simplexBlacklistBytes []byte
	if simplexBlacklist != nil {
		simplexBlacklistBytes = simplexBlacklist.Bytes()
	}

	// In order to know where in the epoch change process we are,
	// we identify the current state by looking at the parent block's epoch info.
	currentState, err := sm.identifyCurrentState(parentBlock.Metadata.SimplexEpochInfo)
	if err != nil {
		return nil, err
	}

	simplexMetadataBytes := simplexMetadata.Bytes()
	prevBlockSeq := simplexMetadata.Seq - 1

	switch currentState {
	case stateFirstSimplexBlock:
		return sm.buildBlockZero(ctx, parentBlock, simplexMetadataBytes, simplexBlacklistBytes)
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
	timestamp := time.UnixMilli(int64(prevMD.Timestamp))

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

func (sm *StateMachine) identifyCurrentState(prevBlockSimplexEpochInfo SimplexEpochInfo) (state, error) {
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
	if prevBlockSimplexEpochInfo.SealingBlockSeq > 0 || prevBlockSimplexEpochInfo.BlockValidationDescriptor != nil {
		return stateBuildBlockEpochSealed, nil
	}

	// In any other case, NextPChainReferenceHeight > 0 but the previous block is not a Telock or sealing block,
	// it means we are in the process of collecting approvals for the next epoch.
	return stateBuildCollectingApprovals, nil
}

func computePrevVMBlockSeq(parentBlock StateMachineBlock, prevBlockSeq uint64) uint64 {
	// Either our parent block has no inner block, in which case we just inherit its previous VM block sequence,
	if parentBlock.InnerBlock == nil {
		return parentBlock.Metadata.SimplexEpochInfo.PrevVMBlockSeq
	}
	// or it has an inner block, in which case it is the previous block sequence.
	return prevBlockSeq
}

// buildBlockNormalOp builds a block while not trying to transition to a new epoch.
func (sm *StateMachine) buildBlockNormalOp(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata, simplexBlacklist []byte, prevBlockSeq uint64) (*StateMachineBlock, error) {
	// Since in the previous block, we were not transitioning to a new epoch,
	// the P-chain reference height and epoch of the new block should remain the same.
	newSimplexEpochInfo := SimplexEpochInfo{
		PChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight,
		EpochNumber:           parentBlock.Metadata.SimplexEpochInfo.EpochNumber,
		PrevVMBlockSeq:        computePrevVMBlockSeq(parentBlock, prevBlockSeq),
	}

	blockBuildingDecider := sm.createBlockBuildingDecider(parentBlock)
	decisionToBuildBlock, pChainHeight, err := blockBuildingDecider.shouldBuildBlock(ctx)
	if err != nil {
		return nil, err
	}

	sm.Logger.Debug("Block building decision", zap.Stringer("decision", decisionToBuildBlock))

	var childBlock VMBlock

	switch decisionToBuildBlock {
	case blockBuildingDecisionBuildBlock, blockBuildingDecisionBuildBlockAndTransitionEpoch:
		// If we reached here, we need to build a new block, and maybe also transition to a new epoch.
		return sm.buildBlockAndMaybeTransitionEpoch(ctx, parentBlock, simplexMetadata, simplexBlacklist, childBlock, decisionToBuildBlock, newSimplexEpochInfo, pChainHeight)
	case blockBuildingDecisionTransitionEpoch:
		// If we reached here, we don't need to build an inner block, yet we need to transition to a new epoch.
		// Initiate the epoch transition by setting the next P-chain reference height for the new epoch info,
		// and build a block without an inner block.
		newSimplexEpochInfo.NextPChainReferenceHeight = pChainHeight
		return sm.wrapBlock(parentBlock, nil, newSimplexEpochInfo, pChainHeight, simplexMetadata, simplexBlacklist), nil
	case blockBuildingDecisionContextCanceled:
		return nil, ctx.Err()
	default:
		return nil, fmt.Errorf("unknown block building decision %d", decisionToBuildBlock)
	}
}

func (sm *StateMachine) createBlockBuildingDecider(parentBlock StateMachineBlock) blockBuildingDecider {
	blockBuildingDecider := blockBuildingDecider{
		logger:                   sm.Logger,
		maxBlockBuildingWaitTime: sm.MaxBlockBuildingWaitTime,
		pChainlistener:           sm.PChainProgressListener,
		getPChainHeight:          sm.GetPChainHeight,
		waitForPendingBlock:      sm.BlockBuilder.WaitForPendingBlock,
		shouldTransitionEpoch: func(pChainHeight uint64) (bool, error) {
			// The given pChainHeight was sampled by the caller of shouldTransitionEpoch().
			// We compare between the current validator set, defined by the P-chain reference height in the parent block,
			// and the new validator set defined by the given pChainHeight.
			// If they are different, then we should transition to a new epoch.

			currentValidatorSet, err := sm.GetValidatorSet(parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight)
			if err != nil {
				return false, err
			}

			newValidatorSet, err := sm.GetValidatorSet(pChainHeight)
			if err != nil {
				return false, err
			}

			if !currentValidatorSet.Equal(newValidatorSet) {
				return true, nil
			}
			return false, nil
		},
	}
	return blockBuildingDecider
}

func (sm *StateMachine) buildBlockAndMaybeTransitionEpoch(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata []byte, simplexBlacklist []byte, childBlock VMBlock, decisionToBuildBlock blockBuildingDecision, newSimplexEpochInfo SimplexEpochInfo, pChainHeight uint64) (*StateMachineBlock, error) {
	childBlock, err := sm.BlockBuilder.BuildBlock(ctx, parentBlock.Metadata.ICMEpochInfo.PChainEpochHeight)
	if err != nil {
		return nil, err
	}

	if decisionToBuildBlock == blockBuildingDecisionBuildBlockAndTransitionEpoch {
		// We need to also transition to a new epoch, in addition to building an inner block,
		// so set the next P-chain reference height for the new epoch info.
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

// buildBlockZero builds the first ever block for Simplex,
// which is a special block that introduces the first validator set and starts the first epoch.
func (sm *StateMachine) buildBlockZero(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata, simplexBlacklist []byte) (*StateMachineBlock, error) {
	pChainHeight := sm.GetPChainHeight()

	newValidatorSet, err := sm.GetValidatorSet(pChainHeight)
	if err != nil {
		return nil, err
	}

	var prevVMBlockSeq uint64
	if parentBlock.InnerBlock != nil {
		prevVMBlockSeq = parentBlock.InnerBlock.Height()
	} else {
		// We can only have blocks without inner blocks in Simplex blocks, but this is the first Simplex block.
		// Therefore, the parent block must have an inner block.
		sm.Logger.Error("Parent block has no inner block, cannot determine previous VM block sequence for zero block",)
		return nil, fmt.Errorf("failed constructing zero block: parent block has no inner block")
	}
	simplexEpochInfo := constructSimplexZeroBlock(pChainHeight, newValidatorSet, prevVMBlockSeq)

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
	if !NodeBLSMappings(membership).Equal(expectedValidatorSet) {
		return fmt.Errorf("invalid BlockValidationDescriptor: should match validator set at P-chain height %d", simplexEpochInfo.PChainReferenceHeight)
	}

	// If we have compared all fields so far, the rest of the fields we compare by constructing an explicit expected SimplexEpochInfo
	expectedSimplexEpochInfo := constructSimplexZeroBlock(simplexEpochInfo.PChainReferenceHeight, expectedValidatorSet, prevVMBlockSeq)

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
		proposedTime = time.UnixMilli(int64(prevBlock.Metadata.Timestamp))
	}

	expectedTimestamp := proposedTime.UnixMilli()
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

// constructSimplexZeroBlock constructs the SimplexEpochInfo for the zero block, which is the first ever block built by Simplex.
func constructSimplexZeroBlock(pChainHeight uint64, newValidatorSet NodeBLSMappings, prevVMBlockSeq uint64) SimplexEpochInfo {
	newSimplexEpochInfo := SimplexEpochInfo{
		PChainReferenceHeight: pChainHeight,
		EpochNumber:           1,
		// We treat the zero block as a special case, and we encode in it the block validation descriptor,
		// despite it not actually being a sealing block. This is because the zero block is the first block that introduces the validator set.
		BlockValidationDescriptor: &BlockValidationDescriptor{
			AggregatedMembership: AggregatedMembership{
				Members: newValidatorSet,
			},
		},
		NextEpochApprovals:        nil, // We don't need to collect approvals to seal the first ever epoch.
		PrevVMBlockSeq:            prevVMBlockSeq,
		SealingBlockSeq:           0,          // We don't have a sealing block in the zero block.
		PrevSealingBlockHash:      [32]byte{}, // The zero block has no previous sealing block.
		NextPChainReferenceHeight: 0,
	}
	return newSimplexEpochInfo
}

func (sm *StateMachine) buildBlockCollectingApprovals(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata, simplexBlacklist []byte, prevBlockSeq uint64) (*StateMachineBlock, error) {
	// The P-chain reference height and epoch number should remain the same until we transition to the new epoch.
	// The next P-chain reference height should have been set in the previous block,
	// which is the reason why we are collecting approvals in the first place.
	newSimplexEpochInfo := SimplexEpochInfo{
		PChainReferenceHeight:     parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight,
		EpochNumber:               parentBlock.Metadata.SimplexEpochInfo.EpochNumber,
		NextPChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight,
		PrevVMBlockSeq:            computePrevVMBlockSeq(parentBlock, prevBlockSeq),
	}

	// We prepare information that is needed to compute the approvals for the new epoch,
	// such as the validator set for the next epoch, and the approvals from peers.
	validators, err := sm.GetValidatorSet(parentBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight)
	if err != nil {
		return nil, err
	}

	// We retrieve approvals that validators have sent us for the next epoch.
	// These approvals are signed by validators of the next epoch.
	approvalsFromPeers := sm.ApprovalsRetriever.RetrieveApprovals()
	auxInfo := parentBlock.Metadata.AuxiliaryInfo
	nextPChainHeight := newSimplexEpochInfo.NextPChainReferenceHeight
	prevNextEpochApprovals := parentBlock.Metadata.SimplexEpochInfo.NextEpochApprovals

	newApprovals, err := computeNewApprovals(prevNextEpochApprovals, auxInfo, approvalsFromPeers, nextPChainHeight, sm.SignatureAggregator, validators)
	if err != nil {
		return nil, err
	}

	// This might be the first time we created approvals for the next epoch,
	// so we need to initialize the NextEpochApprovals.
	if newSimplexEpochInfo.NextEpochApprovals == nil {
		newSimplexEpochInfo.NextEpochApprovals = &NextEpochApprovals{}
	}
	// The node IDs and signature are aggregated across all past and present approvals.
	newSimplexEpochInfo.NextEpochApprovals.NodeIDs = newApprovals.nodeIDs
	newSimplexEpochInfo.NextEpochApprovals.Signature = newApprovals.signature
	pChainHeight := parentBlock.Metadata.PChainHeight

	// We either don't have enough approvals to seal the current epoch,
	// in which case we just carry over the approvals we have so far to the next block,
	// so that eventually we'll have enough approvals to seal the epoch.

	if !newApprovals.canSeal {
		return sm.buildBlockImpatiently(ctx, parentBlock, simplexMetadata, simplexBlacklist, newSimplexEpochInfo, pChainHeight)
	}

	// Else, we have enough approvals to seal the epoch, so we create the sealing block.
	return sm.createSealingBlock(ctx, parentBlock, simplexMetadata, simplexBlacklist, newSimplexEpochInfo, newApprovals, pChainHeight)
}

// buildBlockImpatiently builds a block by waiting for the VM to build a block until MaxBlockBuildingWaitTime.
// If the VM fails to build a block within that time, we build a block without an inner block,
// so that we can continue making progress and not get stuck waiting for the VM.
func (sm *StateMachine) buildBlockImpatiently(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata []byte, simplexBlacklist []byte, simplexEpochInfo SimplexEpochInfo, pChainHeight uint64) (*StateMachineBlock, error) {
	impatientContext, cancel := context.WithTimeout(ctx, sm.MaxBlockBuildingWaitTime)
	defer cancel()

	start := time.Now()

	childBlock, err := sm.BlockBuilder.BuildBlock(impatientContext, parentBlock.Metadata.ICMEpochInfo.PChainEpochHeight)
	if err != nil && impatientContext.Err() == nil {
		// If we got an error building the block, and we didn't time out, log the error but continue building the block without the inner block,
		// so that we can continue making progress and not get stuck on a single block.
		sm.Logger.Error("Error building block, building block without inner block instead", zap.Error(err))
	}
	if impatientContext.Err() != nil {
		sm.Logger.Debug("Timed out waiting for block to be built, building block without inner block instead",
			zap.Duration("elapsed", time.Since(start)), zap.Duration("maxBlockBuildingWaitTime", sm.MaxBlockBuildingWaitTime))
	}
	return sm.wrapBlock(parentBlock, childBlock, simplexEpochInfo, pChainHeight, simplexMetadata, simplexBlacklist), nil
}

func (sm *StateMachine) createSealingBlock(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata []byte, simplexBlacklist []byte, simplexEpochInfo SimplexEpochInfo, newApprovals *approvals, pChainHeight uint64) (*StateMachineBlock, error) {
	validators, err := sm.GetValidatorSet(simplexEpochInfo.NextPChainReferenceHeight)
	if err != nil {
		return nil, err
	}
	if simplexEpochInfo.BlockValidationDescriptor == nil {
		simplexEpochInfo.BlockValidationDescriptor = &BlockValidationDescriptor{}
	}
	simplexEpochInfo.BlockValidationDescriptor.AggregatedMembership.Members = validators

	// If this is not the first epoch, and this is the sealing block, we set the hash of the previous sealing block.
	if simplexEpochInfo.EpochNumber > 1 {
		prevSealingBlock, _, err := sm.GetBlock(RetrievingOpts{Height: simplexEpochInfo.EpochNumber, ShouldBeFinalized: true})
		if err != nil {
			sm.Logger.Error("Error retrieving previous sealing block", zap.Uint64("seq", simplexEpochInfo.EpochNumber), zap.Error(err))
			return nil, fmt.Errorf("failed to retrieve previous sealing InnerBlock at epoch %d: %w", simplexEpochInfo.EpochNumber-1, err)
		}
		simplexEpochInfo.PrevSealingBlockHash = prevSealingBlock.Digest()
	} else { // Else, this is the first epoch, so we use the hash of the first ever Simplex block.

		firstSimplexBlock, err := findFirstSimplexBlock(sm.GetBlock, sm.LatestPersistedHeight+1)
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

// wrapBlock creates a new StateMachineBlock by wrapping the VM block (if applicable) and adding the appropriate metadata.
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
		timestamp = uint64(newTimestamp.UnixMilli())
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
		parentTimestamp := time.UnixMilli(int64(parentMetadata.Timestamp))
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

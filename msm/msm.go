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

// BlockRetriever retrieves the block at the given sequence number along with
// its finalization status. The digest is the expected hash of the block and
// can be used for validation; callers that don't care about digest validation
// may pass the zero value.
// If the block cannot be found it returns ErrBlockNotFound.
// If an error occurs during retrieval, it returns a non-nil error.
type BlockRetriever func(seq uint64, digest [32]byte) (StateMachineBlock, *simplex.Finalization, error)

// BlockBuilder builds a new VM block with the given observed P-chain height.
type BlockBuilder interface {
	BuildBlock(ctx context.Context, pChainHeight uint64) (VMBlock, error)

	// WaitForPendingBlock returns when either the given context is cancelled,
	// or when the VM signals that a block should be built.
	WaitForPendingBlock(ctx context.Context)
}

type Config struct {
	// LatestPersistedHeight is the height of the most recently persisted block.
	LatestPersistedHeight uint64
	// MaxBlockBuildingWaitTime is the maximum duration to wait for the VM to build a block
	// before producing a block without an inner block.
	MaxBlockBuildingWaitTime time.Duration
	// TimeSkewLimit is the maximum allowed time difference between a block's timestamp and the current time.
	TimeSkewLimit time.Duration
	// GetTime returns the current time.
	GetTime func() time.Time
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
}

// StateMachine manages block building and verification across epoch transitions.
type StateMachine struct {
	Config

	// verifiers is the list of verifiers used to verify proposed blocks.
	// Each verifier is responsible for verifying a specific aspect of the block's metadata.
	verifiers []verifier
}

// NewStateMachine builds a StateMachine from the given configuration and wires
// up its block verifiers. Callers should use this constructor rather than
// constructing a StateMachine literal directly.
//
// The verifiers read their dependencies through closures over the returned
// pointer, so later mutations of sm.GetBlock, sm.GetValidatorSet, and the
// other function-valued fields are visible to them.
func NewStateMachine(config Config) *StateMachine {
	out := &StateMachine{Config: config}

	getPChainHeight := func() uint64 { return out.GetPChainHeight() }
	getTime := func() time.Time { return out.GetTime() }
	getBlock := func(seq uint64, digest [32]byte) (StateMachineBlock, *simplex.Finalization, error) {
		return out.GetBlock(seq, digest)
	}
	getValidatorSet := func(height uint64) (NodeBLSMappings, error) {
		return out.GetValidatorSet(height)
	}

	out.verifiers = []verifier{
		&pChainHeightVerifier{
			getPChainHeight: getPChainHeight,
		},
		&timestampVerifier{
			timeSkewLimit: out.TimeSkewLimit,
			getTime:       getTime,
		},
		&pChainReferenceHeightVerifier{},
		&epochNumberVerifier{},
		&prevSealingBlockHashVerifier{
			getBlock:              getBlock,
			latestPersistedHeight: &out.LatestPersistedHeight,
		},
		&nextPChainReferenceHeightVerifier{
			getPChainHeight: getPChainHeight,
			getValidatorSet: getValidatorSet,
		},
		&vmBlockSeqVerifier{
			getBlock: getBlock,
		},
		&validationDescriptorVerifier{
			getValidatorSet: getValidatorSet,
		},
		&nextEpochApprovalsVerifier{
			getValidatorSet: getValidatorSet,
			keyAggregator:   out.KeyAggregator,
			sigVerifier:     out.SignatureVerifier,
		},
		&sealingBlockSeqVerifier{},
	}
	return out
}

type state uint8

const (
	stateFirstSimplexBlock state = iota + 1
	stateBuildBlockNormalOp
	stateBuildCollectingApprovals
	stateBuildBlockEpochSealed
)

// BuildBlock constructs the next block on top of the given parent block, and passes in the provided simplex metadata and blacklist.
func (sm *StateMachine) BuildBlock(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata simplex.ProtocolMetadata, blacklist simplex.Blacklist) (*StateMachineBlock, error) {
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

	blacklistBytes := blacklist.Bytes()

	// In order to know where in the epoch change process we are,
	// we identify the current state by looking at the parent block's epoch info.
	currentState := parentBlock.Metadata.SimplexEpochInfo.CurrentState()

	switch currentState {
	case stateFirstSimplexBlock:
		return sm.buildBlockZero(ctx, parentBlock, simplexMetadata, blacklistBytes)
	case stateBuildBlockNormalOp:
		return sm.buildBlockNormalOp(ctx, parentBlock, simplexMetadata, blacklistBytes)
	case stateBuildCollectingApprovals:
		return sm.buildBlockCollectingApprovals(ctx, parentBlock, simplexMetadata, blacklistBytes)
	case stateBuildBlockEpochSealed:
		return sm.buildBlockEpochSealed(ctx, parentBlock, simplexMetadata, blacklistBytes)
	default:
		return nil, fmt.Errorf("unknown state %d", currentState)
	}
}

// VerifyBlock validates a proposed block by checking its metadata, epoch info,
// and inner block against the previous block and the current state.
func (sm *StateMachine) VerifyBlock(ctx context.Context, block *StateMachineBlock) error {
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

	prevBlock, _, err := sm.GetBlock(seq-1, pmd.Prev)
	if err != nil {
		return fmt.Errorf("failed to retrieve previous (%d) inner block: %w", seq-1, err)
	}

	prevMD := prevBlock.Metadata
	currentState := prevMD.SimplexEpochInfo.CurrentState()

	switch currentState {
	case stateFirstSimplexBlock:
		err = sm.verifyBlockZero(ctx, block, prevBlock)
	default:
		err = sm.verifyNonZeroBlock(ctx, block, prevBlock.Metadata, currentState, pmd.Prev, seq-1)
	}
	return err
}

func (sm *StateMachine) verifyNonZeroBlock(ctx context.Context, block *StateMachineBlock, prevBlockMD StateMachineMetadata, state state, prevHash [32]byte, prevSeq uint64) error {
	blockType := IdentifyBlockType(block.Metadata, prevBlockMD, prevSeq)
	sm.Logger.Debug("Identified block type",
		zap.Stringer("blockType", blockType),
		zap.Bool("nextHasBVD", block.Metadata.SimplexEpochInfo.BlockValidationDescriptor != nil),
		zap.Uint64("nextEpochNumber", block.Metadata.SimplexEpochInfo.EpochNumber),
		zap.Bool("prevHasBVD", prevBlockMD.SimplexEpochInfo.BlockValidationDescriptor != nil),
		zap.Uint64("prevEpochNumber", prevBlockMD.SimplexEpochInfo.EpochNumber),
		zap.Uint64("prevNextPChainRefHeight", prevBlockMD.SimplexEpochInfo.NextPChainReferenceHeight),
		zap.Uint64("prevSealingBlockSeq", prevBlockMD.SimplexEpochInfo.SealingBlockSeq),
		zap.Uint64("prevSeq", prevSeq),
	)

	var innerBlockTimestamp time.Time
	if block.InnerBlock != nil {
		innerBlockTimestamp = block.InnerBlock.Timestamp()
	}

	for _, verifier := range sm.verifiers {
		if err := verifier.Verify(verificationInput{
			proposedBlockMD:     block.Metadata,
			nextBlockType:       blockType,
			prevMD:              prevBlockMD,
			state:               state,
			prevBlockSeq:        prevSeq,
			prevBlockHash:       prevHash,
			hasInnerBlock:       block.InnerBlock != nil,
			innerBlockTimestamp: innerBlockTimestamp,
		}); err != nil {
			sm.Logger.Debug("Invalid block", zap.Error(err))
			return err
		}
	}

	if block.InnerBlock == nil {
		return nil
	}

	return block.InnerBlock.Verify(ctx)
}

// CurrentState identifies, given the SimplexEpochInfo of the parent block, which
// state the state machine is in for the purpose of building the next block.
func (sei *SimplexEpochInfo) CurrentState() state {
	// If this is the first ever epoch, then this is also the first ever block to be built by Simplex.
	if sei.EpochNumber == 0 {
		return stateFirstSimplexBlock
	}

	// If we don't have a next P-chain preference height, it means we are not transitioning to a new epoch just yet.
	if sei.NextPChainReferenceHeight == 0 {
		return stateBuildBlockNormalOp
	}

	// If the previous block has a sealing block sequence, it's a Telock.
	// If it has a block validation descriptor, it's a sealing block.
	// Either way, the epoch has been sealed.
	if sei.SealingBlockSeq > 0 || sei.BlockValidationDescriptor != nil {
		return stateBuildBlockEpochSealed
	}

	// In any other case, NextPChainReferenceHeight > 0 but the previous block is not a Telock or sealing block,
	// it means we are in the process of collecting approvals for the next epoch.
	return stateBuildCollectingApprovals
}

// buildBlockNormalOp builds a block while not trying to transition to a new epoch.
func (sm *StateMachine) buildBlockNormalOp(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata simplex.ProtocolMetadata, blacklist []byte) (*StateMachineBlock, error) {
	// Since in the previous block, we were not transitioning to a new epoch,
	// the P-chain reference height and epoch of the new block should remain the same.
	newSimplexEpochInfo := SimplexEpochInfo{
		PChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight,
		EpochNumber:           parentBlock.Metadata.SimplexEpochInfo.EpochNumber,
		PrevVMBlockSeq:        computePrevVMBlockSeq(parentBlock, simplexMetadata.Seq-1),
	}

	blockBuildingDecider := newBlockBuildingDecider(sm, parentBlock)
	decisionToBuildBlock, pChainHeight, err := blockBuildingDecider.shouldBuildBlock(ctx)
	if err != nil {
		return nil, err
	}

	sm.Logger.Debug("Block building decision", zap.Stringer("decision", decisionToBuildBlock))

	switch decisionToBuildBlock {
	case blockBuildingDecisionBuildBlock, blockBuildingDecisionBuildBlockAndTransitionEpoch:
		// If we reached here, we need to build a new block, and maybe also transition to a new epoch.
		transitionEpoch := decisionToBuildBlock == blockBuildingDecisionBuildBlockAndTransitionEpoch
		return sm.buildBlockAndMaybeTransitionEpoch(ctx, parentBlock, simplexMetadata, blacklist, newSimplexEpochInfo, pChainHeight, transitionEpoch)
	case blockBuildingDecisionTransitionEpoch:
		// If we reached here, we don't need to build an inner block, yet we need to transition to a new epoch.
		// Initiate the epoch transition by setting the next P-chain reference height for the new epoch info,
		// and build a block without an inner block.
		newSimplexEpochInfo.NextPChainReferenceHeight = pChainHeight
		return sm.wrapBlock(parentBlock, nil, newSimplexEpochInfo, pChainHeight, simplexMetadata, blacklist), nil
	case blockBuildingDecisionContextCanceled:
		return nil, ctx.Err()
	default:
		return nil, fmt.Errorf("unknown block building decision %d", decisionToBuildBlock)
	}
}

func (sm *StateMachine) buildBlockAndMaybeTransitionEpoch(
	ctx context.Context,
	parentBlock StateMachineBlock,
	simplexMetadata simplex.ProtocolMetadata,
	blacklist []byte,
	newSimplexEpochInfo SimplexEpochInfo,
	pChainHeight uint64,
	transitionEpoch bool,
) (*StateMachineBlock, error) {
	// TODO: This P-chain height should be taken from the ICM epoch
	childBlock, err := sm.BlockBuilder.BuildBlock(ctx, pChainHeight)
	if err != nil {
		return nil, err
	}

	if transitionEpoch {
		// We need to also transition to a new epoch, in addition to building an inner block,
		// so set the next P-chain reference height for the new epoch info.
		newSimplexEpochInfo.NextPChainReferenceHeight = pChainHeight
	}

	return sm.wrapBlock(parentBlock, childBlock, newSimplexEpochInfo, pChainHeight, simplexMetadata, blacklist), nil
}

// buildBlockZero builds the first ever block for Simplex,
// which is a special block that introduces the first validator set and starts the first epoch.
func (sm *StateMachine) buildBlockZero(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata simplex.ProtocolMetadata, blacklist []byte) (*StateMachineBlock, error) {
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
		sm.Logger.Error("Parent block has no inner block, cannot determine previous VM block sequence for zero block")
		return nil, fmt.Errorf("failed constructing zero block: parent block has no inner block")
	}
	simplexEpochInfo := constructSimplexZeroBlock(pChainHeight, newValidatorSet, prevVMBlockSeq)

	return sm.buildBlockImpatiently(ctx, parentBlock, simplexMetadata, blacklist, simplexEpochInfo, pChainHeight)
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

	_, err = sm.verifyZeroBlockTimestamp(block, prevBlock)
	if err != nil {
		return err
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

func (sm *StateMachine) buildBlockCollectingApprovals(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata simplex.ProtocolMetadata, blacklist []byte) (*StateMachineBlock, error) {
	// The P-chain reference height and epoch number should remain the same until we transition to the new epoch.
	// The next P-chain reference height should have been set in the previous block,
	// which is the reason why we are collecting approvals in the first place.
	newSimplexEpochInfo := SimplexEpochInfo{
		PChainReferenceHeight:     parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight,
		EpochNumber:               parentBlock.Metadata.SimplexEpochInfo.EpochNumber,
		NextPChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight,
		PrevVMBlockSeq:            computePrevVMBlockSeq(parentBlock, simplexMetadata.Seq-1),
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
	nextPChainHeight := newSimplexEpochInfo.NextPChainReferenceHeight
	prevNextEpochApprovals := parentBlock.Metadata.SimplexEpochInfo.NextEpochApprovals

	newApprovals, err := computeNewApprovals(prevNextEpochApprovals, approvalsFromPeers, nextPChainHeight, sm.SignatureAggregator, validators)
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

	// We might not have enough approvals to seal the current epoch,
	// in which case we just carry over the approvals we have so far to the next block,
	// so that eventually we'll have enough approvals to seal the epoch.
	if !newApprovals.canSeal {
		return sm.buildBlockImpatiently(ctx, parentBlock, simplexMetadata, blacklist, newSimplexEpochInfo, pChainHeight)
	}

	// Else, we have enough approvals to seal the epoch, so we create the sealing block.
	return sm.createSealingBlock(ctx, parentBlock, simplexMetadata, blacklist, newSimplexEpochInfo, pChainHeight)
}

// buildBlockImpatiently builds a block by waiting for the VM to build a block until MaxBlockBuildingWaitTime.
// If the VM fails to build a block within that time, we build a block without an inner block,
// so that we can continue making progress and not get stuck waiting for the VM.
func (sm *StateMachine) buildBlockImpatiently(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata simplex.ProtocolMetadata, blacklist []byte, simplexEpochInfo SimplexEpochInfo, pChainHeight uint64) (*StateMachineBlock, error) {
	impatientContext, cancel := context.WithTimeout(ctx, sm.MaxBlockBuildingWaitTime)
	defer cancel()

	start := time.Now()

	// TODO: This P-chain height should be taken from the ICM epoch
	childBlock, err := sm.BlockBuilder.BuildBlock(impatientContext, pChainHeight)
	if err != nil && impatientContext.Err() == nil {
		// If we got an error building the block, and we didn't time out, log the error but continue building the block without the inner block,
		// so that we can continue making progress and not get stuck on a single block.
		sm.Logger.Error("Error building block, building block without inner block instead", zap.Error(err))
	}
	if impatientContext.Err() != nil {
		sm.Logger.Debug("Timed out waiting for block to be built, building block without inner block instead",
			zap.Duration("elapsed", time.Since(start)), zap.Duration("maxBlockBuildingWaitTime", sm.MaxBlockBuildingWaitTime))
	}
	return sm.wrapBlock(parentBlock, childBlock, simplexEpochInfo, pChainHeight, simplexMetadata, blacklist), nil
}

func (sm *StateMachine) createSealingBlock(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata simplex.ProtocolMetadata, blacklist []byte, simplexEpochInfo SimplexEpochInfo, pChainHeight uint64) (*StateMachineBlock, error) {
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
		prevSealingBlock, finalization, err := sm.GetBlock(simplexEpochInfo.EpochNumber, [32]byte{})
		if err != nil {
			sm.Logger.Error("Error retrieving previous sealing block", zap.Uint64("seq", simplexEpochInfo.EpochNumber), zap.Error(err))
			return nil, fmt.Errorf("failed to retrieve previous sealing InnerBlock at epoch %d: %w", simplexEpochInfo.EpochNumber-1, err)
		}
		if finalization == nil {
			sm.Logger.Error("Previous sealing block is not finalized", zap.Uint64("seq", simplexEpochInfo.EpochNumber))
			return nil, fmt.Errorf("previous sealing InnerBlock at epoch %d is not finalized", simplexEpochInfo.EpochNumber-1)
		}
		simplexEpochInfo.PrevSealingBlockHash = prevSealingBlock.Digest()
	} else { // Else, this is the first epoch, so we use the hash of the first ever Simplex block.

		firstSimplexBlock, err := findFirstSimplexBlock(sm.GetBlock, sm.LatestPersistedHeight+1)
		if err != nil {
			return nil, fmt.Errorf("failed to find first simplex block: %w", err)
		}
		firstSimplexBlockRetrieved, _, err := sm.GetBlock(firstSimplexBlock, [32]byte{})
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve first simplex block at height %d: %w", firstSimplexBlock, err)
		}
		simplexEpochInfo.PrevSealingBlockHash = firstSimplexBlockRetrieved.Digest()
	}

	return sm.buildBlockImpatiently(ctx, parentBlock, simplexMetadata, blacklist, simplexEpochInfo, pChainHeight)
}

// wrapBlock creates a new StateMachineBlock by wrapping the VM block (if applicable) and adding the appropriate metadata.
func (sm *StateMachine) wrapBlock(parentBlock StateMachineBlock, childBlock VMBlock, newSimplexEpochInfo SimplexEpochInfo, pChainHeight uint64, simplexMetadata simplex.ProtocolMetadata, blacklist []byte) *StateMachineBlock {
	parentMetadata := parentBlock.Metadata
	timestamp := parentMetadata.Timestamp

	hasChildBlock := childBlock != nil

	var newTimestamp time.Time
	if hasChildBlock {
		newTimestamp = childBlock.Timestamp()
		timestamp = uint64(newTimestamp.UnixMilli())
	}

	return &StateMachineBlock{
		InnerBlock: childBlock,
		Metadata: StateMachineMetadata{
			Timestamp:               timestamp,
			SimplexProtocolMetadata: simplexMetadata.Bytes(),
			SimplexBlacklist:        blacklist,
			SimplexEpochInfo:        newSimplexEpochInfo,
			PChainHeight:            pChainHeight,
		},
	}
}

// buildBlockEpochSealed builds a block where the epoch is being sealed due to a sealing block already created in this epoch.
func (sm *StateMachine) buildBlockEpochSealed(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata simplex.ProtocolMetadata, blacklist []byte) (*StateMachineBlock, error) {
	// We check if the sealing block has already been finalized.
	// If not, we build a Telock block.

	prevBlockSeq := simplexMetadata.Seq - 1
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
		SealingBlockSeq:           sealingBlockSeq,
		PrevVMBlockSeq:            computePrevVMBlockSeq(parentBlock, prevBlockSeq),
	}

	_, finalization, err := sm.GetBlock(sealingBlockSeq, [32]byte{})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve sealing block at sequence %d: %w", sealingBlockSeq, err)
	}

	isSealingBlockFinalized := finalization != nil

	if !isSealingBlockFinalized {
		pChainHeight := parentBlock.Metadata.PChainHeight
		return sm.wrapBlock(parentBlock, nil, newSimplexEpochInfo, pChainHeight, simplexMetadata, blacklist), nil
	}

	// Else, we build a block for the new epoch.
	newSimplexEpochInfo = SimplexEpochInfo{
		// P-chain reference height is previous block's NextPChainReferenceHeight.
		PChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight,
		// The epoch number is the sequence of the sealing block.
		EpochNumber:    sealingBlockSeq,
		PrevVMBlockSeq: computePrevVMBlockSeq(parentBlock, prevBlockSeq),
	}

	// TODO: This P-chain height should be taken from the ICM epoch
	childBlock, err := sm.BlockBuilder.BuildBlock(ctx, sm.GetPChainHeight())
	if err != nil {
		return nil, err
	}

	return sm.wrapBlock(parentBlock, childBlock, newSimplexEpochInfo, parentBlock.Metadata.PChainHeight, simplexMetadata, blacklist), nil
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

func computeNewApprovals(
	nextEpochApprovals *NextEpochApprovals,
	approvalsFromPeers ValidatorSetApprovals,
	pChainHeight uint64,
	aggregator SignatureAggregator,
	validators NodeBLSMappings,
) (*approvals, error) {
	if nextEpochApprovals == nil {
		nextEpochApprovals = &NextEpochApprovals{}
	}

	oldApprovingNodes := bitmaskFromBytes(nextEpochApprovals.NodeIDs)

	// We map each validator to its relative index in the validator set.
	nodeID2ValidatorIndex := make(map[nodeID]int, len(validators))
	for i, nbm := range validators {
		nodeID2ValidatorIndex[nbm.NodeID] = i
	}

	// We have the approvals obtained from peers, but we need to sanitize them by filtering out approvals that are not valid,
	// such as approvals that do not agree with our candidate auxiliary info digest and P-Chain height,
	// and approvals that are from nodes that are not in the validator set or have already approved in prior blocks.
	approvalsFromPeers = sanitizeApprovals(approvalsFromPeers, pChainHeight, nodeID2ValidatorIndex, oldApprovingNodes)

	// Next we aggregate both previous and new approvals to compute the new aggregated signatures and the new bitmask of approving nodes.
	aggregatedSignature, newApprovingNodes, err := computeNewApproverSignaturesAndSigners(nextEpochApprovals, approvalsFromPeers, oldApprovingNodes, nodeID2ValidatorIndex, aggregator)
	if err != nil {
		return nil, err
	}

	// we check if we have enough approvals to seal the epoch by computing the relative approval ratio,
	// which is the ratio of the total weight of approving nodes divided by the total weight of all validators.
	canSeal, err := canSealBlock(validators, newApprovingNodes)
	if err != nil {
		return nil, err
	}

	return &approvals{
		canSeal:   canSeal,
		signature: aggregatedSignature,
		nodeIDs:   newApprovingNodes.Bytes(),
	}, nil
}

// computeNewApproverSignaturesAndSigners computes the signatures of the nodes that approve the next epoch including the previous aggregated signature,
// and bitmask of nodes that correspond to those signatures, and aggregates all signatures together.
func computeNewApproverSignaturesAndSigners(nextEpochApprovals *NextEpochApprovals, approvalsFromPeers ValidatorSetApprovals, oldApprovingNodes bitmask, nodeID2ValidatorIndex map[nodeID]int, aggregator SignatureAggregator) ([]byte, bitmask, error) {
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

	return aggregatedSignature, *newApprovingNodes, nil
}

func canSealBlock(validators NodeBLSMappings, newApprovingNodes bitmask) (bool, error) {
	approvingWeight, err := computeApprovingWeight(validators, &newApprovingNodes)
	if err != nil {
		return false, err
	}

	totalWeight, err := computeTotalWeight(validators)
	if err != nil {
		return false, err
	}

	threshold := big.NewRat(2, 3)

	approvingRatio := big.NewRat(approvingWeight, totalWeight)

	canSeal := approvingRatio.Cmp(threshold) > 0
	return canSeal, nil
}

// sanitizeApprovals filters out approvals that are not valid by checking if they agree with our candidate auxiliary info digest and P-Chain height,
// and if they are from the validator set and haven't already been approved.
func sanitizeApprovals(approvals ValidatorSetApprovals, pChainHeight uint64, nodeID2ValidatorIndex map[nodeID]int, oldApprovingNodes bitmask) ValidatorSetApprovals {
	filter1 := approvalsThatAgreeWithAuxInfoAndPChainHeight(pChainHeight)
	filter2 := approvalsThatAreInValidatorSetAndHaveNotAlreadyApproved(oldApprovingNodes.Clone(), nodeID2ValidatorIndex)
	return approvals.Filter(filter1).Filter(filter2).UniqueByNodeID()
}

func approvalsThatAgreeWithAuxInfoAndPChainHeight(pChainHeight uint64) func(approval ValidatorSetApproval) bool {
	return func(approval ValidatorSetApproval) bool {
		// Pick only approvals that agree with our candidate auxiliary info digest and P-Chain height
		return approval.PChainHeight == pChainHeight
	}
}

func approvalsThatAreInValidatorSetAndHaveNotAlreadyApproved(oldApprovingNodes *bitmask, nodeID2ValidatorIndex map[nodeID]int) func(approval ValidatorSetApproval) bool {
	return func(approval ValidatorSetApproval) bool {
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
	for i, nbm := range validators {
		if !approvingNodes.Contains(i) {
			continue
		}
		sum, err := safeAdd(approvingWeight, nbm.Weight)
		if err != nil {
			return 0, fmt.Errorf("failed to compute approving weights: %w", err)
		}
		approvingWeight = sum
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
		block, _, err := getBlock(uint64(i), [32]byte{})
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
	// Either our parent block has no inner block, in which case we just inherit its previous VM block sequence.
	if parentBlock.InnerBlock == nil {
		return parentBlock.Metadata.SimplexEpochInfo.PrevVMBlockSeq
	}
	// Otherwise, it has an inner block, in which case it is the previous block sequence.
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

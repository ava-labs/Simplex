// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"context"
	"crypto/sha256"
	"fmt"
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

// KeyAggregator combines multiple public keys into a single aggregated public key.
type KeyAggregator interface {
	AggregateKeys(keys ...[]byte) ([]byte, error)
}

// SignatureVerifier verifies a cryptographic signature against a message and public key.
// Used to verify Approvals from validators for epoch transitions.
type SignatureVerifier interface {
	VerifySignature(signature []byte, message []byte, publicKey []byte) error
}

// ValidatorSetRetriever retrieves the validator set at a given P-chain height.
type ValidatorSetRetriever func(pChainHeight uint64) (NodeBLSMappings, error)

// BlockRetriever retrieves a block and its finalization status given the block's sequence number and expected digest.
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

// StateMachine manages block building and verification across epoch transitions.
type StateMachine struct {
	// verifiers is the list of verifiers used to verify proposed blocks.
	// Each verifier is responsible for verifying a specific aspect of the block's metadata.
	verifiers []verifier

	*Config
}

// Config contains the dependencies and configuration parameters needed to initialize the StateMachine.
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
	Logger simplex.Logger
	// GetValidatorSet retrieves the validator set at a given P-chain height.
	GetValidatorSet ValidatorSetRetriever
	// GetBlock retrieves a previously built or finalized block.
	GetBlock BlockRetriever
	// ApprovalsRetriever retrieves validator approvals for epoch transitions.
	ApprovalsRetriever ApprovalsRetriever
	// SignatureAggregatorCreator creates a new SignatureAggregator for aggregating validator signatures for epoch transitions.
	SignatureAggregatorCreator simplex.SignatureAggregatorCreator
	// KeyAggregator aggregates public keys from validators.
	KeyAggregator KeyAggregator
	// SignatureVerifier verifies signatures from validators.
	SignatureVerifier SignatureVerifier
	// PChainProgressListener listens for changes in the P-chain height to trigger block building or epoch transitions.
	PChainProgressListener PChainProgressListener
	// FirstEverSimplexBlock is the first block ever built by Simplex, or nil if Simplex has yet to build a block.
	FirstEverSimplexBlock func() *StateMachineBlock
	// LastNonSimplexBlockPChainHeight is the P-chain height of the last block built by a non-Simplex proposer.
	// It is used to determine the validator set of the first ever Simplex epoch.
	LastNonSimplexBlockPChainHeight uint64
	// LastNonSimplexInnerBlock is the inner block of the last block built by a non-Simplex proposer.
	LastNonSimplexInnerBlock VMBlock
	// GenesisValidatorSet is the validator set used for the genesis block.
	GenesisValidatorSet NodeBLSMappings
}

type state uint8

const (
	stateFirstSimplexBlock state = iota + 1
	stateBuildBlockNormalOp
	stateBuildCollectingApprovals
	stateBuildBlockEpochSealed
)

func NewStateMachine(config *Config) *StateMachine {
	sm := StateMachine{Config: config}
	sm.init()
	return &sm
}

// BuildBlock constructs the next block on top of the given parent block, and passes in the provided simplex metadata and blacklist.
func (sm *StateMachine) BuildBlock(ctx context.Context, simplexMetadata simplex.ProtocolMetadata, simplexBlacklist *simplex.Blacklist) (*StateMachineBlock, error) {
	// The zero sequence number is reserved for the genesis block, which should never be built.
	if simplexMetadata.Seq == 0 {
		return nil, fmt.Errorf("invalid ProtocolMetadata sequence number: should be > 0, got %d", simplexMetadata.Seq)
	}

	parentBlock, _, err := sm.GetBlock(simplexMetadata.Seq-1, simplexMetadata.Prev)
	if err != nil {
		return nil, fmt.Errorf("failed retrieving parent block at height %d with digest %s: %w", simplexMetadata.Seq-1, simplexMetadata.Prev.String(), err)
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
	currentState := parentBlock.Metadata.SimplexEpochInfo.NextState()

	simplexMetadataBytes := simplexMetadata.Bytes()
	prevBlockSeq := simplexMetadata.Seq - 1

	switch currentState {
	case stateFirstSimplexBlock:
		return sm.buildBlockZero(parentBlock, simplexMetadataBytes, simplexBlacklistBytes)
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
	currentState := prevMD.SimplexEpochInfo.NextState()

	switch currentState {
	case stateFirstSimplexBlock:
		err = sm.verifyBlockZero(block, prevBlock)
	default:
		err = sm.verifyNonZeroBlock(ctx, block, prevBlock.Metadata, currentState, seq-1)
	}
	return err
}

func (sm *StateMachine) init() {
	sm.verifiers = []verifier{
		&pChainHeightVerifier{
			getPChainHeight: func() uint64 {
				return sm.Config.GetPChainHeight()
			},
		},
		&timestampVerifier{
			timeSkewLimit: sm.TimeSkewLimit,
			getTime: func() time.Time {
				return sm.Config.GetTime()
			},
		},
		&pChainReferenceHeightVerifier{},
		&epochNumberVerifier{},
		&validationDescriptorVerifier{
			getValidatorSet: func(pChainHeight uint64) (NodeBLSMappings, error) {
				return sm.Config.GetValidatorSet(pChainHeight)
			},
		},
		&prevSealingBlockHashVerifier{
			firstEverSimplexBlock: func() *StateMachineBlock {
				return sm.Config.FirstEverSimplexBlock()
			},
			getBlock: func(seq uint64, digest [32]byte) (StateMachineBlock, *simplex.Finalization, error) {
				return sm.Config.GetBlock(seq, digest)
			},
			latestPersistedHeight: &sm.LatestPersistedHeight,
		},
		&nextPChainReferenceHeightVerifier{
			getPChainHeight: func() uint64 {
				return sm.Config.GetPChainHeight()
			},
			getValidatorSet: func(pChainHeight uint64) (NodeBLSMappings, error) {
				return sm.Config.GetValidatorSet(pChainHeight)
			},
		},
		&vmBlockSeqVerifier{
			getBlock: func(seq uint64, digest [32]byte) (StateMachineBlock, *simplex.Finalization, error) {
				return sm.Config.GetBlock(seq, digest)
			},
		},
		&nextEpochApprovalsVerifier{
			getValidatorSet: func(pChainHeight uint64) (NodeBLSMappings, error) {
				return sm.Config.GetValidatorSet(pChainHeight)
			},
			keyAggregator: sm.KeyAggregator,
			sigVerifier:   sm.SignatureVerifier,
			sigAggregatorCreator: func(weights []simplex.NodeWeight) simplex.SignatureAggregator {
				return sm.Config.SignatureAggregatorCreator(weights)
			},
		},
		&sealingBlockSeqVerifier{},
	}
}

func (sm *StateMachine) verifyNonZeroBlock(ctx context.Context, block *StateMachineBlock, prevBlockMD StateMachineMetadata, state state, prevSeq uint64) error {
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
	case decisionBuild, decisionBuildAndTransitionEpoch:
		// If we reached here, we need to build a new block, and maybe also transition to a new epoch.
		return sm.buildBlockAndMaybeTransitionEpoch(ctx, parentBlock, simplexMetadata, simplexBlacklist, childBlock, decisionToBuildBlock, newSimplexEpochInfo, pChainHeight)
	case decisionTransitionEpoch:
		// If we reached here, we don't need to build an inner block, yet we need to transition to a new epoch.
		// Initiate the epoch transition by setting the next P-chain reference height for the new epoch info,
		// and build a block without an inner block.
		newSimplexEpochInfo.NextPChainReferenceHeight = pChainHeight
		sm.Logger.Debug("Transitioning epoch without building block", zap.Uint64("newPChainRefHeight", pChainHeight))
		return sm.wrapBlock(parentBlock, nil, newSimplexEpochInfo, pChainHeight, simplexMetadata, simplexBlacklist), nil
	case decisionContextCanceled:
		return nil, ctx.Err()
	default:
		return nil, fmt.Errorf("unknown block building decision %d", decisionToBuildBlock)
	}
}

func (sm *StateMachine) createBlockBuildingDecider(parentBlock StateMachineBlock) blockBuildingDecider {
	blockBuildingDecider := blockBuildingDecider{
		logger:                   sm.Logger,
		maxBlockBuildingWaitTime: sm.MaxBlockBuildingWaitTime,
		pChainListener:           sm.PChainProgressListener,
		getPChainHeight:          sm.GetPChainHeight,
		waitForPendingBlock:      sm.BlockBuilder.WaitForPendingBlock,
		hasValidatorSetChanged: func(pChainHeight uint64) (bool, error) {
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
				sm.Logger.Debug("Validator set has changed, should transition epoch",
					zap.String("currentValidatorSet", fmt.Sprintf("%v", currentValidatorSet.NodeWeights())),
					zap.String("newValidatorSet", fmt.Sprintf("%v", newValidatorSet.NodeWeights())),
					zap.Uint64("currentPChainRefHeight", parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight),
					zap.Uint64("newPChainHeight", pChainHeight))
				return true, nil
			}
			return false, nil
		},
	}
	return blockBuildingDecider
}

func (sm *StateMachine) buildBlockAndMaybeTransitionEpoch(ctx context.Context,
	parentBlock StateMachineBlock,
	simplexMetadata []byte,
	simplexBlacklist []byte,
	childBlock VMBlock,
	decisionToBuildBlock blockBuildingDecision,
	newSimplexEpochInfo SimplexEpochInfo,
	pChainHeight uint64) (*StateMachineBlock, error) {
	// TODO: This P-chain height should be taken from the ICM epoch
	childBlock, err := sm.BlockBuilder.BuildBlock(ctx, pChainHeight)
	if err != nil {
		return nil, err
	}

	if decisionToBuildBlock == decisionBuildAndTransitionEpoch {
		// We need to also transition to a new epoch, in addition to building an inner block,
		// so set the next P-chain reference height for the new epoch info.
		newSimplexEpochInfo.NextPChainReferenceHeight = pChainHeight
		sm.Logger.Debug("Transitioning epoch after building block", zap.Uint64("newPChainRefHeight", pChainHeight))
	}

	return sm.wrapBlock(parentBlock, childBlock, newSimplexEpochInfo, pChainHeight, simplexMetadata, simplexBlacklist), nil
}

// buildBlockZero builds the first ever block for Simplex,
// which is a special block that introduces the first validator set and starts the first epoch.
func (sm *StateMachine) buildBlockZero(parentBlock StateMachineBlock, simplexMetadata, simplexBlacklist []byte) (*StateMachineBlock, error) {
	if sm.LastNonSimplexInnerBlock == nil {
		sm.Logger.Error("Last non-Simplex inner block is nil, cannot build zero block with correct metadata")
		return nil, fmt.Errorf("failed constructing zero block: last non-Simplex inner block is nil")
	}

	pChainHeight := sm.LastNonSimplexBlockPChainHeight

	var validatorSet NodeBLSMappings
	if sm.LastNonSimplexInnerBlock.Height() == 0 {
		validatorSet = sm.GenesisValidatorSet
	} else {
		var err error
		validatorSet, err = sm.GetValidatorSet(pChainHeight)
		if err != nil {
			return nil, err
		}
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

	timestamp := sm.LastNonSimplexInnerBlock.Timestamp().UnixMilli()
	simplexEpochInfo := constructSimplexZeroBlockSimplexEpochInfo(pChainHeight, validatorSet, prevVMBlockSeq)

	md, err := simplex.ProtocolMetadataFromBytes(simplexMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to parse simplex metadata: %w", err)
	}
	md.Prev = sm.LastNonSimplexInnerBlock.Digest()
	md.Seq = sm.LastNonSimplexInnerBlock.Height()

	return &StateMachineBlock{
		Metadata: StateMachineMetadata{
			Timestamp:               uint64(timestamp),
			SimplexProtocolMetadata: simplexMetadata,
			SimplexBlacklist:        simplexBlacklist,
			SimplexEpochInfo:        simplexEpochInfo,
			PChainHeight:            pChainHeight,
		},
	}, nil
}

func (sm *StateMachine) verifyBlockZero(block *StateMachineBlock, prevBlock StateMachineBlock) error {
	if block == nil {
		return fmt.Errorf("block is nil")
	}

	if sm.LastNonSimplexInnerBlock == nil {
		return fmt.Errorf("failed verifying zero block: last non-Simplex inner block is nil")
	}

	simplexEpochInfo := block.Metadata.SimplexEpochInfo

	if simplexEpochInfo.EpochNumber != 1 {
		return fmt.Errorf("invalid epoch number (%d), should be 1", simplexEpochInfo.EpochNumber)
	}

	if prevBlock.InnerBlock == nil {
		return fmt.Errorf("parent inner block (%s) has no inner block", prevBlock.Digest())
	}

	pChainHeight := sm.LastNonSimplexBlockPChainHeight
	prevVMBlockSeq := prevBlock.InnerBlock.Height()

	if block.Metadata.PChainHeight != pChainHeight {
		return fmt.Errorf("invalid P-chain height (%d), expected to be %d",
			block.Metadata.PChainHeight, pChainHeight)
	}

	var expectedValidatorSet NodeBLSMappings
	if prevBlock.InnerBlock.Height() == 0 {
		expectedValidatorSet = sm.GenesisValidatorSet
	} else {
		var err error
		expectedValidatorSet, err = sm.GetValidatorSet(pChainHeight)
		if err != nil {
			return fmt.Errorf("failed to retrieve validator set at height %d: %w", pChainHeight, err)
		}
	}

	if simplexEpochInfo.BlockValidationDescriptor == nil {
		return fmt.Errorf("invalid BlockValidationDescriptor: should not be nil")
	}

	membership := simplexEpochInfo.BlockValidationDescriptor.AggregatedMembership.Members
	if !NodeBLSMappings(membership).Equal(expectedValidatorSet) {
		return fmt.Errorf("invalid BlockValidationDescriptor: should match validator set at P-chain height %d", pChainHeight)
	}

	// If we have compared all fields so far, the rest of the fields we compare by constructing an explicit expected SimplexEpochInfo
	expectedSimplexEpochInfo := constructSimplexZeroBlockSimplexEpochInfo(pChainHeight, expectedValidatorSet, prevVMBlockSeq)

	if !expectedSimplexEpochInfo.Equal(&simplexEpochInfo) {
		return fmt.Errorf("invalid SimplexEpochInfo: expected %v, got %v", expectedSimplexEpochInfo, simplexEpochInfo)
	}

	// The InnerBlock must match the last non-Simplex inner block.
	if block.InnerBlock != nil {
		return fmt.Errorf("zero block must not have an inner block")
	}
	if prevBlock.InnerBlock.Digest() != sm.LastNonSimplexInnerBlock.Digest() {
		return fmt.Errorf("zero block inner block digest does not match last non-Simplex inner block digest")
	}

	// The timestamp must equal the last non-Simplex inner block's timestamp.
	expectedTimestamp := uint64(sm.LastNonSimplexInnerBlock.Timestamp().UnixMilli())
	if block.Metadata.Timestamp != expectedTimestamp {
		return fmt.Errorf("expected timestamp to be %d but got %d", expectedTimestamp, block.Metadata.Timestamp)
	}

	return nil
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
	sm.Logger.Debug("Retrieved approvals from peers", zap.Int("numApprovals", len(approvalsFromPeers)))

	nextPChainHeight := newSimplexEpochInfo.NextPChainReferenceHeight
	prevNextEpochApprovals := parentBlock.Metadata.SimplexEpochInfo.NextEpochApprovals

	sigAggr := sm.SignatureAggregatorCreator(validators.NodeWeights())

	newApprovals, err := computeNewApprovals(prevNextEpochApprovals, approvalsFromPeers, nextPChainHeight, sigAggr, validators, sm.Logger)
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
		sm.Logger.Debug("Not enough approvals to seal epoch, building block without sealing the epoch")
		return sm.buildBlockImpatiently(ctx, parentBlock, simplexMetadata, simplexBlacklist, newSimplexEpochInfo, pChainHeight)
	}

	sm.Logger.Debug("Have enough approvals to seal epoch, building sealing block")

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
		firstSimplexBlock := sm.FirstEverSimplexBlock()
		if firstSimplexBlock == nil {
			return nil, fmt.Errorf("first ever Simplex block is not set, but attempted to create a sealing block for the first epoch")
		}
		simplexEpochInfo.PrevSealingBlockHash = firstSimplexBlock.Digest()
	}

	return sm.buildBlockImpatiently(ctx, parentBlock, simplexMetadata, simplexBlacklist, simplexEpochInfo, pChainHeight)
}

// wrapBlock creates a new StateMachineBlock by wrapping the VM block (if applicable) and adding the appropriate metadata.
func (sm *StateMachine) wrapBlock(parentBlock StateMachineBlock, childBlock VMBlock, newSimplexEpochInfo SimplexEpochInfo, pChainHeight uint64, simplexMetadata, simplexBlacklist []byte) *StateMachineBlock {
	timestamp := parentBlock.Metadata.Timestamp

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
			SimplexProtocolMetadata: simplexMetadata,
			SimplexBlacklist:        simplexBlacklist,
			SimplexEpochInfo:        newSimplexEpochInfo,
			PChainHeight:            pChainHeight,
		},
	}
}

// buildBlockEpochSealed builds a block where the epoch is being sealed due to a sealing block already created in this epoch.
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

	// TODO: This P-chain height should be taken from the ICM epoch
	childBlock, err := sm.BlockBuilder.BuildBlock(ctx, sm.GetPChainHeight())
	if err != nil {
		return nil, err
	}

	return sm.wrapBlock(parentBlock, childBlock, newSimplexEpochInfo, parentBlock.Metadata.PChainHeight, simplexMetadata, simplexBlacklist), nil
}

// constructSimplexZeroBlockSimplexEpochInfo constructs the SimplexEpochInfo for the zero block, which is the first ever block built by Simplex.
func constructSimplexZeroBlockSimplexEpochInfo(pChainHeight uint64, newValidatorSet NodeBLSMappings, prevVMBlockSeq uint64) SimplexEpochInfo {
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
	sigAggr simplex.SignatureAggregator,
	validators NodeBLSMappings,
	logger simplex.Logger,
) (*approvals, error) {
	if nextEpochApprovals == nil {
		nextEpochApprovals = &NextEpochApprovals{}
	}

	oldApprovingNodes := bitmaskFromBytes(nextEpochApprovals.NodeIDs)

	// We map each validator to its relative index in the validator set.
	nodeID2ValidatorIndex := make(map[nodeID]int)
	for i, nbm := range validators {
		nodeID2ValidatorIndex[nbm.NodeID] = i
	}

	oldApprovalFromPeersCount := len(approvalsFromPeers)
	// We have the approvals obtained from peers, but we need to sanitize them by filtering out approvals that are not valid,
	// such as approvals that do not agree with our candidate auxiliary info digest and P-Chain height,
	// and approvals that are from nodes that are not in the validator set or have already approved in prior blocks.
	approvalsFromPeers = sanitizeApprovals(approvalsFromPeers, pChainHeight, nodeID2ValidatorIndex, oldApprovingNodes, logger)
	logger.Debug("Santizied approvals after filtering out invalid approvals", zap.Int("numApprovalsBefore", oldApprovalFromPeersCount), zap.Int("numApprovalsAfter", len(approvalsFromPeers)))

	// Next we aggregate both previous and new approvals to compute the new aggregated signatures and the new bitmask of approving nodes.
	aggregatedSignature, newApprovingNodes, err := computeNewApproverSignaturesAndSigners(nextEpochApprovals, approvalsFromPeers, oldApprovingNodes, nodeID2ValidatorIndex, sigAggr, logger)
	if err != nil {
		return nil, err
	}

	// we check if we have enough approvals to seal the epoch by computing the relative approval ratio,
	// which is the ratio of the total weight of approving nodes divided by the total weight of all validators.
	canSeal := sigAggr.IsQuorum(validators.SelectSubset(newApprovingNodes))

	return &approvals{
		canSeal:   canSeal,
		signature: aggregatedSignature,
		nodeIDs:   newApprovingNodes.Bytes(),
	}, nil
}

// computeNewApproverSignaturesAndSigners computes the signatures of the nodes that approve the next epoch including the previous aggregated signature,
// and bitmask of nodes that correspond to those signatures, and aggregates all signatures together.
func computeNewApproverSignaturesAndSigners(
	nextEpochApprovals *NextEpochApprovals,
	approvalsFromPeers ValidatorSetApprovals,
	oldApprovingNodes bitmask,
	nodeID2ValidatorIndex map[nodeID]int,
	sigAggr simplex.SignatureAggregator,
	logger simplex.Logger,
) ([]byte, bitmask, error) {
	if nextEpochApprovals == nil {
		return nil, bitmask{}, fmt.Errorf("next epoch approvals is nil")
	}
	// Prepare the new signatures from the new approvals that haven't approved yet and that agree with our candidate auxiliary info digest and P-Chain height.
	newSignatures := make([][]byte, 0, len(approvalsFromPeers)+1)

	// We will overwrite the old approving nodes with the new approving nodes, by turning on the bits for the new approvers.
	newApprovingNodes := oldApprovingNodes.Clone()

	logger.Debug("Existing approving nodes bitmask before adding new approvals",
		zap.Int("count", oldApprovingNodes.Len()))

	logger.Debug("New approvals from peers that we will consider for aggregation", zap.Int("count", len(approvalsFromPeers)))

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

	// Finally, we aggregate all signatures together, to compute the new aggregated signature.
	aggregatedSignature, err := sigAggr.AppendSignatures(existingSignature, newSignatures...)
	if err != nil {
		return nil, bitmask{}, fmt.Errorf("failed to aggregate signatures: %w", err)
	}

	return aggregatedSignature, newApprovingNodes, nil
}

// sanitizeApprovals filters out approvals that are not valid by checking if they agree with our candidate auxiliary info digest and P-Chain height,
// and if they are from the validator set and haven't already been approved.
func sanitizeApprovals(approvals ValidatorSetApprovals, pChainHeight uint64, nodeID2ValidatorIndex map[nodeID]int, oldApprovingNodes bitmask, logger simplex.Logger) ValidatorSetApprovals {
	filter1 := approvalsThatAgreeWithPChainHeight(pChainHeight)
	filter2 := approvalsThatAreInValidatorSetAndHaveNotAlreadyApproved(oldApprovingNodes.Clone(), nodeID2ValidatorIndex)
	return approvals.Filter(filter1, logger).Filter(filter2, logger).UniqueByNodeID()
}

func approvalsThatAgreeWithPChainHeight(pChainHeight uint64) func(i int, approval ValidatorSetApproval, logger simplex.Logger) bool {
	return func(i int, approval ValidatorSetApproval, logger simplex.Logger) bool {
		// Pick only approvals that agree with our P-Chain height
		ok := approval.PChainHeight == pChainHeight
		if !ok {
			logger.Debug("Filtering out approval that does not agree with our P-Chain height",
				zap.String("nodeID", fmt.Sprintf("%x", approval.NodeID)),
				zap.Uint64("approvalPChainHeight", approval.PChainHeight),
				zap.Uint64("expectedPChainHeight", pChainHeight))
		}
		return ok
	}
}

func approvalsThatAreInValidatorSetAndHaveNotAlreadyApproved(oldApprovingNodes bitmask, nodeID2ValidatorIndex map[nodeID]int) func(i int, approval ValidatorSetApproval, logger simplex.Logger) bool {
	return func(i int, approval ValidatorSetApproval, logger simplex.Logger) bool {
		approvingNodeIndexOfNewApprover, exists := nodeID2ValidatorIndex[approval.NodeID]
		if !exists {
			logger.Debug("Filtering out approval from node that is not in the validator set",
				zap.String("nodeID", fmt.Sprintf("%x", approval.NodeID)))
			// If the approving node is not in the validator set, we ignore this approval.
			return false
		}
		// Only pick approvals from nodes that haven't already approved
		return !oldApprovingNodes.Contains(approvingNodeIndexOfNewApprover)
	}
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
		// Condition satisfied vacuously.
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

type approvals struct {
	canSeal   bool
	nodeIDs   []byte
	signature []byte
}

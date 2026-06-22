// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"context"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/ava-labs/simplex/common"
	"go.uber.org/zap"
)

const (
	// Max allowed time difference between a block's timestamp and the current time.
	// A block cannot be more than a certain time in the future compared to the current time.
	maxSkew = 10 * time.Second
)

// state encodes the different stages of the epoch transition process, which determines how we build and verify blocks.
//
// SimplexEpochInfo.NextState() inspects the parent block's metadata to perform the following state transitions:
//
//	 (initial state: No Simplex blocks yet)
//	                  │
//	                  ▼
//	┌───────────────────────────────────┐
//	│       stateFirstSimplexBlock      │  builds the zero block (no inner block);
//	│                                   │  creates epoch 1 with the initial validator set
//	└─────────────────┬─────────────────┘
//	                  │
//	                  ▼
//	┌───────────────────────────────────┐ ◀── validator set unchanged ──┐
//	│      stateBuildBlockNormalOp      │                               │
//	│  builds inner blocks within the   │ ──────────────────────────────┘
//	│  current epoch                    │ ◀────────────────────────────────────────────┐
//	└─────────────────┬─────────────────┘                                               │
//	                  │ validator set changed                                           │
//	                  │ (sets NextPChainReferenceHeight > 0)                            │
//	                  ▼                                                                 │
//	┌───────────────────────────────────┐ ◀── not enough approvals ─────┐               │
//	│   stateBuildCollectingApprovals   │                               │               │
//	│  aggregates approvals from        │ ──────────────────────────────┘               │
//	│  the next epoch's validator set   │                                               │
//	└─────────────────┬─────────────────┘                                               │
//	                  │ quorum reached: emit sealing block                              │
//	                  │ (BlockValidationDescriptor set)                                 │
//	                  ▼                                                                 │
//	┌───────────────────────────────────┐ ◀── sealing block ────────────┐               │
//	│    stateBuildBlockEpochSealed     │     not finalized yet         │               │
//	│  emits Telock (no inner block)    │ ──────────────────────────────┘               │
//	│  until the sealing block is       │                                               │
//	│  finalized; then opens the new    │ ─── new epoch (EpochNumber advanced) ─────────┘
//	│  epoch                            │
//	└───────────────────────────────────┘

var (
	errLastNonSimplexInnerBlockNil    = errors.New("failed constructing zero block: last non-Simplex inner block is nil")
	errInvalidProtocolMetadataSeq     = errors.New("invalid ProtocolMetadata sequence number: should be > 0")
	errInvalidProtocolMetadataEpoch   = errors.New("invalid ProtocolMetadata epoch number")
	errUnknownState                   = errors.New("unknown state")
	errBuiltGenesisInnerBlock         = errors.New("received a genesis block")
	errZeroBlockParentNoInnerBlock    = errors.New("zero block's parent has no inner block")
	errNilBlock                       = errors.New("block is nil")
	errInvalidPChainHeight            = errors.New("invalid P-chain height")
	errZeroBlockHasInnerBlock         = errors.New("zero block must not have an inner block")
	errZeroBlockInnerDigestMismatch   = errors.New("zero block inner block digest does not match last non-Simplex inner block digest")
	errZeroBlockTimestampMismatch     = errors.New("zero block timestamp does not match last non-Simplex inner block timestamp")
	errPrevSealingBlockNotFinalized   = errors.New("previous sealing block is not finalized")
	errBlockDigestMismatch            = errors.New("does not match proposed block digest")
	errSealingBlockSeqUnset           = errors.New("cannot build epoch sealed block: sealing block sequence is 0 or undefined")
	errEmptyNextEpochApprovals        = errors.New("next epoch approvals are empty")
	errPChainReferenceHeightMismatch  = errors.New("unexpected P-chain reference height")
	errPChainReferenceHeightDecreased = errors.New("P-chain reference height is decreasing")
	errValidatorSetUnchanged          = errors.New("validator set unchanged; next P-chain reference height should not have advanced")
	errPChainHeightNotReached         = errors.New("haven't reached referenced P-chain height yet")
	errPChainHeightTooBig             = errors.New("invalid P-chain height: greater than current")
	errPChainHeightSmallerThanParent  = errors.New("invalid P-chain height: smaller than parent block's")
	errSignerSetShrunk                = errors.New("some signers from parent block are missing from next epoch approvals of proposed block")
	errNextEpochApprovalsShrunk       = errors.New("previous block has next epoch approvals but proposed block doesn't have next epoch approvals")
	errTimestampTooBig                = errors.New("invalid timestamp: exceeds maximum int64 value")
	errTimestampDecreasing            = errors.New("invalid timestamp: proposed timestamp is before parent block's timestamp")
	errTimestampTooFarInFuture        = errors.New("invalid timestamp: proposed timestamp is too far in the future compared to current time")
	errAuxInfoBlockRetrieval          = errors.New("failed to retrieve block while collecting auxiliary info")

	signatureContext = "MSM approval"
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

// ICMEpochInput defines the input for computing the ICM Epoch information for the next block.
type ICMEpochInput struct {
	// ParentPChainHeight is the P-chain height recorded in the parent block.
	ParentPChainHeight uint64
	// ParentTimestamp is the timestamp of the parent block.
	ParentTimestamp time.Time
	// ChildTimestamp is the timestamp of the block being built.
	ChildTimestamp time.Time
	// ParentEpoch is the ICM epoch information from the parent block.
	ParentEpoch ICMEpochInfo
}

// ICMEpochTransition computes the next ICM epoch given the current upgrade configuration and epoch input.
type ICMEpochTransition func(ICMEpochInput) ICMEpochInfo

// ApprovalsRetriever retrieves the approvals from validators of the next epoch for the epoch change.
type ApprovalsRetriever interface {
	Approvals() ValidatorSetApprovals
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
type BlockRetriever func(seq uint64, digest [32]byte) (StateMachineBlock, *common.Finalization, error)

// BlockBuilder builds a new VM block with the given observed P-chain height.
type BlockBuilder interface {
	BuildBlock(ctx context.Context, pChainHeight uint64) (VMBlock, error)

	// WaitForPendingBlock returns when either the given context is cancelled,
	// or when the VM signals that a block should be built.
	WaitForPendingBlock(ctx context.Context)
}

// AuxiliaryInfoGenVerifier abstracts the application-specific logic for generating and verifying auxiliary information
// that is piggybacked on epoch transitions.
type AuxiliaryInfoGenVerifier interface {
	// IsLegalAppend checks whether the given auxiliary information byte slice [x]
	// can be appended to the history of auxiliary information for the given versionID, according to the app's rules.
	// Returns nil if the append is legal, or an error if the append is not legal or if any error occurs during the check.
	IsLegalAppend(versionID VersionID, nodes NodeBLSMappings, history [][]byte, x []byte) error

	// IsSufficient checks whether the given history of auxiliary information for the given versionID is sufficient
	// to start the epoch transition process.
	IsSufficient(versionID VersionID, nodes NodeBLSMappings, history [][]byte) (bool, error)

	// Generate generates an auxiliary information encoded as a byte slice based on the history of auxiliary information
	// for the given versionID in the current epoch so far.
	// If this is the first invocation in the epoch, DefaultversionID() should be passed as the VersionID.
	// Otherwise, the versionID from previous blocks in the epoch should be used.
	// If the application deems the given history to be sufficient for the epoch change, it can return a nil byte slice,
	// in which case it will not be appended to the history.
	Generate(versionID VersionID, nodes NodeBLSMappings, history [][]byte) ([]byte, error)

	// DefaultVersionID returns the default VersionID that should be used for epochs that don't have any any auxiliary information yet.
	DefaultVersionID() VersionID
}

// StateMachine manages block building and verification across epoch transitions.
type StateMachine struct {
	*Config
	lock sync.RWMutex
	approvalStore *ApprovalStore
	approvalStoreValidatorSet NodeBLSMappings
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
	// GetPChainHeightForProposing returns the latest known P-chain height to be used when building a block.
	GetPChainHeightForProposing func(context.Context) (uint64, error)
	// GetPChainHeightForVerifying returns the latest known P-chain height to be used when verifying a block.
	GetPChainHeightForVerifying func(context.Context) (uint64, error)
	// BlockBuilder builds new VM blocks.
	BlockBuilder BlockBuilder
	// Logger is used for logging state machine operations.
	Logger common.Logger
	// GetValidatorSet retrieves the validator set at a given P-chain height.
	GetValidatorSet ValidatorSetRetriever
	// GetBlock retrieves a previously built or finalized block.
	GetBlock BlockRetriever
	// SignatureAggregatorCreator creates a new SignatureAggregator for aggregating validator signatures for epoch transitions.
	SignatureAggregatorCreator common.SignatureAggregatorCreator
	// KeyAggregator aggregates public keys from validators.
	KeyAggregator KeyAggregator
	// SignatureVerifier verifies signatures from validators.
	SignatureVerifier SignatureVerifier
	// PChainProgressListener listens for changes in the P-chain height to trigger block building or epoch transitions.
	PChainProgressListener PChainProgressListener
	// LastNonSimplexBlockPChainHeight is the P-chain height of the last block built by a non-Simplex proposer.
	// It is used to determine the validator set of the first ever Simplex epoch.
	LastNonSimplexBlockPChainHeight uint64
	// LastNonSimplexInnerBlock is the inner block of the last block built by a non-Simplex proposer.
	LastNonSimplexInnerBlock VMBlock
	// GenesisValidatorSet is the validator set used for the genesis block.
	GenesisValidatorSet NodeBLSMappings
	// MyNodeID
	MyNodeID common.NodeID
	// Signer
	Signer common.Signer
	// ComputeICMEpoch computes the ICM epoch information in order to know which P-chain height to encode.
	ComputeICMEpoch ICMEpochTransition
	// AuxiliaryInfoApp abstracts an application that piggybacks on epoch changes.
	AuxiliaryInfoApp AuxiliaryInfoGenVerifier
}

type state uint8

const (
	stateFirstSimplexBlock state = iota + 1
	stateBuildBlockNormalOp
	stateBuildCollectingApprovals
	stateBuildBlockEpochSealed
)

func NewStateMachine(config *Config) (*StateMachine, error) {
	if config.LastNonSimplexInnerBlock == nil {
		config.Logger.Error("Last non-Simplex inner block is nil, cannot build zero block with correct metadata")
		return nil, errLastNonSimplexInnerBlockNil
	}
	if config.TimeSkewLimit == 0 {
		config.TimeSkewLimit = maxSkew
	}
	sm := StateMachine{Config: config}
	return &sm, nil
}

func (sm *StateMachine) HandleApproval(approval *ValidatorSetApproval, timestamp uint64) error {
	sm.lock.Lock()
	approvalStore := sm.approvalStore
	sm.lock.Unlock()

	if approvalStore == nil {
		sm.Logger.Debug("Approval store is not initialized, ignoring approval",
			zap.String("nodeID", fmt.Sprintf("%x", approval.NodeID)),
			zap.Uint64("pChainHeight", approval.PChainHeight))
		return nil
	}

	return approvalStore.HandleApproval(approval, timestamp)
}

func (sm *StateMachine) maybeInitializeApprovalStore(validatorSet NodeBLSMappings) error {
	sm.lock.Lock()
	defer sm.lock.Unlock()

	// If the approval store is not initialized or the validator set has changed, create a new approval store.
	if sm.approvalStore == nil || ! validatorSet.Equal(sm.approvalStoreValidatorSet) {
		// We first save the old approval store to copy over any existing approvals to the new approval store.
		oldApprovalStore := sm.approvalStore
		sm.approvalStore = NewApprovalStore(sm.SignatureVerifier, validatorSet, sm.Logger)
		sm.approvalStoreValidatorSet = validatorSet
		if oldApprovalStore != nil {
			oldApprovalStore.PutApprovals(sm.approvalStore)
		}
		return nil
	}

	return nil
}

// BuildBlock constructs the next block on top of the given parent block, and passes in the provided simplex metadata and blacklist.
func (sm *StateMachine) BuildBlock(ctx context.Context, metadata common.ProtocolMetadata, blacklist *common.Blacklist) (*StateMachineBlock, error) {
	// The zero sequence number is reserved for the genesis block, which should never be built.
	if metadata.Seq == 0 {
		return nil, fmt.Errorf("%w: got %d", errInvalidProtocolMetadataSeq, metadata.Seq)
	}

	prevBlockSeq := metadata.Seq - 1

	parentBlock, _, err := sm.GetBlock(prevBlockSeq, metadata.Prev)
	if err != nil {
		return nil, fmt.Errorf("failed retrieving parent block at height %d with digest %s: %w", prevBlockSeq, metadata.Prev.String(), err)
	}

	start := time.Now()

	sm.Logger.Debug("Building block",
		zap.Uint64("seq", metadata.Seq),
		zap.Uint64("epoch", metadata.Epoch),
		zap.Stringer("prevHash", metadata.Prev))

	defer func() {
		elapsed := time.Since(start)
		sm.Logger.Debug("Built block",
			zap.Uint64("seq", metadata.Seq),
			zap.Uint64("epoch", metadata.Epoch),
			zap.Stringer("prevHash", metadata.Prev),
			zap.Duration("elapsed", elapsed),
		)
	}()

	var simplexBlacklistBytes []byte
	if blacklist != nil {
		simplexBlacklistBytes = blacklist.Bytes()
	}

	// In order to know where in the epoch change process we are,
	// we identify the current state by looking at the parent block's epoch info.
	currentState := parentBlock.Metadata.SimplexEpochInfo.NextState()

	simplexMetadataBytes := metadata.Bytes()

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
		return nil, fmt.Errorf("%w: %d", errUnknownState, currentState)
	}
}

// VerifyBlock validates a proposed block by checking its metadata, epoch info,
// and inner block against the previous block and the current state.
func (sm *StateMachine) VerifyBlock(ctx context.Context, block *StateMachineBlock) error {
	if block == nil {
		return errNilBlock
	}

	pmd, err := common.ProtocolMetadataFromBytes(block.Metadata.SimplexProtocolMetadata)
	if err != nil {
		return fmt.Errorf("failed to parse ProtocolMetadata: %w", err)
	}

	seq := pmd.Seq

	if seq == 0 {
		// This shouldn't happen, but in case we're asked to verify a block with a sequence of 0,
		// we should reject it, because the zero sequence number is reserved for the genesis block, which should never be proposed.
		return errBuiltGenesisInnerBlock
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
		err = sm.verifyNonZeroBlock(ctx, block, &prevBlock, seq-1)
	}
	return err
}

func (sm *StateMachine) verifyNonZeroBlock(ctx context.Context, block, prevBlock *StateMachineBlock, prevSeq uint64) error {
	prevBlockMD := prevBlock.Metadata
	currentState := prevBlockMD.SimplexEpochInfo.NextState()

	if err := verifyTimestamp(block, prevBlock, sm.GetTime(), sm.TimeSkewLimit); err != nil {
		return fmt.Errorf("failed to verify timestamp: %w", err)
	}

	currentPChainHeight, err := sm.GetPChainHeightForVerifying(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current P-chain height for verifying: %w", err)
	}
	prevPChainHeight := prevBlockMD.PChainHeight
	proposedPChainHeight := block.Metadata.PChainHeight

	if err := verifyPChainHeight(proposedPChainHeight, currentPChainHeight, prevPChainHeight); err != nil {
		return fmt.Errorf("failed to verify P-chain height: %w", err)
	}

	err = sm.verifyEpochNumber(block)
	if err != nil {
		return err
	}

	switch currentState {
	case stateBuildBlockNormalOp:
		return sm.verifyNormalBlock(ctx, *prevBlock, block, prevSeq)
	case stateBuildCollectingApprovals:
		return sm.verifyCollectingApprovalsBlock(ctx, *prevBlock, block, prevSeq)
	case stateBuildBlockEpochSealed:
		return sm.verifyBlockEpochSealed(ctx, *prevBlock, block, prevSeq)
	default:
		return fmt.Errorf("%w: %d", errUnknownState, currentState)
	}
}

func verifyTimestamp(block *StateMachineBlock, prevBlock *StateMachineBlock, now time.Time, timeSkewLimit time.Duration) error {
	if block.Metadata.Timestamp > math.MaxInt64 {
		return fmt.Errorf("%w: timestamp %d exceeds maximum int64 value", errTimestampTooBig, block.Metadata.Timestamp)
	}

	if block.Metadata.Timestamp < prevBlock.Metadata.Timestamp {
		return fmt.Errorf("%w: proposed %d < parent %d", errTimestampDecreasing, block.Metadata.Timestamp, prevBlock.Metadata.Timestamp)
	}

	proposedTime := time.UnixMilli(int64(block.Metadata.Timestamp))

	if now.Add(timeSkewLimit).Before(proposedTime) {
		return fmt.Errorf("%w: proposed timestamp %v, max skew: %v", errTimestampTooFarInFuture, proposedTime, maxSkew)
	}
	return nil
}

func (sm *StateMachine) verifyEpochNumber(block *StateMachineBlock) error {
	md, err := common.ProtocolMetadataFromBytes(block.Metadata.SimplexProtocolMetadata)
	if err != nil {
		return fmt.Errorf("failed to parse ProtocolMetadata: %w", err)
	}
	if md.Epoch != block.Metadata.SimplexEpochInfo.EpochNumber {
		return fmt.Errorf("%w: got %d, expected %d", errInvalidProtocolMetadataEpoch, md.Epoch, block.Metadata.SimplexEpochInfo.EpochNumber)
	}
	return nil
}

// buildBlockNormalOp builds a block while potentially also transitioning to a new epoch, depending on the P-chain.
//
// Relevant SimplexEpochInfo fields (PCH = PChainReferenceHeight,
// EN = EpochNumber, NPCH = NextPChainReferenceHeight):
//
//	parent (NormalOp)            validator set unchanged    validator set changed at p'
//	┌─────────────────┐          ┌─────────────────┐        ┌─────────────────┐
//	│ PCH  = p        │   ───►   │ PCH  = p (copy) │        │ PCH  = p (copy) │
//	│ EN   = e        │    OR    │ EN   = e (copy) │   OR   │ EN   = e (copy) │
//	│ NPCH = 0        │          │ NPCH = 0        │        │ NPCH = p' (> 0) │
//	└─────────────────┘          └─────────────────┘        └─────────────────┘
//	                             → stays NormalOp           → CollectingApprovals
func (sm *StateMachine) buildBlockNormalOp(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata, simplexBlacklist []byte, prevBlockSeq uint64) (*StateMachineBlock, error) {
	// Since in the previous block, we were not transitioning to a new epoch,
	// the P-chain reference height and epoch of the new block should remain the same.
	newSimplexEpochInfo := SimplexEpochInfo{
		PChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight,
		EpochNumber:           parentBlock.Metadata.SimplexEpochInfo.EpochNumber,
		PrevVMBlockSeq:        computePrevVMBlockSeq(parentBlock, prevBlockSeq),
	}

	return sm.buildBlockOrTransitionEpoch(ctx, parentBlock, simplexMetadata, simplexBlacklist, newSimplexEpochInfo)
}

// buildBlockOrTransitionEpoch builds a block and decides whether to transition to a new epoch based on the P-chain height and validator set changes.
func (sm *StateMachine) buildBlockOrTransitionEpoch(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata, simplexBlacklist []byte, newSimplexEpochInfo SimplexEpochInfo) (*StateMachineBlock, error) {
	var isSealingBlockFinalized bool
	sealingBlockSeq := parentBlock.Metadata.SimplexEpochInfo.EpochNumber
	_, finalization, err := sm.GetBlock(sealingBlockSeq, [32]byte{})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve sealing block for previous epoch (%d): %w", sealingBlockSeq, err)
	}
	if finalization != nil {
		isSealingBlockFinalized = true
	}

	blockBuildingDecider := sm.createBlockBuildingDecider(newSimplexEpochInfo.PChainReferenceHeight)
	decisionToBuildBlock, err := blockBuildingDecider.shouldBuildBlock(ctx)
	if err != nil {
		return nil, err
	}

	sm.Logger.Debug("Block building decision",
		zap.Bool("build inner block", decisionToBuildBlock.buildInnerBlock),
		zap.Bool("transition epoch", decisionToBuildBlock.transitionEpoch),
		zap.Uint64("P-chain height", decisionToBuildBlock.pChainHeight))

	if decisionToBuildBlock.transitionEpoch && isSealingBlockFinalized {
		sm.Logger.Debug("Transitioning epoch after building block", zap.Uint64("newPChainRefHeight", decisionToBuildBlock.pChainHeight))
		newSimplexEpochInfo.NextPChainReferenceHeight = decisionToBuildBlock.pChainHeight
		sm.maybeInitializeApprovalStore(decisionToBuildBlock.validatorSet)
	}

	now := sm.GetTime()
	icmEpochInfo := computeICMEpochInfo(parentBlock, sm.ComputeICMEpoch, now)

	var innerBlock VMBlock

	if decisionToBuildBlock.buildInnerBlock {
		innerBlock, err = sm.BlockBuilder.BuildBlock(ctx, icmEpochInfo.PChainEpochHeight)
		if err != nil {
			return nil, err
		}
	}

	return wrapBlock(innerBlock, newSimplexEpochInfo, decisionToBuildBlock.pChainHeight, simplexMetadata, simplexBlacklist, now, icmEpochInfo, nil), nil
}

func computeICMEpochInfo(parentBlock StateMachineBlock, computeICMEpoch ICMEpochTransition, childTimestamp time.Time) ICMEpochInfo {
	parentTimestamp := time.UnixMilli(int64(parentBlock.Metadata.Timestamp))

	icmEpochInfo := computeICMEpoch(ICMEpochInput{
		ParentPChainHeight: parentBlock.Metadata.PChainHeight,
		ParentEpoch: ICMEpochInfo{
			PChainEpochHeight: parentBlock.Metadata.ICMEpochInfo.PChainEpochHeight,
			EpochNumber:       parentBlock.Metadata.ICMEpochInfo.EpochNumber,
			EpochStartTime:    parentBlock.Metadata.ICMEpochInfo.EpochStartTime,
		},
		ChildTimestamp:  childTimestamp,
		ParentTimestamp: parentTimestamp,
	})
	return icmEpochInfo
}

func verifyAgainstExpected(
	ctx context.Context,
	innerBlock VMBlock,
	expectedSimplexEpochInfo SimplexEpochInfo,
	expectedPChainHeight uint64,
	nextBlock *StateMachineBlock,
	timestamp time.Time,
	expectedIcmEpochInfo ICMEpochInfo,
	auxInfo *AuxiliaryInfo,
) error {
	if innerBlock != nil {
		if err := innerBlock.Verify(ctx, expectedIcmEpochInfo.PChainEpochHeight); err != nil {
			return err
		}
	}
	expectedBlock := wrapBlock(
		innerBlock, expectedSimplexEpochInfo, expectedPChainHeight,
		nextBlock.Metadata.SimplexProtocolMetadata, nextBlock.Metadata.SimplexBlacklist, timestamp, expectedIcmEpochInfo, auxInfo)
	if expectedBlock.Digest() != nextBlock.Digest() {
		return fmt.Errorf("expected block digest %s does not match proposed block digest %s: %w",
			expectedBlock.Digest(),
			nextBlock.Digest(),
			errBlockDigestMismatch)
	}
	return nil
}

func (sm *StateMachine) verifyNormalBlock(ctx context.Context, parentBlock StateMachineBlock, nextBlock *StateMachineBlock, prevBlockSeq uint64) error {
	newSimplexEpochInfo := SimplexEpochInfo{
		PChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight,
		EpochNumber:           parentBlock.Metadata.SimplexEpochInfo.EpochNumber,
		PrevVMBlockSeq:        computePrevVMBlockSeq(parentBlock, prevBlockSeq),
	}

	proposedPChainHeight := nextBlock.Metadata.PChainHeight

	timestamp := time.UnixMilli(int64(nextBlock.Metadata.Timestamp))

	icmEpochInfo := computeICMEpochInfo(parentBlock, sm.ComputeICMEpoch, timestamp)

	if err := sm.verifyNextPChainRefHeightNormal(ctx, parentBlock.Metadata, nextBlock.Metadata.SimplexEpochInfo); err != nil {
		return fmt.Errorf("failed to verify next P-chain reference height for normal block: %w", err)
	}
	newSimplexEpochInfo.NextPChainReferenceHeight = nextBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight

	return verifyAgainstExpected(ctx, nextBlock.InnerBlock, newSimplexEpochInfo, proposedPChainHeight, nextBlock, timestamp, icmEpochInfo, nil)
}

func verifyPChainHeight(proposedPChainHeight uint64, currentPChainHeight uint64, prevPChainHeight uint64) error {
	if proposedPChainHeight > currentPChainHeight {
		return fmt.Errorf("%w: proposed %d, current %d",
			errPChainHeightTooBig, proposedPChainHeight, currentPChainHeight)
	}

	if prevPChainHeight > proposedPChainHeight {
		return fmt.Errorf("%w: proposed %d, parent %d",
			errPChainHeightSmallerThanParent, proposedPChainHeight, prevPChainHeight)
	}
	return nil
}

func (sm *StateMachine) verifyNextPChainRefHeightNormal(ctx context.Context, prevMD StateMachineMetadata, next SimplexEpochInfo) error {
	prev := prevMD.SimplexEpochInfo
	// Next P-chain height can only increase, not decrease.
	if next.NextPChainReferenceHeight > 0 && prev.PChainReferenceHeight > next.NextPChainReferenceHeight {
		return fmt.Errorf("%w: previous P-chain reference height is %d and the proposed P-chain reference height is %d", errPChainReferenceHeightDecreased, prev.PChainReferenceHeight, next.NextPChainReferenceHeight)
	}

	// If the previous block already has a next P-chain reference height,
	// we should keep the same next P-chain reference height until we reach it.
	if prev.NextPChainReferenceHeight > 0 {
		if next.NextPChainReferenceHeight != prev.NextPChainReferenceHeight {
			return fmt.Errorf("%w: expected %d but got %d", errPChainReferenceHeightMismatch, prev.NextPChainReferenceHeight, next.NextPChainReferenceHeight)
		}
		return nil
	}

	// If we reached here, then prev.NextPChainReferenceHeight == 0.
	// If the previous block's next P-chain reference height is 0, and the new block's next P-chain reference height is > 0,
	// we need to ensure that we have finalized the sealing block of the previous epoch.
	if next.NextPChainReferenceHeight > 0 {
		sealingBlockSeq := prev.EpochNumber
		_, finalization, err := sm.GetBlock(sealingBlockSeq, [32]byte{})
		if err != nil {
			return fmt.Errorf("failed to retrieve sealing block for previous epoch (%d): %w", sealingBlockSeq, err)
		}
		if finalization == nil {
			return fmt.Errorf("%w: sealing block sequence %d", errPrevSealingBlockNotFinalized, sealingBlockSeq)
		}
	}

	// Make sure we have reached the next P-chain reference height, otherwise we won't be able to validate it.
	pChainHeight, err := sm.GetPChainHeightForVerifying(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current P-chain height for verifying: %w", err)
	}

	if pChainHeight < next.NextPChainReferenceHeight {
		return fmt.Errorf("%w: target %d, current %d", errPChainHeightNotReached, next.NextPChainReferenceHeight, pChainHeight)
	}

	// It might be that this block is the first block that has set the next P-chain reference height for the epoch,
	// so check if it has done so correctly by observing whether the validator set has indeed changed.

	currentValidatorSet, err := sm.GetValidatorSet(prevMD.SimplexEpochInfo.PChainReferenceHeight)
	if err != nil {
		return err
	}

	newValidatorSet, err := sm.GetValidatorSet(next.NextPChainReferenceHeight)
	if err != nil {
		return err
	}

	// If the validator set doesn't change, we shouldn't have increased the next P-chain reference height.
	if currentValidatorSet.Equal(newValidatorSet) && next.NextPChainReferenceHeight > 0 {
		return fmt.Errorf("%w: validator set at proposed next P-chain reference height %d matches previous block's P-chain reference height %d",
			errValidatorSetUnchanged, next.NextPChainReferenceHeight, prev.PChainReferenceHeight)
	}

	// Else, ! currentValidatorSet.Equal(newValidatorSet) || next.NextPChainReferenceHeight == 0
	// so if next.NextPChainReferenceHeight > 0, we should initialize the approval store for the new validator set.
	if next.NextPChainReferenceHeight > 0 {
		sm.maybeInitializeApprovalStore(newValidatorSet)
	}

	// Else, either the validator set has changed, or the next P-chain reference height is still 0.
	// Both of these cases are fine.

	return nil
}

// verifyNextPChainRefHeightForNewEpoch validates the proposed NextPChainReferenceHeight on the
// first block of a new epoch.
// This handles a corner case where the first block of an epoch initiates an epoch transition.
// We cannot reuse verifyNextPChainRefHeightNormal here — the baseline
// for the validator-set change check is the new epoch's PChainReferenceHeight, not the parent's,
// as in verifyNextPChainRefHeightNormal.
func (sm *StateMachine) verifyNextPChainRefHeightForNewEpoch(ctx context.Context, expectedEpochInfo SimplexEpochInfo, next SimplexEpochInfo) error {
	// The first block of the epoch doesn't trigger an epoch change, we're all set.
	if next.NextPChainReferenceHeight == 0 {
		return nil
	}

	// Next P-chain reference height cannot be smaller than the P-chain reference height,
	// as the P-chain reference height itself cannot decrease, and the next P-chain reference height
	// becomes the P-chain reference height when we change epochs.
	if next.NextPChainReferenceHeight < expectedEpochInfo.PChainReferenceHeight {
		return fmt.Errorf("%w: new epoch P-chain reference height is %d and the proposed next P-chain reference height is %d",
			errPChainReferenceHeightDecreased, expectedEpochInfo.PChainReferenceHeight, next.NextPChainReferenceHeight)
	}

	// If we haven't reached this P-chain height yet, we cannot accept the next P-chain reference height,
	// because there is no way of querying the validator set for the next P-chain reference height.
	pChainHeight, err := sm.GetPChainHeightForVerifying(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current P-chain height for verifying: %w", err)
	}
	if pChainHeight < next.NextPChainReferenceHeight {
		return fmt.Errorf("%w: target %d, current %d", errPChainHeightNotReached, next.NextPChainReferenceHeight, pChainHeight)
	}

	currentValidatorSet, err := sm.GetValidatorSet(expectedEpochInfo.PChainReferenceHeight)
	if err != nil {
		return err
	}

	newValidatorSet, err := sm.GetValidatorSet(next.NextPChainReferenceHeight)
	if err != nil {
		return err
	}

	if currentValidatorSet.Equal(newValidatorSet) {
		return fmt.Errorf("%w: validator set at proposed next P-chain reference height %d matches new epoch's P-chain reference height %d",
			errValidatorSetUnchanged, next.NextPChainReferenceHeight, expectedEpochInfo.PChainReferenceHeight)
	}

	sm.maybeInitializeApprovalStore(newValidatorSet)

	return nil
}

func (sm *StateMachine) createBlockBuildingDecider(pChainReferenceHeight uint64) blockBuildingDecider {
	blockBuildingDecider := blockBuildingDecider{
		logger:                   sm.Logger,
		maxBlockBuildingWaitTime: sm.MaxBlockBuildingWaitTime,
		pChainListener:           sm.PChainProgressListener,
		getPChainHeight:          sm.GetPChainHeightForProposing,
		waitForPendingBlock:      sm.BlockBuilder.WaitForPendingBlock,
		hasValidatorSetChanged: func(pChainHeight uint64) (bool, NodeBLSMappings, error) {
			// The given pChainHeight was sampled by the caller of shouldTransitionEpoch().
			// We compare between the current validator set, defined by the P-chain reference height in the parent block,
			// and the new validator set defined by the given pChainHeight.
			// If they are different, then we should transition to a new epoch.

			currentValidatorSet, err := sm.GetValidatorSet(pChainReferenceHeight)
			if err != nil {
				return false, nil, err
			}

			newValidatorSet, err := sm.GetValidatorSet(pChainHeight)
			if err != nil {
				return false, nil, err
			}

			if !currentValidatorSet.Equal(newValidatorSet) {
				sm.Logger.Debug("Validator set has changed, should transition epoch",
					zap.String("currentValidatorSet", fmt.Sprintf("%v", currentValidatorSet.NodeWeights())),
					zap.String("newValidatorSet", fmt.Sprintf("%v", newValidatorSet.NodeWeights())),
					zap.Uint64("currentPChainRefHeight", pChainReferenceHeight),
					zap.Uint64("newPChainHeight", pChainHeight))
				return true,newValidatorSet,  nil
			}
			return false, nil, nil
		},
	}
	return blockBuildingDecider
}

// buildBlockZero builds the first ever block for Simplex,
// which is a special block that introduces the first validator set and starts the first epoch.
//
// How EpochNumber (EN), PrevSealingBlockHash (PSH), and SealingBlockSeq (SBS)
// evolve along the block chain (Seq = block sequence number; h(n) = digest of
// the block at sequence n):
//
//		────────────────── Epoch 1 ────────────────────────────────────│─── Epoch s ────
//		                                                               │
//		Seq:     z          ...     s            s+1     ...    s+x    │ s+1  (Telocks get pruned) ...
//		       ┌──────┐            ┌────────┐  ┌──────┐       ┌──────┐ │ ┌────────────┐
//		       │ Zero │    ...     │Sealing │  │Telock│  ...  │Telock│ │ │first block │  ...
//		       │ block│            │ block  │  │      │       │      │ │ │ of epoch s │
//		       └──────┘            └────────┘  └──────┘       └──────┘ │ └────────────┘
//		       EN  = 1             EN  = 1     EN  = 1        EN  = 1  │ EN  = s
//		       SBS = 0             SBS = 0     SBS = s        SBS = s  │ SBS = 0
//		       PSH = 0             PSH = h(z)  PSH = 0        PSH = 0  │ PSH = 0
//
//		- EN  : copied within an epoch; on the first block of a new epoch, EN
//		        equals the sequence number of the previous epoch's sealing block.
//	         The first epoch number is set to the sequence number of that block.
//		- PSH : only set on a sealing block. In the first epoch it points to the zero block;
//		        otherwise it points to the previous epoch's sealing block.
//		- SBS : 0 except on Telocks of a sealed-but-not-yet-finalized epoch, where
//		        it equals the sequence number of that epoch's sealing block.
func (sm *StateMachine) buildBlockZero(parentBlock StateMachineBlock, simplexMetadata, simplexBlacklist []byte) (*StateMachineBlock, error) {
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
		return nil, errZeroBlockParentNoInnerBlock
	}

	// For the zero block, we set the timestamp to be the same as the last non-Simplex inner block's timestamp.
	// We do it because we need to carry over a minimum timestamp from the non-Simplex blocks.
	timestamp := sm.LastNonSimplexInnerBlock.Timestamp().UnixMilli()
	simplexEpochInfo := constructSimplexZeroBlockSimplexEpochInfo(pChainHeight, validatorSet, prevVMBlockSeq)

	md, err := common.ProtocolMetadataFromBytes(simplexMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to parse simplex metadata: %w", err)
	}
	md.Prev = sm.LastNonSimplexInnerBlock.Digest()
	md.Seq = sm.LastNonSimplexInnerBlock.Height()

	// The zero block carries over the parent's ICM epoch unchanged, just as it carries over the
	// timestamp. If the parent is a genesis block that predates ICM, the carried-over epoch is empty,
	// and the first ICM epoch begins on the block built on top of the zero block.
	parentICMEpochInfo := parentBlock.Metadata.ICMEpochInfo
	icmEpochInfo := ICMEpochInfo{
		PChainEpochHeight: parentICMEpochInfo.PChainEpochHeight,
		EpochNumber:       parentICMEpochInfo.EpochNumber,
		EpochStartTime:    parentICMEpochInfo.EpochStartTime,
	}

	return wrapBlock(nil,
		simplexEpochInfo,
		pChainHeight,
		simplexMetadata,
		simplexBlacklist,
		time.UnixMilli(timestamp),
		icmEpochInfo,
		nil,
	), nil
}

func (sm *StateMachine) verifyBlockZero(block *StateMachineBlock, prevBlock StateMachineBlock) error {
	if prevBlock.InnerBlock == nil {
		return fmt.Errorf("%w: parent digest %s", errZeroBlockParentNoInnerBlock, prevBlock.Digest())
	}

	pChainHeight := sm.LastNonSimplexBlockPChainHeight
	prevVMBlockSeq := prevBlock.InnerBlock.Height()

	if block.Metadata.PChainHeight != pChainHeight {
		return fmt.Errorf("%w: got %d, expected %d",
			errInvalidPChainHeight, block.Metadata.PChainHeight, pChainHeight)
	}

	var expectedValidatorSet NodeBLSMappings
	if prevVMBlockSeq == 0 {
		expectedValidatorSet = sm.GenesisValidatorSet
	} else {
		var err error
		expectedValidatorSet, err = sm.GetValidatorSet(pChainHeight)
		if err != nil {
			return fmt.Errorf("failed to retrieve validator set at height %d: %w", pChainHeight, err)
		}
	}

	now := sm.GetTime()
	if err := verifyTimestamp(block, &prevBlock, now, sm.TimeSkewLimit); err != nil {
		return fmt.Errorf("failed to verify timestamp for zero block: %w", err)
	}

	// The zero block carries over the parent's ICM epoch unchanged (see buildBlockZero).
	expectedICMEpochInfo := prevBlock.Metadata.ICMEpochInfo

	// If we have compared all fields so far, the rest of the fields we compare by constructing an explicit expected SimplexEpochInfo
	expectedSimplexEpochInfo := constructSimplexZeroBlockSimplexEpochInfo(pChainHeight, expectedValidatorSet, prevVMBlockSeq)
	expectedBlock := wrapBlock(nil,
		expectedSimplexEpochInfo,
		pChainHeight,
		block.Metadata.SimplexProtocolMetadata,
		block.Metadata.SimplexBlacklist,
		time.UnixMilli(int64(block.Metadata.Timestamp)),
		expectedICMEpochInfo,
		nil,
	)

	if expectedBlock.Digest() != block.Digest() {
		return fmt.Errorf("%w: expected %s but got %s", errBlockDigestMismatch, expectedBlock.Digest(), block.Digest())
	}

	// The InnerBlock must match the last non-Simplex inner block.
	if block.InnerBlock != nil {
		return errZeroBlockHasInnerBlock
	}
	if prevBlock.InnerBlock.Digest() != sm.LastNonSimplexInnerBlock.Digest() {
		return errZeroBlockInnerDigestMismatch
	}

	// The timestamp must equal the last non-Simplex inner block's timestamp.
	// We do it because we need to carry over a minimum timestamp from the non-Simplex blocks.
	expectedTimestamp := uint64(sm.LastNonSimplexInnerBlock.Timestamp().UnixMilli())
	if block.Metadata.Timestamp != expectedTimestamp {
		return fmt.Errorf("%w: expected %d but got %d", errZeroBlockTimestampMismatch, expectedTimestamp, block.Metadata.Timestamp)
	}

	return nil
}

// buildBlockCollectingApprovals builds either another collecting-approvals block (if not enough approvals yet)
// or a sealing block (if quorum is reached).
//
// Relevant SimplexEpochInfo fields (EN = EpochNumber, NPCH = NextPChainReferenceHeight,
// NEA = NextEpochApprovals, BVD = BlockValidationDescriptor, PSH = PrevSealingBlockHash):
//
//	parent (Collecting)              not enough approvals yet           quorum of approvals reached: sealing block
//	┌──────────────────┐             ┌────────────────────┐             ┌────────────────────────────┐
//	│ EN   = e         │             │ EN   = e           │             │ EN   = e                   │
//	│ NPCH = p'        │   ────►     │ NPCH = p'          │             │ NPCH = p'                  │
//	│ NEA  = A_old     │             │ NEA  = A_old ∪ new │     OR      │ NEA  = A_old ∪ new         │
//	│ BVD  = nil       │             │ BVD  = nil         │             │ BVD  = validator set at p' │
//	│                  │             │                    │             │ PSH  = h(prev epoch's      │
//	│                  │             │                    │             │        sealing block)      │
//	└──────────────────┘             └────────────────────┘             └────────────────────────────┘
//	                                 → stays Collecting                 → BuildBlockEpochSealed
func (sm *StateMachine) buildBlockCollectingApprovals(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata, simplexBlacklist []byte, prevBlockSeq uint64) (*StateMachineBlock, error) {
	// We prepare information that is needed to compute the approvals for the new epoch,
	// such as the validator set for the next epoch, and the approvals from peers.
	prevBlockNextPChainReferenceHeight := parentBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight
	validators, err := sm.GetValidatorSet(prevBlockNextPChainReferenceHeight)
	if err != nil {
		return nil, err
	}

	auxInfo, isAuxInfoReadyForEpochTransition, auxInfoDigest, err := sm.computeAuxInfo(parentBlock, prevBlockSeq, validators)
	if err != nil {
		return nil, fmt.Errorf("failed to compute auxiliary info: %w", err)
	}

	var newApprovals *approvals
	if isAuxInfoReadyForEpochTransition {
		newApprovals, err = sm.computeNewApprovals(parentBlock, validators, auxInfoDigest)
		if err != nil {
			return nil, err
		}
	} else {
		// We're not ready for epoch transition yet, so putting a zero-value approvals here
		// makes us stay in the collecting approvals state without contributing any approvals.
		newApprovals = &approvals{}
	}

	newSimplexEpochInfo := computeSimplexEpochInfoForCollectingApprovalsBlock(parentBlock, prevBlockSeq, newApprovals)

	pChainHeight := parentBlock.Metadata.PChainHeight

	now := sm.GetTime()
	icmEpochInfo := computeICMEpochInfo(parentBlock, sm.ComputeICMEpoch, now)

	// We might not have enough approvals to seal the current epoch,
	// in which case we just carry over the approvals we have so far to the next block,
	// so that eventually we'll have enough approvals to seal the epoch.
	if !newApprovals.canSeal {
		sm.Logger.Debug("Not enough approvals to seal epoch, building block without sealing the epoch")
		return sm.buildBlockImpatiently(ctx, now, simplexMetadata, simplexBlacklist, newSimplexEpochInfo, pChainHeight, icmEpochInfo, auxInfo)
	}

	sm.Logger.Debug("Have enough approvals to seal epoch, building sealing block")

	// Else, we have enough approvals to seal the epoch, so we create the sealing block.
	return sm.createSealingBlock(ctx, now, simplexMetadata, simplexBlacklist, newSimplexEpochInfo, pChainHeight, icmEpochInfo, auxInfo)
}

func (sm *StateMachine) verifyCollectingApprovalsBlock(ctx context.Context, parentBlock StateMachineBlock, nextBlock *StateMachineBlock, prevBlockSeq uint64) error {
	prevEpochInfo := parentBlock.Metadata.SimplexEpochInfo
	nextEpochInfo := nextBlock.Metadata.SimplexEpochInfo

	validators, err := sm.GetValidatorSet(prevEpochInfo.NextPChainReferenceHeight)
	if err != nil {
		return err
	}

	sm.maybeInitializeApprovalStore(validators)

	newApprovals := nextBlock.Metadata.SimplexEpochInfo.NextEpochApprovals

	expectedAuxInfo, auxInfoDigest, isAuxInfoReady, err := sm.computeExpectedAuxInfoForApprovalCollection(parentBlock, nextBlock, prevBlockSeq, validators)
	if err != nil {
		return fmt.Errorf("failed to compute expected auxiliary info for approval collection: %w", err)
	}

	// If the Auxiliary info is ready for epoch change, the block builder should at least include its own approval in the block it builds,
	// so we should have some approvals in the proposed block.
	if newApprovals == nil || len(newApprovals.NodeIDs) == 0 || len(newApprovals.Signature) == 0 {
		if isAuxInfoReady {
			return errEmptyNextEpochApprovals
		}
	}

	// If we aren't ready for epoch transition, we cannot collect approvals just yet.
	// So just make an empty NextEpochApprovals.
	if !isAuxInfoReady {
		if newApprovals != nil && (len(newApprovals.NodeIDs) > 0 || len(newApprovals.Signature) > 0) {
			return fmt.Errorf("expected no approvals when auxiliary info is not ready for epoch transition, but got some")
		}
		newSimplexEpochInfo := computeSimplexEpochInfoForCollectingApprovalsBlock(parentBlock, prevBlockSeq, &approvals{})
		timestamp := time.UnixMilli(int64(nextBlock.Metadata.Timestamp))
		icmEpochInfo := computeICMEpochInfo(parentBlock, sm.ComputeICMEpoch, timestamp)

		return verifyAgainstExpected(ctx, nextBlock.InnerBlock, newSimplexEpochInfo, nextBlock.Metadata.PChainHeight, nextBlock, timestamp, icmEpochInfo, expectedAuxInfo)
	}

	newSimplexEpochInfo := computeSimplexEpochInfoForCollectingApprovalsBlock(parentBlock, prevBlockSeq, &approvals{
		nodeIDs:   newApprovals.NodeIDs,
		signature: newApprovals.Signature,
	})

	err = sm.verifyNextEpochApprovalsSignature(parentBlock.Metadata, nextBlock.Metadata, validators, auxInfoDigest)
	if err != nil {
		return err
	}

	// A node cannot remove other nodes' approvals, only add its own approval if it wasn't included in the previous block.
	// So the set of signers in next.NextEpochApprovals should be a superset of the set of signers in prev.NextEpochApprovals.
	if err := areNextEpochApprovalsSignersSupersetOfApprovalsOfPrevBlock(prevEpochInfo, nextEpochInfo); err != nil {
		return err
	}

	sigAggr := sm.SignatureAggregatorCreator(validators.NodeWeights())
	approvals := bitmaskFromBytes(newApprovals.NodeIDs)
	canSeal := sigAggr.IsQuorum(validators.SelectSubset(approvals))

	if canSeal {
		newSimplexEpochInfo, err = sm.computeSimplexEpochInfoForSealingBlock(newSimplexEpochInfo)
		if err != nil {
			return fmt.Errorf("failed to compute simplex epoch info for sealing block: %w", err)
		}
	}

	timestamp := time.UnixMilli(int64(nextBlock.Metadata.Timestamp))
	icmEpochInfo := computeICMEpochInfo(parentBlock, sm.ComputeICMEpoch, timestamp)

	return verifyAgainstExpected(ctx, nextBlock.InnerBlock, newSimplexEpochInfo, nextBlock.Metadata.PChainHeight, nextBlock, timestamp, icmEpochInfo, expectedAuxInfo)
}

func (sm *StateMachine) verifyNextEpochApprovalsSignature(prevMD StateMachineMetadata, nextMD StateMachineMetadata, validators NodeBLSMappings, auxInfoDigest [32]byte) error {
	prev := prevMD.SimplexEpochInfo
	next := nextMD.SimplexEpochInfo

	// First figure out which validators are approving the next epoch by looking at the bitmask of approving nodes,
	// and then aggregate their public keys together to verify the signature.

	nodeIDsBitmask := next.NextEpochApprovals.NodeIDs
	aggPK, err := sm.aggregatePubKeysForBitmask(nodeIDsBitmask, validators)
	if err != nil {
		return err
	}

	pChainHeight := prev.NextPChainReferenceHeight

	toBeSigned, err := assembleApprovalToBeSigned(pChainHeight, auxInfoDigest)
	if err != nil {
		return err
	}

	if err := sm.SignatureVerifier.VerifySignature(next.NextEpochApprovals.Signature, toBeSigned, aggPK); err != nil {
		return fmt.Errorf("failed to verify signature: %w", err)
	}
	return nil
}

// assembleApprovalToBeSigned assembles the payload that is signed when approving an epoch transition.
// It consists of the P-chain reference height and the aux info digest (zeroed if not applicable).
func assembleApprovalToBeSigned(pChainHeight uint64, auxInfoDigest [32]byte) ([]byte, error) {
	payloadToSignBuff := make([]byte, 8+32) // 8 bytes for the P-chain height and 32 bytes for the aux info digest if applicable.
	binary.BigEndian.PutUint64(payloadToSignBuff[:8], pChainHeight)
	copy(payloadToSignBuff[8:], auxInfoDigest[:])

	signedMsg := common.SignedMessage{Payload: payloadToSignBuff, Context: signatureContext}
	return asn1.Marshal(signedMsg)
}

func (sm *StateMachine) aggregatePubKeysForBitmask(nodeIDsBitmask []byte, validators NodeBLSMappings) ([]byte, error) {
	approvingNodes := bitmaskFromBytes(nodeIDsBitmask)
	publicKeys := make([][]byte, 0, len(validators))
	for i := range validators {
		if !approvingNodes.Contains(i) {
			continue
		}
		publicKeys = append(publicKeys, validators[i].BLSKey)
	}

	aggPK, err := sm.KeyAggregator.AggregateKeys(publicKeys...)
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate public keys: %w", err)
	}
	return aggPK, nil
}

func computeSimplexEpochInfoForCollectingApprovalsBlock(parentBlock StateMachineBlock, prevBlockSeq uint64, newApprovals *approvals) SimplexEpochInfo {
	// The P-chain reference height and epoch number should remain the same until we transition to the new epoch.
	// The next P-chain reference height should have been set in the previous block,
	// which is the reason why we are collecting approvals in the first place.
	newSimplexEpochInfo := SimplexEpochInfo{
		PChainReferenceHeight:     parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight,
		EpochNumber:               parentBlock.Metadata.SimplexEpochInfo.EpochNumber,
		NextPChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight,
		PrevVMBlockSeq:            computePrevVMBlockSeq(parentBlock, prevBlockSeq),
	}

	// This might be the first time we created approvals for the next epoch,
	// so we need to initialize the NextEpochApprovals.
	if newSimplexEpochInfo.NextEpochApprovals == nil {
		newSimplexEpochInfo.NextEpochApprovals = &NextEpochApprovals{}
	}
	// The node IDs and signature are aggregated across all past and present approvals.
	newSimplexEpochInfo.NextEpochApprovals.NodeIDs = newApprovals.nodeIDs
	newSimplexEpochInfo.NextEpochApprovals.Signature = newApprovals.signature
	return newSimplexEpochInfo
}

func (sm *StateMachine) computeNewApprovals(parentBlock StateMachineBlock, validators NodeBLSMappings, auxInfoDigest [32]byte) (*approvals, error) {
	prevBlockNextPChainReferenceHeight := parentBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight
	sigAggr := sm.SignatureAggregatorCreator(validators.NodeWeights())

	// We retrieve approvals that validators have sent us for the next epoch.
	// These approvals are signed by validators of the next epoch.
	sm.maybeInitializeApprovalStore(validators)
	approvalsFromPeers := sm.approvalStore.Approvals()
	sm.Logger.Debug("Retrieved approvals from peers", zap.Int("numApprovals", len(approvalsFromPeers)))

	// Optimistically sign the epoch transition even if we have already did so in a previous round.
	// We'll just deduplicate this approval later on.

	sig, err := sm.createSelfApproval(prevBlockNextPChainReferenceHeight, auxInfoDigest)
	if err != nil {
		return nil, err
	}

	approvalsFromPeers = append(approvalsFromPeers, ValidatorSetApproval{
		NodeID:        nodeID(sm.MyNodeID),
		PChainHeight:  prevBlockNextPChainReferenceHeight,
		AuxInfoDigest: auxInfoDigest,
		Signature:     sig,
	})

	nextPChainHeight := prevBlockNextPChainReferenceHeight
	prevNextEpochApprovals := parentBlock.Metadata.SimplexEpochInfo.NextEpochApprovals

	newApprovals, err := computeNewApprovals(prevNextEpochApprovals, approvalsFromPeers, nextPChainHeight, auxInfoDigest, sigAggr, validators, sm.Logger)
	if err != nil {
		return nil, err
	}
	return newApprovals, nil
}

func (sm *StateMachine) createSelfApproval(nextPChainReferenceHeight uint64, auxInfoDigest [32]byte) ([]byte, error) {
	toBeSigned, err := assembleApprovalToBeSigned(nextPChainReferenceHeight, auxInfoDigest)
	if err != nil {
		return nil, err
	}

	sig, err := sm.Signer.Sign(toBeSigned)
	if err != nil {
		return nil, fmt.Errorf("failed to sign approval: %w", err)
	}
	return sig, nil
}

type auxInfoHistory struct {
	data    [][]byte
	lastSeq uint64
}

func (aih *auxInfoHistory) lastHistory() []byte {
	if len(aih.data) == 0 {
		return nil
	}
	return aih.data[len(aih.data)-1]
}

// collectAuxiliaryInfo traverses backwards starting from the given block and collects the AuxiliaryInfo of all blocks in the chain.
// returns the collected AuxiliaryInfo, the corresponding sequences of the blocks they were collected from,
// and the application ID of the oldest block that contains a non empty Info (or defaultVersionID if there was none).
func collectAuxiliaryInfo(block StateMachineBlock, startSeq uint64, getBlock BlockRetriever, defaultVersionID VersionID) (auxInfoHistory, VersionID, error) {
	var lastSeq *uint64
	var history [][]byte
	var versionID = defaultVersionID

	// We traverse the chain of blocks backwards in the following manner:
	// (1) Every block that doesn't have AuxiliaryInfo, its parents also do not have AuxiliaryInfo.
	// (2) Every block that has AuxiliaryInfo, its descendants also have AuxiliaryInfo.
	// (3) A block that has AuxiliaryInfo may have an empty Info field, but its PrevAuxInfoSeq field must point
	// to a block that its AuxiliaryInfo isn't nil, and its Info field is also non-nil.
	// (4) When a block with an empty Info field is built on a parent block that has AuxiliaryInfo,
	// if its parent block has a non-empty Info field, then the block's PrevAuxInfoSeq points to its parent block.
	// Else, its parent block has an empty Info field, then the block's PrevAuxInfoSeq is inherited from its parent block's PrevAuxInfoSeq.

	auxInfo := block.Metadata.AuxiliaryInfo
	currentSeq := startSeq
	for auxInfo != nil {
		if len(auxInfo.Info) > 0 {
			history = append(history, auxInfo.Info)
			if lastSeq == nil {
				lastSeq = new(uint64)
				*lastSeq = currentSeq
			}
			versionID = auxInfo.VersionID
		}
		if auxInfo.PrevAuxInfoSeq == 0 {
			// This is the first auxiliary info of the epoch, we can stop traversing back.
			break
		}
		currentSeq = auxInfo.PrevAuxInfoSeq
		prevBlock, _, err := getBlock(auxInfo.PrevAuxInfoSeq, [32]byte{})
		if err != nil {
			return auxInfoHistory{}, 0, fmt.Errorf("%w: at sequence %d: %w", errAuxInfoBlockRetrieval, auxInfo.PrevAuxInfoSeq, err)
		}
		auxInfo = prevBlock.Metadata.AuxiliaryInfo
	}

	if lastSeq == nil {
		lastSeq = new(uint64)
		*lastSeq = 0
	}

	// Reverse so the history (and the matching seqs) are ordered from oldest to newest.
	slices.Reverse(history)
	return auxInfoHistory{data: history, lastSeq: *lastSeq}, versionID, nil
}

// buildBlockImpatiently builds a block by waiting for the VM to build a block until MaxBlockBuildingWaitTime.
// If the VM fails to build a block within that time, we build a block without an inner block,
// so that we can continue making progress and not get stuck waiting for the VM.
func (sm *StateMachine) buildBlockImpatiently(ctx context.Context,
	timestamp time.Time,
	simplexMetadata []byte,
	simplexBlacklist []byte,
	simplexEpochInfo SimplexEpochInfo,
	pChainHeight uint64,
	icmEpochInfo ICMEpochInfo,
	auxInfo *AuxiliaryInfo) (*StateMachineBlock, error) {
	impatientContext, cancel := context.WithTimeout(ctx, sm.MaxBlockBuildingWaitTime)
	defer cancel()

	start := sm.GetTime()

	innerBlock, err := sm.BlockBuilder.BuildBlock(impatientContext, icmEpochInfo.PChainEpochHeight)
	if err != nil && impatientContext.Err() == nil {
		// If we got an error building the block, and we didn't time out, log the error but continue building the block without the inner block,
		// so that we can continue making progress and not get stuck on a single block.
		sm.Logger.Error("Error building block, building block without inner block instead", zap.Error(err))
	}
	if impatientContext.Err() != nil {
		sm.Logger.Debug("Timed out waiting for block to be built, building block without inner block instead",
			zap.Duration("elapsed", time.Since(start)), zap.Duration("maxBlockBuildingWaitTime", sm.MaxBlockBuildingWaitTime))
	}

	return wrapBlock(innerBlock, simplexEpochInfo, pChainHeight, simplexMetadata, simplexBlacklist, timestamp, icmEpochInfo, auxInfo), nil
}

func (sm *StateMachine) createSealingBlock(ctx context.Context,
	timestamp time.Time,
	simplexMetadata []byte,
	simplexBlacklist []byte,
	simplexEpochInfo SimplexEpochInfo,
	pChainHeight uint64,
	icmEpochInfo ICMEpochInfo,
	auxInfo *AuxiliaryInfo) (*StateMachineBlock, error) {
	simplexEpochInfo, err := sm.computeSimplexEpochInfoForSealingBlock(simplexEpochInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to compute simplex epoch info for sealing block: %w", err)
	}
	return sm.buildBlockImpatiently(ctx, timestamp, simplexMetadata, simplexBlacklist, simplexEpochInfo, pChainHeight, icmEpochInfo, auxInfo)
}

func (sm *StateMachine) computeSimplexEpochInfoForSealingBlock(simplexEpochInfo SimplexEpochInfo) (SimplexEpochInfo, error) {
	validators, err := sm.GetValidatorSet(simplexEpochInfo.NextPChainReferenceHeight)
	if err != nil {
		return SimplexEpochInfo{}, err
	}
	if simplexEpochInfo.BlockValidationDescriptor == nil {
		simplexEpochInfo.BlockValidationDescriptor = &BlockValidationDescriptor{}
	}
	simplexEpochInfo.BlockValidationDescriptor.AggregatedMembership.Members = validators

	prevSealingBlock, finalization, err := sm.GetBlock(simplexEpochInfo.EpochNumber, [32]byte{})
	if err != nil {
		sm.Logger.Error("Error retrieving previous sealing block", zap.Uint64("seq", simplexEpochInfo.EpochNumber), zap.Error(err))
		return SimplexEpochInfo{}, fmt.Errorf("failed to retrieve previous sealing InnerBlock at epoch %d: %w", simplexEpochInfo.EpochNumber, err)
	}
	if finalization == nil {
		sm.Logger.Error("Previous sealing block is not finalized", zap.Uint64("seq", simplexEpochInfo.EpochNumber))
		return SimplexEpochInfo{}, fmt.Errorf("%w: epoch %d", errPrevSealingBlockNotFinalized, simplexEpochInfo.EpochNumber)
	}
	simplexEpochInfo.PrevSealingBlockHash = prevSealingBlock.Digest()

	return simplexEpochInfo, nil
}

// wrapBlock creates a new StateMachineBlock by wrapping the VM block (if applicable) and adding the appropriate metadata.
func wrapBlock(
	childBlock VMBlock,
	newSimplexEpochInfo SimplexEpochInfo,
	pChainHeight uint64,
	simplexMetadata,
	simplexBlacklist []byte,
	timestamp time.Time,
	icmEpochInfo ICMEpochInfo,
	auxiliaryInfo *AuxiliaryInfo) *StateMachineBlock {

	return &StateMachineBlock{
		InnerBlock: childBlock,
		Metadata: StateMachineMetadata{
			Timestamp:               uint64(timestamp.UnixMilli()),
			SimplexProtocolMetadata: simplexMetadata,
			SimplexBlacklist:        simplexBlacklist,
			SimplexEpochInfo:        newSimplexEpochInfo,
			PChainHeight:            pChainHeight,
			ICMEpochInfo:            icmEpochInfo,
			AuxiliaryInfo:           auxiliaryInfo,
		},
	}
}

func (sm *StateMachine) areWeReadyToTransitionEpoch(parentBlock StateMachineBlock, prevBlockSeq uint64) (bool, uint64, StateMachineBlock, error) {
	sealingBlockSeq := parentBlock.Metadata.SimplexEpochInfo.SealingBlockSeq

	// If the sealing block sequence is still 0, it means previous block was the sealing block.
	if sealingBlockSeq == 0 {
		sealingBlockSeq = prevBlockSeq
	}

	if sealingBlockSeq == 0 {
		return false, 0, StateMachineBlock{}, errSealingBlockSeqUnset
	}

	sealingBlock, finalization, err := sm.GetBlock(sealingBlockSeq, [32]byte{})
	if err != nil {
		return false, 0, StateMachineBlock{}, fmt.Errorf("failed to retrieve sealing block at sequence %d: %w", sealingBlockSeq, err)
	}

	return finalization != nil, sealingBlockSeq, sealingBlock, nil
}

// buildBlockEpochSealed builds a block where the epoch is being sealed due to a sealing block already created in this epoch.
//
// Relevant SimplexEpochInfo fields (PCH = PChainReferenceHeight, EN = EpochNumber,
// NPCH = NextPChainReferenceHeight, SBS = SealingBlockSeq, BVD = BlockValidationDescriptor):
//
//	parent (sealing block)        sealing block NOT finalized      sealing block IS finalized
//	                              → emit Telock (no inner block)   → first block of new epoch
//	┌──────────────────┐          ┌──────────────────┐             ┌──────────────────────────┐
//	│ Seq  = s         │          │ Seq  = s+1       │             │ Seq  = s+1               │
//	│ PCH  = p         │          │ PCH  = p (copy)  │             │ PCH  = p' (was NPCH)     │
//	│ EN   = e         │   ──►    │ EN   = e (copy)  │      OR     │ EN   = s                 │
//	│ NPCH = p'        │    OR    │ NPCH = p' (copy) │             │ NPCH = 0  (reset)        │
//	│ SBS  = 0         │          │ SBS  = s         │             │ SBS  = 0                 │
//	│ BVD  = vset@p'   │          │ BVD  = nil       │             │ BVD  = nil               │
//	└──────────────────┘          └──────────────────┘             └──────────────────────────┘
//	                              → stays EpochSealed              → NormalOp (new epoch)
func (sm *StateMachine) buildBlockEpochSealed(ctx context.Context, parentBlock StateMachineBlock, simplexMetadata, simplexBlacklist []byte, prevBlockSeq uint64) (*StateMachineBlock, error) {
	// We check if the sealing block has already been finalized.
	// If not, we build a Telock block.
	readyToTransitionEpoch, sealingBlockSeq, sealingBlock, err := sm.areWeReadyToTransitionEpoch(parentBlock, prevBlockSeq)
	if err != nil {
		return nil, err
	}

	if !readyToTransitionEpoch {
		now := sm.GetTime()
		icmEpochInfo := computeICMEpochInfo(parentBlock, sm.ComputeICMEpoch, now)
		newSimplexEpochInfo := computeSimplexEpochInfoForTelock(parentBlock, sealingBlockSeq, prevBlockSeq)
		pChainHeight := parentBlock.Metadata.PChainHeight
		return wrapBlock(nil, newSimplexEpochInfo, pChainHeight, simplexMetadata, simplexBlacklist, now, icmEpochInfo, nil), nil
	}

	// Else, we build a block for the new epoch.
	newSimplexEpochInfo := computeSimplexEpochInfoForNewEpoch(parentBlock, sealingBlockSeq, prevBlockSeq)

	return sm.buildBlockOrTransitionEpoch(ctx, sealingBlock, simplexMetadata, simplexBlacklist, newSimplexEpochInfo)

}

func computeSimplexEpochInfoForNewEpoch(parentBlock StateMachineBlock, sealingBlockSeq uint64, prevBlockSeq uint64) SimplexEpochInfo {
	newSimplexEpochInfo := SimplexEpochInfo{
		// P-chain reference height is previous block's NextPChainReferenceHeight.
		PChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight,
		// The epoch number is the sequence of the sealing block.
		EpochNumber:    sealingBlockSeq,
		PrevVMBlockSeq: computePrevVMBlockSeq(parentBlock, prevBlockSeq),
	}
	return newSimplexEpochInfo
}

func computeSimplexEpochInfoForTelock(parentBlock StateMachineBlock, sealingBlockSeq uint64, prevBlockSeq uint64) SimplexEpochInfo {
	newSimplexEpochInfo := SimplexEpochInfo{
		PChainReferenceHeight:     parentBlock.Metadata.SimplexEpochInfo.PChainReferenceHeight,
		EpochNumber:               parentBlock.Metadata.SimplexEpochInfo.EpochNumber,
		NextPChainReferenceHeight: parentBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight,
		SealingBlockSeq:           sealingBlockSeq,
		PrevVMBlockSeq:            computePrevVMBlockSeq(parentBlock, prevBlockSeq),
	}
	return newSimplexEpochInfo
}

func (sm *StateMachine) verifyBlockEpochSealed(ctx context.Context, parentBlock StateMachineBlock, nextBlock *StateMachineBlock, prevBlockSeq uint64) error {
	isSealingBlockFinalized, sealingBlockSeq, _, err := sm.areWeReadyToTransitionEpoch(parentBlock, prevBlockSeq)
	if err != nil {
		return err
	}

	timestamp := time.UnixMilli(int64(nextBlock.Metadata.Timestamp))

	icmEpochInfo := computeICMEpochInfo(parentBlock, sm.ComputeICMEpoch, timestamp)

	newSimplexEpochInfo := computeSimplexEpochInfoForTelock(parentBlock, sealingBlockSeq, prevBlockSeq)

	if !isSealingBlockFinalized {
		return verifyAgainstExpected(ctx, nil, newSimplexEpochInfo, nextBlock.Metadata.PChainHeight, nextBlock, timestamp, icmEpochInfo, nil)
	}

	// Else, it's a new epoch.
	newSimplexEpochInfo = computeSimplexEpochInfoForNewEpoch(parentBlock, sealingBlockSeq, prevBlockSeq)

	// The first block of the new epoch may itself transition again, so trust and validate
	// the proposed pchain height and (optional) next pchain reference height, mirroring
	// what buildBlockOrTransitionEpoch does on the build side.
	proposedPChainHeight := nextBlock.Metadata.PChainHeight
	currentPChainHeight, err := sm.GetPChainHeightForVerifying(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current P-chain height for verifying: %w", err)
	}
	prevPChainHeight := parentBlock.Metadata.PChainHeight
	if err := verifyPChainHeight(proposedPChainHeight, currentPChainHeight, prevPChainHeight); err != nil {
		return fmt.Errorf("failed to verify P-chain height: %w", err)
	}

	if err := sm.verifyNextPChainRefHeightForNewEpoch(ctx, newSimplexEpochInfo, nextBlock.Metadata.SimplexEpochInfo); err != nil {
		return fmt.Errorf("failed to verify next P-chain reference height for new epoch block: %w", err)
	}
	newSimplexEpochInfo.NextPChainReferenceHeight = nextBlock.Metadata.SimplexEpochInfo.NextPChainReferenceHeight

	return verifyAgainstExpected(ctx, nextBlock.InnerBlock, newSimplexEpochInfo, proposedPChainHeight, nextBlock, timestamp, icmEpochInfo, nil)
}

// computeExpectedAuxInfoForApprovalCollection computes the expected AuxiliaryInfo that should be included in the proposed block
// for approval collection, and returns the auxiliary info digest, and whether the auxiliary info history is ready for epoch transition.
func (sm *StateMachine) computeExpectedAuxInfoForApprovalCollection(parentBlock StateMachineBlock, nextBlock *StateMachineBlock, prevBlockSeq uint64, validators NodeBLSMappings) (*AuxiliaryInfo, [32]byte, bool, error) {
	nextMD := nextBlock.Metadata
	prevMD := parentBlock.Metadata

	auxInfoHistory, versionID, err := collectAuxiliaryInfo(parentBlock, prevBlockSeq, sm.GetBlock, sm.AuxiliaryInfoApp.DefaultVersionID())
	if err != nil {
		return nil, [32]byte{}, false, err
	}

	if len(auxInfoHistory.data) > 0 && nextMD.AuxiliaryInfo == nil {
		// If we have auxiliary info history but the proposed block doesn't include any auxiliary info,
		// it means the block builder has dropped the auxiliary info, which is not allowed.
		return nil, [32]byte{}, false, fmt.Errorf("expected auxiliary info for application %d with history length %d, but got nil", versionID, len(auxInfoHistory.data))
	}

	// Else, either len(auxInfoHistory) == 0,
	// or the proposed block includes auxiliary info.
	// Both of these cases are fine, because a node doesn't have to include Auxiliary information.
	// We will verify the legality of the proposed auxiliary info (if any) in the next step.

	var expectedAuxInfo *AuxiliaryInfo
	var proposedAuxInf []byte

	if nextMD.AuxiliaryInfo != nil {
		proposedAuxInf = nextMD.AuxiliaryInfo.Info
		expectedAuxInfo = &AuxiliaryInfo{
			VersionID: versionID,
			Info:      proposedAuxInf,
		}
		if prevMD.AuxiliaryInfo != nil {
			expectedAuxInfo.PrevAuxInfoSeq = auxInfoHistory.lastSeq
		}
	}

	if err := sm.AuxiliaryInfoApp.IsLegalAppend(versionID, validators, auxInfoHistory.data, proposedAuxInf); err != nil {
		return nil, [32]byte{}, false, fmt.Errorf("proposed auxiliary info is not a legal append to the history for application %d: %w", versionID, err)
	}

	auxInfoReady, err := sm.AuxiliaryInfoApp.IsSufficient(versionID, validators, auxInfoHistory.data)
	if err != nil {
		return nil, [32]byte{}, false, fmt.Errorf("failed to check if auxiliary info history is final for application %d: %w", versionID, err)
	}

	var digest [32]byte
	if auxInfoReady {
		digest = sha256.Sum256(auxInfoHistory.lastHistory())
	}

	return expectedAuxInfo, digest, auxInfoReady, nil
}

// computeAuxInfo computes the AuxiliaryInfo that should be included in the block being built, and whether the auxiliary info history is ready for epoch transition,
func (sm *StateMachine) computeAuxInfo(parentBlock StateMachineBlock, prevBlockSeq uint64, validators NodeBLSMappings) (*AuxiliaryInfo, bool, common.Digest, error) {
	auxInfoHistory, versionID, err := collectAuxiliaryInfo(parentBlock, prevBlockSeq, sm.GetBlock, sm.AuxiliaryInfoApp.DefaultVersionID())
	if err != nil {
		return nil, false, common.Digest{}, err
	}

	isAuxInfoReadyForEpochTransition, err := sm.AuxiliaryInfoApp.IsSufficient(versionID, validators, auxInfoHistory.data)
	if err != nil {
		return nil, false, common.Digest{}, fmt.Errorf("failed to check if auxiliary info history is final: %w", err)
	}

	var auxInfo *AuxiliaryInfo
	parentAuxInfo := parentBlock.Metadata.AuxiliaryInfo
	if parentAuxInfo != nil {
		auxInfo = &AuxiliaryInfo{
			VersionID:      parentAuxInfo.VersionID,
			PrevAuxInfoSeq: auxInfoHistory.lastSeq,
		}
	}

	if !isAuxInfoReadyForEpochTransition {
		// If the auxiliary info isn't ready for epoch transition,
		// we should focus on contributing to finalizing it before collecting approvals for the epoch transition,
		// as without it being ready, we won't be able to transition epochs anyway.
		auxInf, err := sm.AuxiliaryInfoApp.Generate(versionID, validators, auxInfoHistory.data)
		if err != nil {
			return nil, false, common.Digest{}, fmt.Errorf("failed to generate auxiliary info: %w", err)
		}
		if auxInfo == nil {
			// This is the first auxiliary info we're generating for this epoch,
			// so we need to initialize it.
			auxInfo = &AuxiliaryInfo{
				VersionID: versionID,
				Info:      auxInf,
			}
		} else {
			// Otherwise, we already have auxiliary info from the parent block,
			// so we just update the Info field and carry over the VersionID and PrevAuxInfoSeq.
			auxInfo.Info = auxInf
		}
	}

	var auxInfoDigest common.Digest
	if isAuxInfoReadyForEpochTransition {
		auxInfoDigest = sha256.Sum256(auxInfoHistory.lastHistory())
	}

	return auxInfo, isAuxInfoReadyForEpochTransition, auxInfoDigest, nil
}

// constructSimplexZeroBlockSimplexEpochInfo constructs the SimplexEpochInfo for the zero block, which is the first ever block built by Simplex.
func constructSimplexZeroBlockSimplexEpochInfo(pChainHeight uint64, newValidatorSet NodeBLSMappings, prevVMBlockSeq uint64) SimplexEpochInfo {
	newSimplexEpochInfo := SimplexEpochInfo{
		PChainReferenceHeight: pChainHeight,
		EpochNumber:           prevVMBlockSeq + 1,
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
	prevNextEpochApprovals *NextEpochApprovals,
	approvalsFromPeers ValidatorSetApprovals,
	pChainHeight uint64,
	auxInfoDigest [32]byte,
	sigAggr common.SignatureAggregator,
	validators NodeBLSMappings,
	logger common.Logger,
) (*approvals, error) {
	if prevNextEpochApprovals == nil {
		prevNextEpochApprovals = &NextEpochApprovals{}
	}

	oldApprovingNodes := bitmaskFromBytes(prevNextEpochApprovals.NodeIDs)

	nodeID2ValidatorIndex := validators.IndexByNodeID()

	oldApprovalFromPeersCount := len(approvalsFromPeers)
	// We have the approvals obtained from peers, but we need to sanitize them by filtering out approvals that are not valid,
	// such as approvals that do not agree with our candidate auxiliary info digest and P-Chain height,
	// and approvals that are from nodes that are not in the validator set or have already approved in prior blocks.
	approvalsFromPeers = sanitizeApprovals(approvalsFromPeers, pChainHeight, auxInfoDigest, nodeID2ValidatorIndex, oldApprovingNodes, logger)
	logger.Debug("Sanitized approvals after filtering out invalid approvals", zap.Int("numApprovalsBefore", oldApprovalFromPeersCount), zap.Int("numApprovalsAfter", len(approvalsFromPeers)))

	// Next we aggregate both previous and new approvals to compute the new aggregated signatures and the new bitmask of approving nodes.
	aggregatedSignature, newApprovingNodes, err := computeNewApproverSignaturesAndSigners(prevNextEpochApprovals, approvalsFromPeers, oldApprovingNodes, nodeID2ValidatorIndex, sigAggr, logger)
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
	sigAggr common.SignatureAggregator,
	logger common.Logger,
) ([]byte, bitmask, error) {
	if nextEpochApprovals == nil {
		return nil, bitmask{}, errEmptyNextEpochApprovals
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
func sanitizeApprovals(approvals ValidatorSetApprovals, pChainHeight uint64, auxInfoDigest [32]byte, nodeID2ValidatorIndex map[nodeID]int, oldApprovingNodes bitmask, logger common.Logger) ValidatorSetApprovals {
	filter1 := approvalsThatAgreeWithPChainHeightAndAuxInfoDigest(pChainHeight, auxInfoDigest)
	filter2 := approvalsThatAreInValidatorSetAndHaveNotAlreadyApproved(oldApprovingNodes.Clone(), nodeID2ValidatorIndex)
	return approvals.Filter(filter1, logger).Filter(filter2, logger).UniqueByNodeID()
}

func approvalsThatAgreeWithPChainHeightAndAuxInfoDigest(pChainHeight uint64, auxInfoDigest [32]byte) func(approval ValidatorSetApproval, logger common.Logger) bool {
	return func(approval ValidatorSetApproval, logger common.Logger) bool {
		// Pick only approvals that agree with our P-Chain height
		ok := approval.PChainHeight == pChainHeight && approval.AuxInfoDigest == auxInfoDigest
		if !ok {
			logger.Debug("Filtering out approval that does not agree with our P-Chain height or auxiliary info digest",
				zap.String("nodeID", fmt.Sprintf("%x", approval.NodeID)),
				zap.Uint64("approvalPChainHeight", approval.PChainHeight),
				zap.Uint64("expectedPChainHeight", pChainHeight),
				zap.String("approvalAuxInfoSeqDigest", fmt.Sprintf("%x", approval.AuxInfoDigest)))
		}
		return ok
	}
}

func approvalsThatAreInValidatorSetAndHaveNotAlreadyApproved(oldApprovingNodes bitmask, nodeID2ValidatorIndex map[nodeID]int) func(approval ValidatorSetApproval, logger common.Logger) bool {
	return func(approval ValidatorSetApproval, logger common.Logger) bool {
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

// computePrevVMBlockSeq computes the block sequence of the previous VM block (inner block).
// The block sequence of the previous VM block is the number of VM blocks that have been built since genesis.
func computePrevVMBlockSeq(parentBlock StateMachineBlock, prevBlockSeq uint64) uint64 {
	// Either our parent block has no inner block, in which case we just inherit its previous VM block sequence,
	if parentBlock.InnerBlock == nil {
		return parentBlock.Metadata.SimplexEpochInfo.PrevVMBlockSeq
	}
	// or it has an inner block, in which case it is the previous block sequence.
	return prevBlockSeq
}

func areNextEpochApprovalsSignersSupersetOfApprovalsOfPrevBlock(prev SimplexEpochInfo, next SimplexEpochInfo) error {
	if prev.NextEpochApprovals == nil || len(prev.NextEpochApprovals.NodeIDs) == 0 {
		return nil
	}
	if next.NextEpochApprovals == nil {
		return fmt.Errorf("%w: previous block has next epoch approvals but proposed block doesn't have next epoch approvals", errNextEpochApprovalsShrunk)
	}
	// Make sure that previous signers are still there.
	prevSigners := bitmaskFromBytes(prev.NextEpochApprovals.NodeIDs)
	nextSigners := bitmaskFromBytes(next.NextEpochApprovals.NodeIDs)
	// Remove all bits in nextSigners from prevSigners
	prevSigners.Difference(&nextSigners)
	// If we have some bits left, it means there was a bit in prevSigners that wasn't in nextSigners
	if prevSigners.Len() > 0 {
		return errSignerSetShrunk
	}
	return nil
}

type approvals struct {
	canSeal   bool
	nodeIDs   []byte
	signature []byte
}

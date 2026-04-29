// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ava-labs/simplex"
)

type verificationInput struct {
	prevMD              StateMachineMetadata
	proposedBlockMD     StateMachineMetadata
	hasInnerBlock       bool
	innerBlockTimestamp time.Time // only set when hasInnerBlock is true
	prevBlockSeq        uint64
	nextBlockType       BlockType
	state               state
}

type verifier interface {
	Verify(in verificationInput) error
}
type validationDescriptorVerifier struct {
	getValidatorSet ValidatorSetRetriever
}

func (vd *validationDescriptorVerifier) Verify(in verificationInput) error {
	prev, next := in.prevMD.SimplexEpochInfo, in.proposedBlockMD.SimplexEpochInfo
	switch in.nextBlockType {
	case BlockTypeSealing:
		return vd.verifySealingBlock(prev, next)
	default:
		return vd.verifyEmptyValidationDescriptor(prev, next)
	}
}

func (vd *validationDescriptorVerifier) verifySealingBlock(prev SimplexEpochInfo, next SimplexEpochInfo) error {
	validators, err := vd.getValidatorSet(prev.NextPChainReferenceHeight)
	if err != nil {
		return err
	}

	if next.BlockValidationDescriptor == nil {
		return fmt.Errorf("validation descriptor should not be nil for a sealing block")
	}

	if !validators.Equal(next.BlockValidationDescriptor.AggregatedMembership.Members) {
		return fmt.Errorf("expected validator set specified at P-chain height %d does not match validator set encoded in new block", next.NextPChainReferenceHeight)
	}

	return nil
}

func (vd *validationDescriptorVerifier) verifyEmptyValidationDescriptor(_ SimplexEpochInfo, next SimplexEpochInfo) error {
	if next.BlockValidationDescriptor != nil {
		return fmt.Errorf("block validation descriptor should be nil but got %v", next.BlockValidationDescriptor)
	}
	return nil
}

type nextEpochApprovalsVerifier struct {
	sigVerifier     SignatureVerifier
	getValidatorSet ValidatorSetRetriever
	keyAggregator   KeyAggregator
}

func (nv *nextEpochApprovalsVerifier) Verify(in verificationInput) error {
	prev, next := in.prevMD.SimplexEpochInfo, in.proposedBlockMD.SimplexEpochInfo

	switch in.nextBlockType {
	case BlockTypeSealing:
		return nv.verifySealingBlock(prev, next)
	case BlockTypeNormal:
		return nv.verifyNormal(prev, next)
	default:
		return nv.verifyEmptyNextEpochApprovals(prev, next)
	}
}

func (nv *nextEpochApprovalsVerifier) verifySealingBlock(prev SimplexEpochInfo, next SimplexEpochInfo) error {
	if next.NextEpochApprovals == nil {
		return fmt.Errorf("next epoch approvals should not be nil for a sealing block")
	}

	validators, err := nv.getValidatorSet(prev.NextPChainReferenceHeight)
	if err != nil {
		return err
	}

	err = nv.verifySignature(prev, next, validators)
	if err != nil {
		return err
	}

	approvingNodes := bitmaskFromBytes(next.NextEpochApprovals.NodeIDs)
	canSeal, err := canSealBlock(validators, approvingNodes)
	if err != nil {
		return err
	}

	if !canSeal {
		return fmt.Errorf("not enough approvals to seal block")
	}

	return nil
}

func (nv *nextEpochApprovalsVerifier) verifyNormal(prev SimplexEpochInfo, next SimplexEpochInfo) error {
	if prev.NextPChainReferenceHeight == 0 {
		return nil
	}

	// Otherwise, prev.NextPChainReferenceHeight > 0, so this means we're collecting approvals

	if next.NextEpochApprovals == nil {
		// The node that proposed the block should have included at least its own approval.
		return fmt.Errorf("next epoch approvals should not be nil when collecting approvals")
	}

	validators, err := nv.getValidatorSet(prev.NextPChainReferenceHeight)
	if err != nil {
		return err
	}

	err = nv.verifySignature(prev, next, validators)
	if err != nil {
		return err
	}

	// A node cannot remove other nodes' approvals, only add its own approval if it wasn't included in the previous block.
	// So the set of signers in next.NextEpochApprovals should be a superset of the set of signers in prev.NextEpochApprovals.
	if err := areNextEpochApprovalsSignersSupersetOfApprovalsOfPrevBlock(prev, next); err != nil {
		return err
	}

	return nil
}

func (nv *nextEpochApprovalsVerifier) verifyEmptyNextEpochApprovals(_ SimplexEpochInfo, next SimplexEpochInfo) error {
	if next.NextEpochApprovals != nil {
		return fmt.Errorf("next epoch approvals should be nil but got %v", next.NextEpochApprovals)
	}
	return nil
}

func (nv *nextEpochApprovalsVerifier) verifySignature(prev SimplexEpochInfo, next SimplexEpochInfo, validators NodeBLSMappings) error {
	// First figure out which validators are approving the next epoch by looking at the bitmask of approving nodes,
	// and then aggregate their public keys together to verify the signature.

	nodeIDsBitmask := next.NextEpochApprovals.NodeIDs
	aggPK, err := nv.aggregatePubKeysForBitmask(nodeIDsBitmask, validators)
	if err != nil {
		return err
	}

	message := nv.createMessageToBeVerified(prev)

	if err := nv.sigVerifier.VerifySignature(next.NextEpochApprovals.Signature, message, aggPK); err != nil {
		return fmt.Errorf("failed to verify signature: %w", err)
	}
	return nil
}

func (nv *nextEpochApprovalsVerifier) createMessageToBeVerified(prev SimplexEpochInfo) []byte {
	pChainHeightBuff := pChainNextReferenceHeightAsBytes(prev)

	var bb bytes.Buffer
	bb.Write(pChainHeightBuff)

	message := bb.Bytes()
	return message
}

func (nv *nextEpochApprovalsVerifier) aggregatePubKeysForBitmask(nodeIDsBitmask []byte, validators NodeBLSMappings) ([]byte, error) {
	approvingNodes := bitmaskFromBytes(nodeIDsBitmask)
	publicKeys := make([][]byte, 0, len(validators))
	validators.ForEach(func(i int, nbm NodeBLSMapping) {
		if !approvingNodes.Contains(i) {
			return
		}
		publicKeys = append(publicKeys, nbm.BLSKey)
	})

	aggPK, err := nv.keyAggregator.AggregateKeys(publicKeys...)
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate public keys: %w", err)
	}
	return aggPK, nil
}

func pChainNextReferenceHeightAsBytes(prev SimplexEpochInfo) []byte {
	pChainHeight := prev.NextPChainReferenceHeight
	pChainHeightBuff := make([]byte, 8)
	binary.BigEndian.PutUint64(pChainHeightBuff, pChainHeight)
	return pChainHeightBuff
}

type nextPChainReferenceHeightVerifier struct {
	getValidatorSet ValidatorSetRetriever
	getPChainHeight func() uint64
}

func (n *nextPChainReferenceHeightVerifier) Verify(in verificationInput) error {
	prev, next := in.prevMD.SimplexEpochInfo, in.proposedBlockMD.SimplexEpochInfo
	switch in.nextBlockType {
	case BlockTypeTelock, BlockTypeSealing:
		if prev.NextPChainReferenceHeight != next.NextPChainReferenceHeight {
			return fmt.Errorf("expected P-chain reference height to be %d but got %d", prev.NextPChainReferenceHeight, next.NextPChainReferenceHeight)
		}
	case BlockTypeNormal:
		return n.verifyNextPChainRefHeightNormal(in.prevMD, prev, next)
	case BlockTypeNewEpoch:
		if next.NextPChainReferenceHeight != 0 {
			return fmt.Errorf("expected P-chain reference height to be 0 but got %d", next.NextPChainReferenceHeight)
		}
	default:
		return fmt.Errorf("unknown block type: %d", in.nextBlockType)
	}
	return nil
}

func (n *nextPChainReferenceHeightVerifier) verifyNextPChainRefHeightNormal(prevMD StateMachineMetadata, prev SimplexEpochInfo, next SimplexEpochInfo) error {
	// Next P-chain height can only increase, not decrease.
	if next.NextPChainReferenceHeight > 0 && prev.PChainReferenceHeight > next.NextPChainReferenceHeight {
		return fmt.Errorf("expected P-chain reference height to be non-decreasing, "+
			"but the previous P-chain reference height is %d and the proposed P-chain reference height is %d", prev.PChainReferenceHeight, next.NextPChainReferenceHeight)
	}

	// If the previous block already has a next P-chain reference height,
	// we should keep the same next P-chain reference height until we reach it.
	if prev.NextPChainReferenceHeight > 0 {
		if next.NextPChainReferenceHeight != prev.NextPChainReferenceHeight {
			return fmt.Errorf("expected P-chain reference height to be %d but got %d", prev.NextPChainReferenceHeight, next.NextPChainReferenceHeight)
		}
		return nil
	}

	// If we reached here, then prev.NextPChainReferenceHeight == 0.
	// It might be that this block is the first block that has set the next P-chain reference height for the epoch,
	// so check if it has done so correctly by observing whether the validator set has indeed changed.

	currentValidatorSet, err := n.getValidatorSet(prevMD.SimplexEpochInfo.PChainReferenceHeight)
	if err != nil {
		return err
	}

	newValidatorSet, err := n.getValidatorSet(next.NextPChainReferenceHeight)
	if err != nil {
		return err
	}

	// If the validator set doesn't change, we shouldn't have increased the next P-chain reference height.
	if currentValidatorSet.Equal(newValidatorSet) && next.NextPChainReferenceHeight > 0 {
		return fmt.Errorf("validator set at proposed next P-chain reference height %d is the same as "+
			"validator set at previous block's P-chain reference height %d,"+
			"so expected next P-chain reference height to remain the same but got %d",
			next.NextPChainReferenceHeight, prev.PChainReferenceHeight, next.NextPChainReferenceHeight)
	}

	// Else, either the validator set has changed, or the next P-chain reference height is still 0.
	// Both of these cases are fine, but we should verify that we have observed the next P-chain reference height if it is > 0.

	pChainHeight := n.getPChainHeight()

	if pChainHeight < next.NextPChainReferenceHeight {
		return fmt.Errorf("haven't reached P-chain height %d yet, current P-chain height is only %d", next.NextPChainReferenceHeight, pChainHeight)
	}

	return nil
}

type epochNumberVerifier struct{}

func (e *epochNumberVerifier) Verify(in verificationInput) error {
	prev, next := in.prevMD.SimplexEpochInfo, in.proposedBlockMD.SimplexEpochInfo

	// An epoch number of 0 means this is not a Simplex block, so the next block should be the first Simplex block with epoch number 1.
	if in.prevMD.SimplexEpochInfo.EpochNumber == 0 && in.proposedBlockMD.SimplexEpochInfo.EpochNumber != 1 {
		return fmt.Errorf("expected epoch number of the first block created to be 1 but got %d", next.EpochNumber)
	}

	// The only time in which we should increase the epoch number is when we have a block that marks the start of a new epoch.
	switch in.nextBlockType {
	case BlockTypeNewEpoch:
		// TODO: we have to make sure that Telocks are pruned before moving to a new epoch, otherwise we hit a false negative below.
		if in.prevBlockSeq != next.EpochNumber {
			return fmt.Errorf("expected epoch number to be %d but got %d", in.prevBlockSeq, next.EpochNumber)
		}
	default:
		if prev.EpochNumber != next.EpochNumber {
			return fmt.Errorf("expected epoch number to be %d but got %d", prev.EpochNumber, next.EpochNumber)
		}
	}
	return nil
}

type sealingBlockSeqVerifier struct{}

func (s *sealingBlockSeqVerifier) Verify(in verificationInput) error {
	prev, next := in.prevMD.SimplexEpochInfo, in.proposedBlockMD.SimplexEpochInfo

	// A block should only have a sealing block if it is a Telock.
	switch in.nextBlockType {
	case BlockTypeNewEpoch, BlockTypeNormal, BlockTypeSealing:
		if next.SealingBlockSeq != 0 {
			return fmt.Errorf("expected sealing block sequence number to be 0 but got %d", next.SealingBlockSeq)
		}
	case BlockTypeTelock:
		// This is not the first Telock, make sure the sealing block sequence number doesn't change.

		// prev.SealingBlockSeq > 0 means the previous block is a Telock.
		if prev.SealingBlockSeq > 0 && next.SealingBlockSeq != prev.SealingBlockSeq {
			return fmt.Errorf("expected sealing block sequence number to be %d but got %d", prev.SealingBlockSeq, next.SealingBlockSeq)
		}

		// Else, either this is the first Telock, or the previous block's sealing block sequence is equal to this block's sealing block sequence.

		// We need to check the first case has a valid sealing block sequence, as the second case is fine by definition.
		if prev.BlockValidationDescriptor != nil {
			md, err := simplex.ProtocolMetadataFromBytes(in.prevMD.SimplexProtocolMetadata)
			if err != nil {
				return fmt.Errorf("failed parsing protocol metadata: %w", err)
			}
			if next.SealingBlockSeq != md.Seq {
				return fmt.Errorf("expected sealing block sequence number to be %d but got %d", md.Seq, next.SealingBlockSeq)
			}
		}
	default:
		return fmt.Errorf("unknown block type: %d", in.nextBlockType)
	}

	return nil
}

type pChainHeightVerifier struct {
	getPChainHeight func() uint64
}

func (p *pChainHeightVerifier) Verify(in verificationInput) error {
	currentPChainHeight := p.getPChainHeight()

	if in.proposedBlockMD.PChainHeight > currentPChainHeight {
		return fmt.Errorf("invalid P-chain height (%d) is too big, expected to be ≤ %d",
			in.proposedBlockMD.PChainHeight, currentPChainHeight)
	}

	if in.prevMD.PChainHeight > in.proposedBlockMD.PChainHeight {
		return fmt.Errorf("invalid P-chain height (%d) is smaller than parent block's P-chain height (%d)",
			in.proposedBlockMD.PChainHeight, in.prevMD.PChainHeight)
	}

	return nil
}

type pChainReferenceHeightVerifier struct{}

func (p *pChainReferenceHeightVerifier) Verify(in verificationInput) error {
	prev, next := in.prevMD.SimplexEpochInfo, in.proposedBlockMD.SimplexEpochInfo

	switch in.nextBlockType {
	case BlockTypeNewEpoch:
		if prev.NextPChainReferenceHeight != next.PChainReferenceHeight {
			return fmt.Errorf("expected P-chain reference height of the first block of epoch %d to be %d but got %d",
				prev.SealingBlockSeq, prev.NextPChainReferenceHeight, next.PChainReferenceHeight)
		}
	default:
		if prev.PChainReferenceHeight != next.PChainReferenceHeight {
			return fmt.Errorf("expected P-chain reference height to be %d but got %d", prev.PChainReferenceHeight, next.PChainReferenceHeight)
		}
	}

	return nil
}

type timestampVerifier struct {
	getTime       func() time.Time
	timeSkewLimit time.Duration
}

func (t *timestampVerifier) Verify(in verificationInput) error {
	if !in.hasInnerBlock {
		// If no inner block, the timestamp is inherited from the parent block.
		if in.proposedBlockMD.Timestamp != in.prevMD.Timestamp {
			return fmt.Errorf("block without inner block should inherit parent timestamp %d but got %d", in.prevMD.Timestamp, in.proposedBlockMD.Timestamp)
		}
	} else {
		// If there is an inner block, the timestamp should be the same as the inner block's timestamp.
		if in.proposedBlockMD.Timestamp != uint64(in.innerBlockTimestamp.UnixMilli()) {
			return fmt.Errorf("block timestamp %d does not match inner block timestamp %d", in.proposedBlockMD.Timestamp, in.innerBlockTimestamp.UnixMilli())
		}
	}

	timestamp := time.UnixMilli(int64(in.proposedBlockMD.Timestamp))

	currentTime := t.getTime()
	if currentTime.Add(t.timeSkewLimit).Before(timestamp) {
		return fmt.Errorf("proposed block timestamp is too far in the future, current time is %v but got %v", currentTime, timestamp)
	}

	if in.prevMD.Timestamp > in.proposedBlockMD.Timestamp {
		return fmt.Errorf("proposed block timestamp is older than parent block's timestamp, parent timestamp is %d but got %d", in.prevMD.Timestamp, in.proposedBlockMD.Timestamp)
	}
	return nil
}

type prevSealingBlockHashVerifier struct {
	getBlock              BlockRetriever
	latestPersistedHeight *uint64
}

func (p *prevSealingBlockHashVerifier) Verify(in verificationInput) error {
	prev, _ := in.prevMD.SimplexEpochInfo, in.proposedBlockMD.SimplexEpochInfo

	// Sealing block of the first epoch must point to the first ever Simplex block as the previous sealing block.
	if prev.EpochNumber == 1 && in.nextBlockType == BlockTypeSealing {
		firstEverSimplexBlockSeq, err := findFirstSimplexBlock(p.getBlock, *p.latestPersistedHeight+1)
		if err != nil {
			return fmt.Errorf("failed to find first Simplex block: %w", err)
		}

		block, _, err := p.getBlock(RetrievingOpts{Height: firstEverSimplexBlockSeq})
		if err != nil {
			return fmt.Errorf("failed retrieving first ever simplex block %d: %w", firstEverSimplexBlockSeq, err)
		}

		hash := block.Digest()
		if !bytes.Equal(in.proposedBlockMD.SimplexEpochInfo.PrevSealingBlockHash[:], hash[:]) {
			return fmt.Errorf("expected prev sealing block hash of the first ever simplex block to be %x but got %x", hash, in.proposedBlockMD.SimplexEpochInfo.PrevSealingBlockHash)
		}

		return nil
	}

	// Otherwise, we can only have a previous sealing block hash if this is a sealing block,
	// and in that case, the previous sealing block hash should match the hash of the sealing block of the previous epoch.

	switch in.nextBlockType {
	case BlockTypeSealing:
		prevSealingBlock, _, err := p.getBlock(RetrievingOpts{Height: in.prevMD.SimplexEpochInfo.EpochNumber})
		if err != nil {
			return fmt.Errorf("failed retrieving block: %w", err)
		}
		hash := prevSealingBlock.Digest()
		if !bytes.Equal(in.proposedBlockMD.SimplexEpochInfo.PrevSealingBlockHash[:], hash[:]) {
			return fmt.Errorf("expected prev sealing block hash to be %x but got %x", hash, in.proposedBlockMD.SimplexEpochInfo.PrevSealingBlockHash)
		}
	default: // non-sealing blocks should have an empty previous sealing block hash
		if in.proposedBlockMD.SimplexEpochInfo.PrevSealingBlockHash != [32]byte{} {
			return fmt.Errorf("expected prev sealing block hash of a non sealing block to be empty but got %x", in.proposedBlockMD.SimplexEpochInfo.PrevSealingBlockHash)
		}
	}

	return nil
}

type vmBlockSeqVerifier struct {
	getBlock BlockRetriever
}

func (v *vmBlockSeqVerifier) Verify(in verificationInput) error {
	prev, next := in.prevMD.SimplexEpochInfo, in.proposedBlockMD.SimplexEpochInfo

	// If this is the first ever Simplex block, the PrevVMBlockSeq is simply the seq of the previous block.
	if prev.EpochNumber == 0 {
		if next.PrevVMBlockSeq != in.prevBlockSeq {
			return fmt.Errorf("expected PrevVMBlockSeq to be %d but got %d", in.prevBlockSeq, next.PrevVMBlockSeq)
		}
		return nil
	}

	md, err := simplex.ProtocolMetadataFromBytes(in.proposedBlockMD.SimplexProtocolMetadata)
	if err != nil {
		return fmt.Errorf("failed parsing protocol metadata: %w", err)
	}

	// Else, if the previous block has an inner block, we point to it.
	// Otherwise, we point to the parent block's previous VM block seq.
	prevBlock, _, err := v.getBlock(RetrievingOpts{Height: in.prevBlockSeq, Digest: md.Prev})
	if err != nil {
		return fmt.Errorf("failed retrieving block: %w", err)
	}

	expectedPrevVMBlockSeq := in.prevMD.SimplexEpochInfo.PrevVMBlockSeq

	if prevBlock.InnerBlock != nil {
		expectedPrevVMBlockSeq = in.prevBlockSeq
	}

	if next.PrevVMBlockSeq != expectedPrevVMBlockSeq {
		return fmt.Errorf("expected PrevVMBlockSeq to be %d but got %d", expectedPrevVMBlockSeq, next.PrevVMBlockSeq)
	}

	return nil
}

func areNextEpochApprovalsSignersSupersetOfApprovalsOfPrevBlock(prev SimplexEpochInfo, next SimplexEpochInfo) error {
	if prev.NextEpochApprovals == nil {
		return nil
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

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/ava-labs/simplex"
)

type verificationInput struct {
	prevMD                 StateMachineMetadata
	proposedBlockMD        StateMachineMetadata
	proposedBlockTimestamp time.Time
	hasInnerBlock          bool
	prevBlockSeq           uint64
	nextBlockType          blockType
	state                  state
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
	case blockTypeSealing:
		return vd.verifySealingBlock(prev, next)
	default:
		return vd.verifyEmptyValidationDescriptor(prev, next)
	}
}

func (vd *validationDescriptorVerifier) verifySealingBlock(_ SimplexEpochInfo, next SimplexEpochInfo) error {
	validators, err := vd.getValidatorSet(next.NextPChainReferenceHeight)
	if err != nil {
		return err
	}

	if !validators.Compare(next.BlockValidationDescriptor.AggregatedMembership.Members) {
		return fmt.Errorf("expected validator set specified at P-chain height %d does not match validator set encoded in new inner block", next.NextPChainReferenceHeight)
	}

	return nil
}

func (vd *validationDescriptorVerifier) verifyEmptyValidationDescriptor(_ SimplexEpochInfo, next SimplexEpochInfo) error {
	if next.BlockValidationDescriptor != nil {
		return fmt.Errorf("inner block validation descriptor should be nil but got %v", next.BlockValidationDescriptor)
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
	case blockTypeSealing:
		return nv.verifySealingBlock(prev, next, in.proposedBlockMD.AuxiliaryInfo)
	case blockTypeNormal:
		return nv.verifyNormal(prev, next, in.proposedBlockMD.AuxiliaryInfo)
	default:
		return nv.verifyEmptyNextEpochApprovals(prev, next)
	}
}

func (nv *nextEpochApprovalsVerifier) verifySealingBlock(prev SimplexEpochInfo, next SimplexEpochInfo, auxInfo *AuxiliaryInfo) error {
	if next.NextEpochApprovals == nil {
		return fmt.Errorf("next epoch approvals should not be nil for a sealing inner block")
	}

	validators, err := nv.getValidatorSet(next.NextPChainReferenceHeight)
	if err != nil {
		return err
	}

	err = nv.verifySignature(prev, next, auxInfo, validators)
	if err != nil {
		return err
	}

	canSeal, err := canSealBlock(validators, next)
	if err != nil {
		return err
	}

	if !canSeal {
		return fmt.Errorf("not enough approvals to seal inner block")
	}

	return nil
}

func (nv *nextEpochApprovalsVerifier) verifyNormal(prev SimplexEpochInfo, next SimplexEpochInfo, auxInfo *AuxiliaryInfo) error {
	if prev.NextPChainReferenceHeight == 0 {
		return nil
	}

	// Otherwise, prev.NextPChainReferenceHeight > 0, so this means we're collecting approvals

	if next.NextEpochApprovals == nil {
		return fmt.Errorf("next epoch approvals should not be nil for a sealing inner block")
	}

	validators, err := nv.getValidatorSet(next.NextPChainReferenceHeight)
	if err != nil {
		return err
	}

	err = nv.verifySignature(prev, next, auxInfo, validators)
	if err != nil {
		return err
	}

	if err := areNextEpochApprovalsSignersSupersetOfPrevBlock(prev, next); err != nil {
		return err
	}

	return nil
}

func areNextEpochApprovalsSignersSupersetOfPrevBlock(prev SimplexEpochInfo, next SimplexEpochInfo) error {
	// Make sure that previous signers are still there.
	prevSigners := bitmaskFromBytes(prev.NextEpochApprovals.NodeIDs)
	nextSigners := bitmaskFromBytes(next.NextEpochApprovals.NodeIDs)
	// Remove all bits in nextSigners from prevSigners
	prevSigners.Difference(&nextSigners)
	// If we have some bits left, it means there was a bit in prevSigners that wasn't in nextSigners
	if prevSigners.Len() > 0 {
		return fmt.Errorf("some signers from parent inner block are missing from next epoch approvals of proposed inner block")
	}
	return nil
}

func (nv *nextEpochApprovalsVerifier) verifyEmptyNextEpochApprovals(_ SimplexEpochInfo, next SimplexEpochInfo) error {
	if next.NextEpochApprovals != nil {
		return fmt.Errorf("next epoch approvals should be nil but got %v", next.NextEpochApprovals)
	}
	return nil
}

func canSealBlock(validators NodeBLSMappings, next SimplexEpochInfo) (bool, error) {
	totalWeight, err := computeTotalWeight(validators)
	if err != nil {
		return false, fmt.Errorf("failed computing total weight: %w", err)
	}

	approvingNodes := bitmaskFromBytes(next.NextEpochApprovals.NodeIDs)

	approvingWeight, err := computeApprovingWeight(validators, &approvingNodes)
	if err != nil {
		return false, err
	}

	if approvingWeight > math.MaxInt64 {
		return false, fmt.Errorf("approving weight is too large, overflows int64: %d", approvingWeight)
	}

	threshold := big.NewRat(2, 3)
	approvingRatio := big.NewRat(approvingWeight, totalWeight)

	canSeal := approvingRatio.Cmp(threshold) > 0
	return canSeal, nil
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

func (nv *nextEpochApprovalsVerifier) verifySignature(prev SimplexEpochInfo, next SimplexEpochInfo, auxinfo *AuxiliaryInfo, validators NodeBLSMappings) error {
	approvingNodes := bitmaskFromBytes(next.NextEpochApprovals.NodeIDs)
	publicKeys := make([][]byte, 0, len(validators))
	validators.ForEach(func(i int, nbm NodeBLSMapping) {
		if !approvingNodes.Contains(i) {
			return
		}
		publicKeys = append(publicKeys, nbm.BLSKey)
	})

	aggPK, err := nv.keyAggregator.AggregateKeys(publicKeys...)
	if err != nil {
		return fmt.Errorf("failed to aggregate public keys: %w", err)
	}

	pChainHeightBuff := pChainReferenceHeightAsBytes(prev)

	var bb bytes.Buffer
	bb.Write(pChainHeightBuff)
	if auxinfo != nil {
		bb.Write(auxinfo.Info)
	}

	if err := nv.sigVerifier.VerifySignature(next.NextEpochApprovals.Signature, bb.Bytes(), aggPK); err != nil {
		return fmt.Errorf("failed to verify signature: %w", err)
	}
	return nil
}

func pChainReferenceHeightAsBytes(prev SimplexEpochInfo) []byte {
	pChainHeight := prev.PChainReferenceHeight
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
	case blockTypeTelock, blockTypeSealing:
		if prev.NextPChainReferenceHeight != next.NextPChainReferenceHeight {
			return fmt.Errorf("expected P-chain reference height to be %d but got %d", prev.PChainReferenceHeight, next.PChainReferenceHeight)
		}
	case blockTypeNormal:
		return n.verifyNextPChainHeightNormal(in.prevMD, prev, next)
	case blockTypeNewEpoch:
		if next.NextPChainReferenceHeight != 0 {
			return fmt.Errorf("expected P-chain reference height to be 0 but got %d", next.PChainReferenceHeight)
		}
	default:
		return fmt.Errorf("unknown inner block type: %d", in.nextBlockType)
	}
	return nil
}

func (n *nextPChainReferenceHeightVerifier) verifyNextPChainHeightNormal(prevMD StateMachineMetadata, prev SimplexEpochInfo, next SimplexEpochInfo) error {
	if prev.NextPChainReferenceHeight > 0 {
		if next.NextPChainReferenceHeight != prev.NextPChainReferenceHeight {
			return fmt.Errorf("expected P-chain reference height to be %d but got %d", prev.NextPChainReferenceHeight, next.NextPChainReferenceHeight)
		}
		return nil
	}
	currentValidatorSet, err := n.getValidatorSet(prevMD.SimplexEpochInfo.PChainReferenceHeight)
	if err != nil {
		return err
	}

	newValidatorSet, err := n.getValidatorSet(next.NextPChainReferenceHeight)
	if err != nil {
		return err
	}

	if currentValidatorSet.Compare(newValidatorSet) {
		return nil
	}

	pChainHeight := n.getPChainHeight()

	if pChainHeight < next.NextPChainReferenceHeight {
		return fmt.Errorf("haven't reached P-chain height %d yet, current P-chain height is only %d", next.NextPChainReferenceHeight, pChainHeight)
	}

	return nil
}

type epochNumberVerifier struct{}

func (e *epochNumberVerifier) Verify(in verificationInput) error {
	prev, next := in.prevMD.SimplexEpochInfo, in.proposedBlockMD.SimplexEpochInfo

	if in.prevMD.SimplexEpochInfo.EpochNumber == 0 && in.proposedBlockMD.SimplexEpochInfo.EpochNumber != 1 {
		return fmt.Errorf("expected epoch number of the first inner block created to be 1 but got %d", next.EpochNumber)
	}

	if prev.EpochNumber != next.EpochNumber {
		return fmt.Errorf("expected epoch number to be %d but got %d", prev.EpochNumber, next.EpochNumber)
	}

	switch in.nextBlockType {
	case blockTypeNewEpoch:
		if prev.SealingBlockSeq != next.EpochNumber {
			return fmt.Errorf("expected epoch number to be %d but got %d", prev.SealingBlockSeq, next.EpochNumber)
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
	if prev.SealingBlockSeq != next.SealingBlockSeq {
		return fmt.Errorf("expected sealing inner block sequence number to be %d but got %d", prev.SealingBlockSeq, next.SealingBlockSeq)
	}

	switch in.nextBlockType {
	case blockTypeNewEpoch, blockTypeNormal:
		if next.SealingBlockSeq != 0 {
			return fmt.Errorf("expected sealing inner block sequence number to be 0 but got %d", next.SealingBlockSeq)
		}
	case blockTypeTelock:
		if next.SealingBlockSeq != prev.SealingBlockSeq {
			return fmt.Errorf("expected sealing inner block sequence number to be %d but got %d", prev.SealingBlockSeq, next.SealingBlockSeq)
		}
	case blockTypeSealing:
		md, err := simplex.ProtocolMetadataFromBytes(in.prevMD.SimplexProtocolMetadata)
		if err != nil {
			return fmt.Errorf("failed parsing protocol metadata: %w", err)
		}
		if next.SealingBlockSeq != md.Seq+1 {
			return fmt.Errorf("expected sealing inner block sequence number to be %d but got %d", md.Seq+1, next.SealingBlockSeq)
		}
	default:
		return fmt.Errorf("unknown inner block type: %d", in.nextBlockType)
	}

	return nil
}

type pChainHeightVerifier struct {
	getPChainHeight func() uint64
}

func (p *pChainHeightVerifier) Verify(in verificationInput) error {
	currentPChainHeight := p.getPChainHeight()

	if in.proposedBlockMD.PChainHeight > currentPChainHeight {
		return fmt.Errorf("invalid P-chain reference height (%d) is too big, expected to be ≤ %d",
			in.proposedBlockMD.PChainHeight, currentPChainHeight)
	}

	if in.prevMD.PChainHeight > in.proposedBlockMD.PChainHeight {
		return fmt.Errorf("invalid P-chain height (%d) is smaller than parent inner block's P-chain height (%d)",
			in.proposedBlockMD.PChainHeight, in.prevMD.PChainHeight)
	}

	return nil
}

type pChainReferenceHeightVerifier struct{}

func (p *pChainReferenceHeightVerifier) Verify(in verificationInput) error {
	prev, next := in.prevMD.SimplexEpochInfo, in.proposedBlockMD.SimplexEpochInfo

	switch in.nextBlockType {
	case blockTypeNewEpoch:
		if prev.NextPChainReferenceHeight != next.PChainReferenceHeight {
			return fmt.Errorf("expected P-chain reference height of the first inner block of epoch %d to be %d but got %d",
				prev.SealingBlockSeq, prev.NextPChainReferenceHeight, next.PChainReferenceHeight)
		}
	default:
		if prev.PChainReferenceHeight != next.PChainReferenceHeight {
			return fmt.Errorf("expected P-chain reference height to be %d but got %d", prev.PChainReferenceHeight, next.PChainReferenceHeight)
		}
	}

	return nil
}

type icmEpochInfoVerifier struct {
	getUpdates      func() upgradeConfig
	computeICMEpoch ICMEpochTransition
}

func (i *icmEpochInfoVerifier) Verify(in verificationInput) error {
	prevMD, nextMD := in.prevMD, in.proposedBlockMD
	expectedICMInfo := nextICMEpochInfo(prevMD, in.hasInnerBlock, i.getUpdates, i.computeICMEpoch, in.proposedBlockTimestamp)

	if !expectedICMInfo.Equal(&nextMD.ICMEpochInfo) {
		return fmt.Errorf("expected ICM epoch info to be %v but got %v", expectedICMInfo, nextMD.ICMEpochInfo)
	}

	return nil
}

type timestampVerifier struct{}

func (t *timestampVerifier) Verify(in verificationInput) error {
	expectedTimestamp := in.proposedBlockTimestamp.Unix()
	if expectedTimestamp != int64(in.proposedBlockMD.Timestamp) {
		return fmt.Errorf("expected timestamp to be %d but got %d", expectedTimestamp, int64(in.proposedBlockMD.Timestamp))
	}
	return nil
}

type prevSealingBlockHashVerifier struct {
	getBlock              BlockRetriever
	latestPersistedHeight uint64
}

func (p *prevSealingBlockHashVerifier) Verify(in verificationInput) error {
	prev, _ := in.prevMD.SimplexEpochInfo, in.proposedBlockMD.SimplexEpochInfo

	if prev.EpochNumber == 1 && in.nextBlockType == blockTypeSealing {
		firstEverSimplexBlockSeq, err := findFirstSimplexBlock(p.getBlock, p.latestPersistedHeight)
		if err != nil {
			return fmt.Errorf("failed to find first Simplex inner block: %w", err)
		}

		block, _, err := p.getBlock(firstEverSimplexBlockSeq)
		if err != nil {
			return fmt.Errorf("failed retrieving first ever simplex inner block %d: %w", firstEverSimplexBlockSeq, err)
		}

		hash := block.Digest()
		if !bytes.Equal(in.proposedBlockMD.SimplexEpochInfo.PrevSealingBlockHash[:], hash[:]) {
			return fmt.Errorf("expected prev sealing inner block hash of the first ever simplex inner block to be %s but got %s", hash, in.proposedBlockMD.SimplexEpochInfo.PrevSealingBlockHash)
		}

		return nil
	}

	switch in.nextBlockType {
	case blockTypeSealing:
		prevSealingBlock, _, err := p.getBlock(in.prevMD.SimplexEpochInfo.EpochNumber)
		if err != nil {
			return fmt.Errorf("failed retrieving inner block: %w", err)
		}
		hash := prevSealingBlock.Digest()
		if !bytes.Equal(in.proposedBlockMD.SimplexEpochInfo.PrevSealingBlockHash[:], hash[:]) {
			return fmt.Errorf("expected prev sealing inner block hash to be %s but got %s", hash, in.proposedBlockMD.SimplexEpochInfo.PrevSealingBlockHash)
		}
	default:
		if in.proposedBlockMD.SimplexEpochInfo.PrevSealingBlockHash != [32]byte{} {
			return fmt.Errorf("expected prev sealing inner block hash of a non sealing inner block to be empty but got %s", prev.PrevSealingBlockHash)
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

	// Else, if the previous block has an inner block, we point to it.
	// Otherwise, we point to the parent block's previous VM block seq.
	prevBlock, _, err := v.getBlock(in.prevBlockSeq)
	if err != nil {
		return fmt.Errorf("failed retrieving inner block: %w", err)
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

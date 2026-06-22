// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"fmt"

	"github.com/ava-labs/simplex/common"
	"go.uber.org/zap"
)

type approvalKey struct {
	pChainHeight  uint64
	auxInfoDigest [32]byte
}

type approvalsByPChainHeightAndAuxInfoDigest map[approvalKey]*approvalAndTimestamp

type approvalAndTimestamp struct {
	common.ValidatorSetApproval
	Timestamp uint64
}

type ApprovalStore struct {
	signatureVerifier SignatureVerifier
	validators        common.NodeBLSMappings
	logger            common.Logger
	pkByNodeID        map[common.NodeIdentifier][]byte
	approvalsByNodes  map[common.NodeIdentifier]approvalsByPChainHeightAndAuxInfoDigest
	storedCount       int
}

func NewApprovalStore(signatureVerifier SignatureVerifier, validators common.NodeBLSMappings, logger common.Logger) *ApprovalStore {
	pkByNodeID := make(map[common.NodeIdentifier][]byte)
	for _, vdr := range validators {
		pkByNodeID[vdr.NodeID] = vdr.BLSKey
	}

	approvalsByNodes := make(map[common.NodeIdentifier]approvalsByPChainHeightAndAuxInfoDigest, len(validators))
	for _, vdr := range validators {
		approvalsByNodes[vdr.NodeID] = make(approvalsByPChainHeightAndAuxInfoDigest)
	}

	return &ApprovalStore{
		signatureVerifier: signatureVerifier,
		validators:        validators,
		pkByNodeID:        pkByNodeID,
		logger:            logger,
		approvalsByNodes:  approvalsByNodes,
	}
}

func (as *ApprovalStore) Approvals() common.ValidatorSetApprovals {
	approvals := make(common.ValidatorSetApprovals, 0, as.storedCount)
	for _, approvalsByHeight := range as.approvalsByNodes {
		for _, approval := range approvalsByHeight {
			approvals = append(approvals, (*approval).ValidatorSetApproval)
		}
	}
	return approvals
}

func (as *ApprovalStore) HandleApproval(approval *common.ValidatorSetApproval, timestamp uint64) error {
	// First thing we check is if the node that sent this approval is a validator.
	pk, exists := as.getPKOfNode(approval.NodeID)
	if !exists {
		as.logger.Debug("Received an approval from a node that is not a validator", zap.String("nodeID",
			fmt.Sprintf("%x", approval.NodeID)), zap.Uint64("pChainHeight", approval.PChainHeight))
		return nil
	}

	// Second thing we check is if we already have an approval for this height from this node.
	if as.approvalExistsAndUpToDate(approval, timestamp) {
		as.logger.Debug("Already have an approval from the node", zap.String("nodeID",
			fmt.Sprintf("%x", approval.NodeID)), zap.Uint64("pChainHeight", approval.PChainHeight))
		return nil
	}

	// Third thing we check is if the signature of the approval is valid.
	// We need it to be valid in order for nodes to be able to aggregate it later on along with other approvals.
	if err := as.checkApprovalSignature(approval, pk); err != nil {
		as.logger.Debug("Received an approval with an invalid signature", zap.String("nodeID",
			fmt.Sprintf("%x", approval.NodeID)), zap.Uint64("pChainHeight", approval.PChainHeight))
		return nil
	}

	key := approvalKey{
		pChainHeight:  approval.PChainHeight,
		auxInfoDigest: approval.AuxInfoDigest,
	}

	// Store the approval.
	oldApproval := as.approvalsByNodes[approval.NodeID][key]
	as.approvalsByNodes[approval.NodeID][key] = &approvalAndTimestamp{
		ValidatorSetApproval: *approval,
		Timestamp:            timestamp,
	}

	if oldApproval == nil {
		as.storedCount++
	}

	// We only store the last |as.validators| of approvals for each node,
	// so we need to delete old approvals if we have more than |as.validators| approvals stored for this node.
	as.maybePruneOldApprovals(approval)

	return nil
}

func (as *ApprovalStore) maybePruneOldApprovals(approval *common.ValidatorSetApproval) {
	if len(as.approvalsByNodes[approval.NodeID]) <= len(as.validators) {
		return
	}
	// Find the oldest approval and delete it.
	var oldestApproval *approvalAndTimestamp
	for _, approval := range as.approvalsByNodes[approval.NodeID] {
		if oldestApproval == nil || approval.Timestamp < oldestApproval.Timestamp {
			oldestApproval = approval
		}
	}

	if oldestApproval != nil {
		key := approvalKey{
			pChainHeight:  oldestApproval.PChainHeight,
			auxInfoDigest: oldestApproval.AuxInfoDigest,
		}

		as.logger.Debug("Deleting old approval from node",
			zap.String("nodeID", fmt.Sprintf("%x", oldestApproval.NodeID)),
			zap.String("oldestApprovalPChainHeight",
				fmt.Sprintf("%d", oldestApproval.PChainHeight)), zap.Uint64("oldestApprovalTimestamp", oldestApproval.Timestamp))
		delete(as.approvalsByNodes[approval.NodeID], key)
		as.storedCount--
	}
}

func (as *ApprovalStore) checkApprovalSignature(approval *common.ValidatorSetApproval, pk []byte) error {
	toBeSigned, err := assembleApprovalToBeSigned(approval.PChainHeight, approval.AuxInfoDigest)
	if err != nil {
		return err
	}

	// We check if the signature is valid before we store the approval.
	return as.signatureVerifier.VerifySignature(approval.Signature, toBeSigned, pk)
}

func (as *ApprovalStore) getPKOfNode(nodeID common.NodeIdentifier) ([]byte, bool) {
	pk, exists := as.pkByNodeID[nodeID]
	return pk, exists
}

func (as *ApprovalStore) approvalExistsAndUpToDate(approval *common.ValidatorSetApproval, timestamp uint64) bool {
	if as.approvalsByNodes[approval.NodeID] == nil {
		return false
	}

	key := approvalKey{
		pChainHeight:  approval.PChainHeight,
		auxInfoDigest: approval.AuxInfoDigest,
	}

	existingApproval := as.approvalsByNodes[approval.NodeID][key]
	if existingApproval == nil {
		return false
	}

	return existingApproval.Timestamp >= timestamp
}

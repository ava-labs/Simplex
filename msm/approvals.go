// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"encoding/binary"
	"fmt"

	"github.com/ava-labs/simplex"
	"go.uber.org/zap"
)

type approvalsByPChainHeight map[uint64]*ValidatorSetApproval

type ApprovalStore struct {
	signatureVerifier SignatureVerifier
	validators        NodeBLSMappings
	logger            simplex.Logger
	pkByNodeID        map[nodeID][]byte
	approvalsByNodes  map[nodeID]approvalsByPChainHeight
	storedCount       int
}

func NewApprovalStore(signatureVerifier SignatureVerifier, validators NodeBLSMappings, logger simplex.Logger) *ApprovalStore {
	pkByNodeID := make(map[nodeID][]byte)
	for _, vdr := range validators {
		pkByNodeID[vdr.NodeID] = vdr.BLSKey
	}

	approvalsByNodes := make(map[nodeID]approvalsByPChainHeight, len(validators))
	for _, vdr := range validators {
		approvalsByNodes[vdr.NodeID] = make(approvalsByPChainHeight)
	}

	return &ApprovalStore{
		signatureVerifier: signatureVerifier,
		validators:        validators,
		pkByNodeID:        pkByNodeID,
		logger:            logger,
		approvalsByNodes:  approvalsByNodes,
	}
}

func (as *ApprovalStore) Approvals() ValidatorSetApprovals {
	approvals := make(ValidatorSetApprovals, 0, as.storedCount)
	for _, approvalsByHeight := range as.approvalsByNodes {
		for _, approval := range approvalsByHeight {
			approvals = append(approvals, *approval)
		}
	}
	return approvals
}

func (as *ApprovalStore) HandleApproval(approval *ValidatorSetApproval) error {
	// First thing we check is if the node that sent this approval is a validator.
	pk, exists := as.getPKOfNode(approval.NodeID)
	if !exists {
		as.logger.Debug("Received an approval from a node that is not a validator", zap.String("nodeID",
			fmt.Sprintf("%x", approval.NodeID)), zap.Uint64("pChainHeight", approval.PChainHeight))
		return nil
	}

	// Second thing we check is if we already have an approval for this height from this node.
	if as.approvalExistsAndUpToDate(approval) {
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

	// Store the approval.
	oldApproval := as.approvalsByNodes[approval.NodeID][approval.PChainHeight]
	as.approvalsByNodes[approval.NodeID][approval.PChainHeight] = approval

	if oldApproval == nil {
		as.storedCount++
	}

	// We only store the last |as.validators| of approvals for each node,
	// so we need to delete old approvals if we have more than |as.validators| approvals stored for this node.
	as.maybePruneOldApprovals(approval)

	return nil
}

func (as *ApprovalStore) maybePruneOldApprovals(approval *ValidatorSetApproval) {
	for len(as.approvalsByNodes[approval.NodeID]) > len(as.validators) {
		// Find the oldest approval and delete it.
		var oldestApproval *ValidatorSetApproval
		for _, approval := range as.approvalsByNodes[approval.NodeID] {
			if oldestApproval == nil || approval.Timestamp < oldestApproval.Timestamp {
				oldestApproval = approval
			}
		}

		if oldestApproval != nil {
			as.logger.Debug("Deleting old approval from node",
				zap.String("nodeID", fmt.Sprintf("%x", oldestApproval.NodeID)),
				zap.String("oldestApprovalPChainHeight",
					fmt.Sprintf("%d", oldestApproval.PChainHeight)), zap.Uint64("oldestApprovalTimestamp", oldestApproval.Timestamp))
			delete(as.approvalsByNodes[approval.NodeID], oldestApproval.PChainHeight)
			as.storedCount--
		}
	}
}

func (as *ApprovalStore) checkApprovalSignature(approval *ValidatorSetApproval, pk []byte) error {
	pChainHeight := approval.PChainHeight
	pChainHeightBuff := make([]byte, 8)
	binary.BigEndian.PutUint64(pChainHeightBuff, pChainHeight)

	// We check if the signature is valid before we store the approval.
	return as.signatureVerifier.VerifySignature(approval.Signature, pChainHeightBuff, pk)
}

func (as *ApprovalStore) getPKOfNode(nodeID nodeID) ([]byte, bool) {
	pk, exists := as.pkByNodeID[nodeID]
	return pk, exists
}

func (as *ApprovalStore) approvalExistsAndUpToDate(approval *ValidatorSetApproval) bool {
	if as.approvalsByNodes[approval.NodeID] == nil {
		return false
	}
	existingApproval := as.approvalsByNodes[approval.NodeID][approval.PChainHeight]
	if existingApproval == nil {
		return false
	}

	return existingApproval.Timestamp >= approval.Timestamp
}

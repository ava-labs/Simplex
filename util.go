// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"fmt"

	"go.uber.org/zap"
)

// RetrieveLastIndexFromStorage retrieves the latest block and fCert from storage.
// Returns an error if it cannot be retrieved but the storage has some block.
// Returns (nil, nil) if the storage is empty.
func RetrieveLastIndexFromStorage(s Storage) (Block, *FinalizationCertificate, error) {
	height := s.Height()
	if height == 0 {
		return nil, nil, nil
	}
	lastBlock, fCert, retrieved := s.Retrieve(height - 1)
	if !retrieved {
		return nil, nil, fmt.Errorf("failed retrieving last block from storage with seq %d", height-1)
	}
	return lastBlock, &fCert, nil
}

func IsFinalizationCertificateValid(eligibleSigners map[string]struct{}, fCert *FinalizationCertificate, quorumSize int, logger Logger) bool {
	valid := validateFinalizationQC(eligibleSigners, fCert, quorumSize, logger)
	if !valid {
		return false
	}
	if !valid {
		return false
	}

	return true
}

func validateFinalizationQC(eligibleSigners map[string]struct{}, fCert *FinalizationCertificate, quorumSize int, logger Logger) bool {
	if fCert.QC == nil {
		return false
	}

	// Check enough signers signed the finalization certificate
	if quorumSize > len(fCert.QC.Signers()) {
		logger.Debug("ToBeSignedFinalization certificate signed by insufficient nodes",
			zap.Int("count", len(fCert.QC.Signers())),
			zap.Int("Quorum", quorumSize))
		return false
	}

	doubleSigner, signedTwice := hasSomeNodeSignedTwice(fCert.QC.Signers(), logger)

	if signedTwice {
		logger.Debug("Finalization certificate signed twice by the same node", zap.Stringer("signer", doubleSigner))
		return false
	}

	// Finally, check that all signers are eligible of signing, and we don't have made up identities
	for _, signer := range fCert.QC.Signers() {
		if _, exists := eligibleSigners[string(signer)]; !exists {
			logger.Debug("Finalization Quorum Certificate contains an unknown signer", zap.Stringer("signer", signer))
			return false
		}
	}

	if err := fCert.Verify(); err != nil {
		return false
	}

	return true
}

func hasSomeNodeSignedTwice(nodeIDs []NodeID, logger Logger) (NodeID, bool) {
	seen := make(map[string]struct{}, len(nodeIDs))

	for _, nodeID := range nodeIDs {
		if _, alreadySeen := seen[string(nodeID)]; alreadySeen {
			logger.Warn("Observed a signature originating at least twice from the same node")
			return nodeID, true
		}
		seen[string(nodeID)] = struct{}{}
	}

	return NodeID{}, false
}

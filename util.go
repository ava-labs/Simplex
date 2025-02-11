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


func isFinalizationCertificateValid(fCert *FinalizationCertificate, quorumSize int, logger Logger) (bool, error) {
	valid, err := validateFinalizationQC(fCert, quorumSize, logger)
	if err != nil {
		return false, err
	}
	if !valid {
		return false, nil
	}

	return true, nil
}

func validateFinalizationQC(fCert *FinalizationCertificate, quorumSize int, logger Logger) (bool, error) {
	if fCert.QC == nil {
		return false, nil
	}

	// Check enough signers signed the finalization certificate
	if quorumSize > len(fCert.QC.Signers()) {
		logger.Debug("ToBeSignedFinalization certificate signed by insufficient nodes",
			zap.Int("count", len(fCert.QC.Signers())),
			zap.Int("Quorum", quorumSize))
		return false, nil
	}

	signedTwice := hasSomeNodeSignedTwice(fCert.QC.Signers(), logger)

	if signedTwice {
		return false, nil
	}

	if err := fCert.Verify(); err != nil {
		return false, nil
	}

	return true, nil
}

func hasSomeNodeSignedTwice(nodeIDs []NodeID, logger Logger) bool {
	seen := make(map[string]struct{}, len(nodeIDs))

	for _, nodeID := range nodeIDs {
		if _, alreadySeen := seen[string(nodeID)]; alreadySeen {
			logger.Warn("Observed a signature originating at least twice from the same node")
			return true
		}
		seen[string(nodeID)] = struct{}{}
	}

	return false
}

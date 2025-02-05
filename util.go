// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import "fmt"

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

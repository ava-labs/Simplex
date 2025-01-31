// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import "fmt"

// RetrieveLastBlockFromStorage retrieves the latest block from storage.
// Returns an error if it cannot be retrieved but the storage has some block.
// Returns (nil, nil) if the storage is empty.
func RetrieveLastBlockFromStorage(s Storage) (Block, error) {
	height := s.Height()
	if height == 0 {
		return nil, nil
	}

	lastBlock, _, retrieved := s.Retrieve(height - 1)
	if !retrieved {
		return nil, fmt.Errorf("failed retrieving last block from storage with seq %d", height-1)
	}
	return lastBlock, nil
}

// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"errors"
	. "simplex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetrieveFromStorage(t *testing.T) {
	brokenStorage := newInMemStorage()
	brokenStorage.data[41] = struct {
		Block
		FinalizationCertificate
	}{Block: newTestBlock(ProtocolMetadata{Seq: 41})}

	block := newTestBlock(ProtocolMetadata{Seq: 0})
	normalStorage := newInMemStorage()
	normalStorage.data[0] = struct {
		Block
		FinalizationCertificate
	}{Block: block}

	for _, testCase := range []struct {
		description   string
		storage       Storage
		expectedErr   error
		expectedBlock Block
	}{
		{
			description: "no blocks in storage",
			storage:     newInMemStorage(),
		},
		{
			description: "broken storage",
			storage:     brokenStorage,
			expectedErr: errors.New("failed retrieving last block from storage with seq 0"),
		},
		{
			description:   "normal storage",
			storage:       normalStorage,
			expectedBlock: block,
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			block, err := RetrieveLastBlockFromStorage(testCase.storage)
			require.Equal(t, testCase.expectedErr, err)
			require.Equal(t, testCase.expectedBlock, block)
		})
	}
}

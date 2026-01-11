// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal_test

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/ava-labs/simplex/testutil"
	wal "github.com/ava-labs/simplex/wal"
	"github.com/stretchr/testify/require"
)

type mockWALReader struct{}

func (m *mockWALReader) RetentionTerm(entry []byte) (uint64, error) {
	if len(entry) != 16 {
		return 0, fmt.Errorf("invalid entry size, expected 16, got %d", len(entry))
	}

	return binary.BigEndian.Uint64(entry[:8]), nil
}

func TestGarbageCollectedWAL(t *testing.T) {

	var testWALs []wal.DeleteableWAL
	records := make([][]byte, 10)

	for i := uint64(0); i < 10; i++ {
		buff := make([]byte, 16)
		binary.BigEndian.PutUint64(buff[:8], i)
		_, err := rand.Read(buff[8:])
		require.NoError(t, err)

		testWAL := testutil.NewTestWAL(t)
		require.NoError(t, testWAL.Append(buff))
		testWALs = append(testWALs, testWAL)
		records[i] = buff
	}

	gcw := wal.GarbageCollectedWAL{
		CreateWAL: func() (wal.DeleteableWAL, error) {
			return testutil.NewTestWAL(t), nil
		},
		WALs: testWALs,
	}

	require.ErrorContains(t, gcw.Append(nil), "WAL reader not initialized")

	require.NoError(t, gcw.Init(&mockWALReader{}))

	walRecords, err := gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, records, walRecords)

	require.NoError(t, gcw.Truncate(6))
	walRecords, err = gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, records[6:], walRecords)

	require.NoError(t, gcw.Truncate(10))
	walRecords, err = gcw.ReadAll()
	require.NoError(t, err)
	require.Empty(t, walRecords)

	buff := make([]byte, 16)
	binary.BigEndian.PutUint64(buff[:8], 100)
	_, err = rand.Read(buff[8:])
	require.NoError(t, err)

	require.NoError(t, gcw.Append(buff))
	walRecords, err = gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, [][]byte{buff}, walRecords)
}

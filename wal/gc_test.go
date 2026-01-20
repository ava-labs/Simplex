// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal_test

import (
	"crypto/rand"
	"encoding/binary"
	"testing"

	"github.com/ava-labs/simplex/testutil"
	wal "github.com/ava-labs/simplex/wal"
	"github.com/stretchr/testify/require"
)

type mockWALReader struct{}

func (m *mockWALReader) RetentionTerm(entry []byte) (uint64, error) {
	return binary.BigEndian.Uint64(entry[:8]), nil
}

func TestGarbageCollectedWAL(t *testing.T) {

	var testWALs []wal.DeletableWAL
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

	var newWALCreatedCount int

	gcw, err := wal.NewGarbageCollectedWAL(testWALs, func() (wal.DeletableWAL, error) {
		newWALCreatedCount++
		return testutil.NewTestWAL(t), nil
	}, &mockWALReader{}, 100)

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

	require.Zero(t, newWALCreatedCount)
	require.NoError(t, gcw.Append(buff))
	require.Equal(t, 1, newWALCreatedCount)
	walRecords, err = gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, [][]byte{buff}, walRecords)

	buff1 := buff
	buff = make([]byte, 1024)
	binary.BigEndian.PutUint64(buff[:8], 100)
	_, err = rand.Read(buff[8:])
	require.NoError(t, err)

	buffs := make([][]byte, 2)
	buffs[0] = buff1
	buffs[1] = buff

	require.Equal(t, 1, newWALCreatedCount)
	require.NoError(t, gcw.Append(buff))
	walRecords, err = gcw.ReadAll()
	require.Equal(t, 2, newWALCreatedCount)
	require.Equal(t, buffs, walRecords)
}

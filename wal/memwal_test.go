// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInMemWAL(t *testing.T) {
	require := require.New(t)

	r1 := []byte{4, 5, 6}
	r2 := []byte{10, 11, 12}

	wal := NewMemWAL(t)
	require.NoError(wal.Append(r1))
	require.NoError(wal.Append(r2))

	readRecords, err := wal.ReadAll()
	require.NoError(err)
	require.Equal([][]byte{r1, r2}, readRecords)
}

// TestInMemWALCompact asserts that the in-memory log drops rejected records once the
// compaction threshold is reached, and not before.
func TestInMemWALCompact(t *testing.T) {
	require := require.New(t)

	wal := NewMemWAL(t)
	require.NoError(wal.Append([]byte{1}))
	require.NoError(wal.Append([]byte{2}))

	require.NoError(wal.Compact(func([]byte) bool { return false }))
	readRecords, err := wal.ReadAll()
	require.NoError(err)
	require.Len(readRecords, 2)

	wal.CompactAt = 1
	require.NoError(wal.Compact(func(record []byte) bool { return record[0] == 2 }))
	readRecords, err = wal.ReadAll()
	require.NoError(err)
	require.Equal([][]byte{{2}}, readRecords)
}

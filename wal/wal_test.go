// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func new(t *testing.T) *WriteAheadLog {
	fileName := filepath.Join(t.TempDir(), "simplex.wal")
	wal, err := New(fileName)
	require.NoError(t, err)
	return wal
}

func TestWalSingleRw(t *testing.T) {
	require := require.New(t)

	r := []byte{3, 4, 5}

	// writes and reads from wal
	wal := new(t)
	defer func() {
		require.NoError(wal.Close())
	}()

	require.NoError(wal.Append(r))

	readRecords, err := wal.ReadAll()
	require.NoError(err)
	require.Equal(
		[][]byte{r},
		readRecords,
	)
}

func TestWalMultipleRws(t *testing.T) {
	require := require.New(t)

	r1 := []byte{3, 4, 5}
	r2 := []byte{1, 2, 3}
	records := [][]byte{r1, r2}

	wal := new(t)
	defer func() {
		require.NoError(wal.Close())
	}()

	require.NoError(wal.Append(r1))
	require.NoError(wal.Append(r2))

	readRecords, err := wal.ReadAll()
	require.NoError(err)
	require.Equal(records, readRecords)
}

func TestWalAppendAfterRead(t *testing.T) {
	require := require.New(t)

	r1 := []byte{3, 4, 5}
	r2 := []byte{1, 2, 3}
	records := [][]byte{r1, r2}

	wal := new(t)
	defer func() {
		require.NoError(wal.Close())
	}()

	require.NoError(wal.Append(r1))

	readRecords, err := wal.ReadAll()
	require.NoError(err)
	require.Equal(records[:1], readRecords)

	require.NoError(wal.Append(r2))

	readRecords, err = wal.ReadAll()
	require.NoError(err)
	require.Equal(records, readRecords)
}

// Write 3 records, corrupt 4th
func TestCorruptedFile(t *testing.T) {
	require := require.New(t)

	fileName := filepath.Join(t.TempDir(), "simplex.wal")
	wal, err := New(fileName)
	require.NoError(err)
	defer func() {
		require.NoError(wal.Close())
	}()

	const n = 4
	records := make([][]byte, n)
	for i := range records {
		records[i] = []byte{byte(i), byte(i), byte(i)}
		require.NoError(wal.Append(records[i]))
	}

	// Corrupt the last record
	file, err := os.OpenFile(fileName, os.O_RDWR, 0666)
	require.NoError(err)

	recordSize := recordSizeLen + len(records[0]) + recordChecksumLen
	_, err = file.WriteAt([]byte{0, 1, 2}, int64(3*recordSize))
	require.NoError(err)

	// Close the file to ensure the changes are flushed
	require.NoError(file.Close())

	// Because the last record is corrupted, it should not be read
	readRecords, err := wal.ReadAll()
	require.NoError(err)
	require.Equal(records[:n-1], readRecords)
}

func TestTruncate(t *testing.T) {
	require := require.New(t)

	r := []byte{3, 4, 5}

	wal := new(t)
	defer func() {
		require.NoError(wal.Close())
	}()

	require.NoError(wal.Append(r))
	require.NoError(wal.Truncate())

	readRecords, err := wal.ReadAll()
	require.NoError(err)
	require.Empty(readRecords)
}

func TestReadWriteAfterTruncate(t *testing.T) {
	require := require.New(t)

	r := []byte{3, 4, 5}

	wal := new(t)
	defer func() {
		require.NoError(wal.Close())
	}()

	require.NoError(wal.Append(r))

	readRecords, err := wal.ReadAll()
	require.NoError(err)
	require.Equal(
		[][]byte{r},
		readRecords,
	)

	require.NoError(wal.Truncate())

	readRecords, err = wal.ReadAll()
	require.NoError(err)
	require.Empty(readRecords)

	require.NoError(wal.Append(r))

	readRecords, err = wal.ReadAll()
	require.NoError(err)
	require.Equal(
		[][]byte{r},
		readRecords,
	)
}

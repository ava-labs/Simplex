// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func new(t *testing.T) *WriteAheadLog {
	fileName := filepath.Join(t.TempDir(), "simplex.wal")
	wal := New(fileName)
	return wal
}

func TestWalSingleRw(t *testing.T) {
	require := require.New(t)

	r := []byte{3, 4, 5}

	// writes and reads from wal
	wal := new(t)
	defer func() {
		// Close twice to make sure we can do it
		require.NoError(wal.Close())
		require.NoError(wal.Close())
	}()

	// Close before appending just to make sure we can do it
	require.NoError(wal.Close())

	require.NoError(wal.Append(r))

	// Close before reading just to make sure we can do it
	require.NoError(wal.Close())

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
	wal := New(fileName)
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

// TestPartiallyWrittenRecord covers a crash in the middle of an Append: the record's
// length prefix made it to disk but its payload and checksum did not. ReadAll must return
// the records that were written in full and truncate the torn tail away.
func TestPartiallyWrittenRecord(t *testing.T) {
	require := require.New(t)

	fileName := filepath.Join(t.TempDir(), "simplex.wal")
	wal := New(fileName)
	defer func() {
		require.NoError(wal.Close())
	}()

	const n = 3
	records := make([][]byte, n)
	for i := range records {
		records[i] = []byte{byte(i), byte(i), byte(i)}
		require.NoError(wal.Append(records[i]))
	}

	recordSize := recordSizeLen + len(records[0]) + recordChecksumLen

	// Append a torn record: a prefix declaring a 3 byte payload, followed by only 2 of
	// the 11 bytes that should follow it.
	torn := make([]byte, recordSizeLen)
	binary.BigEndian.PutUint32(torn, 3)
	torn = append(torn, 0xAA, 0xBB)

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0666)
	require.NoError(err)
	_, err = file.Write(torn)
	require.NoError(err)
	require.NoError(file.Close())

	readRecords, err := wal.ReadAll()
	require.NoError(err)
	require.Equal(records, readRecords)

	// The torn tail must be gone, so the next Append starts from a clean boundary.
	info, err := os.Stat(fileName)
	require.NoError(err)
	require.Equal(int64(n*recordSize), info.Size())

	require.NoError(wal.Append([]byte{9, 9, 9}))
	readRecords, err = wal.ReadAll()
	require.NoError(err)
	require.Equal(append(records, []byte{9, 9, 9}), readRecords)
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
}

// TestCompact asserts that Compact keeps only the accepted records, removes its temporary
// file, and leaves a log that can still be appended to and read back.
func TestCompact(t *testing.T) {
	require := require.New(t)

	wal := new(t)
	defer func() {
		require.NoError(wal.Close())
	}()
	wal.compactAt = 1

	records := [][]byte{{1}, {2}, {3}, {4}}
	for _, r := range records {
		require.NoError(wal.Append(r))
	}

	require.NoError(wal.Compact(func(record []byte) bool {
		return record[0]%2 == 0
	}))

	_, err := os.Stat(wal.fileName + ".tmp")
	require.ErrorIs(err, os.ErrNotExist)

	readRecords, err := wal.ReadAll()
	require.NoError(err)
	require.Equal([][]byte{{2}, {4}}, readRecords)

	require.NoError(wal.Append([]byte{5}))
	readRecords, err = wal.ReadAll()
	require.NoError(err)
	require.Equal([][]byte{{2}, {4}, {5}}, readRecords)
}

// TestCompactBelowThreshold asserts that Compact leaves the log alone until enough
// bytes have been appended since the last compaction.
func TestCompactBelowThreshold(t *testing.T) {
	require := require.New(t)

	wal := new(t)
	defer func() {
		require.NoError(wal.Close())
	}()

	require.NoError(wal.Append([]byte{1}))
	require.NoError(wal.Compact(func([]byte) bool { return false }))

	readRecords, err := wal.ReadAll()
	require.NoError(err)
	require.Equal([][]byte{{1}}, readRecords)
}

// TestCompactCountsExistingBytes asserts that a reopened log counts its existing size
// toward the compaction threshold, so a log bloated before a crash is compacted.
func TestCompactCountsExistingBytes(t *testing.T) {
	require := require.New(t)

	fileName := filepath.Join(t.TempDir(), "simplex.wal")
	wal := New(fileName)
	require.NoError(wal.Append([]byte{1}))
	require.NoError(wal.Append([]byte{2}))
	require.NoError(wal.Close())

	reopened := New(fileName)
	defer func() {
		require.NoError(reopened.Close())
	}()
	reopened.compactAt = 1
	require.NoError(reopened.Compact(func(record []byte) bool { return record[0] == 2 }))

	readRecords, err := reopened.ReadAll()
	require.NoError(err)
	require.Equal([][]byte{{2}}, readRecords)
}

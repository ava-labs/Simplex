// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"os"
	"simplex/record"
	"testing"

	"github.com/stretchr/testify/require"
)

func new(t *testing.T) *WriteAheadLog {
	fileName := t.TempDir() + "/simplex.wal"
	wal, err := New(fileName)
	require.NoError(t, err)
	return wal
}

func TestWalSingleRw(t *testing.T) {
	require := require.New(t)

	r := record.Record{
		Version: 1,
		Type:    2,
		Size:    3,
		Payload: []byte{3, 4, 5},
	}

	// writes and reads from wal
	wal := new(t)
	defer func() {
		require.NoError(wal.Close())
	}()

	require.NoError(wal.Append(&r))

	readRecords, err := wal.ReadAll()
	require.NoError(err)
	require.Equal(
		[]record.Record{r},
		readRecords,
	)
}

func TestWalMultipleRws(t *testing.T) {
	require := require.New(t)

	record1 := record.Record{
		Version: 1,
		Type:    2,
		Size:    3,
		Payload: []byte{3, 4, 5},
	}

	record2 := record.Record{
		Version: 3,
		Type:    3,
		Size:    3,
		Payload: []byte{1, 2, 3},
	}
	records := []record.Record{record1, record2}

	wal := new(t)
	defer func() {
		require.NoError(wal.Close())
	}()

	require.NoError(wal.Append(&record1))
	require.NoError(wal.Append(&record2))

	readRecords, err := wal.ReadAll()
	require.NoError(err)
	require.Equal(records, readRecords)
}

func TestWalAppendAfterRead(t *testing.T) {
	require := require.New(t)

	record1 := record.Record{
		Version: 1,
		Type:    2,
		Size:    3,
		Payload: []byte{3, 4, 5},
	}
	record2 := record.Record{
		Version: 3,
		Type:    3,
		Size:    3,
		Payload: []byte{1, 2, 3},
	}
	records := []record.Record{record1, record2}

	wal := new(t)
	defer func() {
		require.NoError(wal.Close())
	}()

	require.NoError(wal.Append(&record1))

	readRecords, err := wal.ReadAll()
	require.NoError(err)
	require.Equal(records[:1], readRecords)

	require.NoError(wal.Append(&record2))

	readRecords, err = wal.ReadAll()
	require.NoError(err)
	require.Equal(records, readRecords)
}

// Write 3 records, corrupt 4th
func TestCorruptedFile(t *testing.T) {
	require := require.New(t)

	fileName := t.TempDir() + "/simplex.wal"
	wal, err := New(fileName)
	require.NoError(err)
	defer func() {
		require.NoError(wal.Close())
	}()

	n := 4
	records := make([]record.Record, n)
	for i := 0; i < n; i++ {
		records[i] = record.Record{
			Version: uint8(i),
			Type:    uint16(i),
			Size:    3,
			Payload: []byte{byte(i), byte(i), byte(i)},
		}
		require.NoError(wal.Append(&records[i]))
	}

	// Corrupt the last record
	file, err := os.OpenFile(fileName, os.O_RDWR, 0666)
	require.NoError(err)

	recordSize := len(records[0].Bytes())
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

	r := record.Record{
		Version: 1,
		Type:    2,
		Size:    3,
		Payload: []byte{3, 4, 5},
	}

	wal := new(t)
	defer func() {
		require.NoError(wal.Close())
	}()

	require.NoError(wal.Append(&r))
	require.NoError(wal.Truncate())

	readRecords, err := wal.ReadAll()
	require.NoError(err)
	require.Empty(readRecords)
}

func TestReadWriteAfterTruncate(t *testing.T) {
	require := require.New(t)

	r := record.Record{
		Version: 1,
		Type:    2,
		Size:    3,
		Payload: []byte{3, 4, 5},
	}

	wal := new(t)
	defer func() {
		require.NoError(wal.Close())
	}()

	require.NoError(wal.Append(&r))

	readRecords, err := wal.ReadAll()
	require.NoError(err)
	require.Equal(
		[]record.Record{r},
		readRecords,
	)

	require.NoError(wal.Truncate())

	readRecords, err = wal.ReadAll()
	require.NoError(err)
	require.Empty(readRecords)

	require.NoError(wal.Append(&r))

	readRecords, err = wal.ReadAll()
	require.NoError(err)
	require.Equal(
		[]record.Record{r},
		readRecords,
	)
}

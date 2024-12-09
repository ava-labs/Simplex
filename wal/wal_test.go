// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"os"
	"simplex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWalSingleRw(t *testing.T) {
	require := require.New(t)
	deleteWal()

	record := simplex.Record{
		Version: 1,
		Type:    2,
		Size:    3,
		Payload: []byte{3, 4, 5},
	}

	// writes and reads from wal
	wal, err := New()
	require.NoError(err)

	defer func() {
		err := wal.Close()
		require.NoError(err)
	}()

	err = wal.Append(&record)
	require.NoError(err)

	records, err := wal.ReadAll()
	require.NoError(err)
	require.Len(records, 1)
	require.Equal(record, records[0])
}

func TestWalMultipleRws(t *testing.T) {
	require := require.New(t)
	deleteWal()

	record1 := simplex.Record{
		Version: 1,
		Type:    2,
		Size:    3,
		Payload: []byte{3, 4, 5},
	}

	record2 := simplex.Record{
		Version: 3,
		Type:    3,
		Size:    3,
		Payload: []byte{1, 2, 3},
	}

	wal, err := New()
	require.NoError(err)

	defer func() {
		err := wal.Close()
		require.NoError(err)
	}()

	err = wal.Append(&record1)
	require.NoError(err)

	err = wal.Append(&record2)
	require.NoError(err)

	records, err := wal.ReadAll()
	require.NoError(err)
	require.Len(records, 2)
	require.Equal(record1, records[0])
	require.Equal(record2, records[1])
}

func TestWalAppendAfterRead(t *testing.T) {
	require := require.New(t)
	deleteWal()

	record1 := simplex.Record{
		Version: 1,
		Type:    2,
		Size:    3,
		Payload: []byte{3, 4, 5},
	}

	record2 := simplex.Record{
		Version: 3,
		Type:    3,
		Size:    3,
		Payload: []byte{1, 2, 3},
	}

	wal, err := New()
	require.NoError(err)

	defer func() {
		err := wal.Close()
		require.NoError(err)
	}()

	err = wal.Append(&record1)
	require.NoError(err)

	records, err := wal.ReadAll()
	require.NoError(err)
	require.Len(records, 1)
	require.Equal(record1, records[0])

	err = wal.Append(&record2)
	require.NoError(err)

	records, err = wal.ReadAll()
	require.NoError(err)
	require.Len(records, 2)
	require.Equal(record1, records[0])
	require.Equal(record2, records[1])
}

// Write 3 records, corrupt 4th
func TestCorruptedFile(t *testing.T) {
	require := require.New(t)
	deleteWal()

	wal, err := New()
	require.NoError(err)

	defer func() {
		err := wal.Close()
		require.NoError(err)
	}()

	n := 4

	records := make([]simplex.Record, n)
	for i := 0; i < n; i++ {
		records[i] = simplex.Record{
			Version: uint8(i),
			Type:    uint16(i),
			Size:    3,
			Payload: []byte{byte(i), byte(i), byte(i)},
		}

		err = wal.Append(&records[i])
		require.NoError(err)
	}

	records, err = wal.ReadAll()
	require.NoError(err)
	require.Len(records, n)

	// Corrupt k records
	file, err := os.OpenFile(WalFilename+WalExtension, os.O_RDWR, 0666)
	require.NoError(err)

	recordSize := len(records[0].Bytes())
	_, err = file.WriteAt([]byte{0, 1, 2}, int64(3*recordSize))
	require.NoError(err)

	err = file.Close()
	require.NoError(err)

	records, err = wal.ReadAll()
	require.NoError(err)
	require.Len(records, n-1)

	for i := 0; i < n-1; i++ {
		require.Equal(records[i], records[i])
	}
}

func TestTruncate(t *testing.T) {
	require := require.New(t)
	deleteWal()

	record := simplex.Record{
		Version: 1,
		Type:    2,
		Size:    3,
		Payload: []byte{3, 4, 5},
	}

	wal, err := New()
	require.NoError(err)

	defer func() {
		err := wal.Close()
		require.NoError(err)
	}()

	err = wal.Append(&record)
	require.NoError(err)

	err = wal.Truncate()
	require.NoError(err)

	records, err := wal.ReadAll()
	require.NoError(err)
	require.Len(records, 0)
}

func TestReadWriteAfterTruncate(t *testing.T) {
	require := require.New(t)
	deleteWal()

	record := simplex.Record{
		Version: 1,
		Type:    2,
		Size:    3,
		Payload: []byte{3, 4, 5},
	}

	wal, err := New()
	require.NoError(err)

	defer func() {
		err := wal.Close()
		require.NoError(err)
	}()

	err = wal.Append(&record)
	require.NoError(err)

	records, err := wal.ReadAll()
	require.NoError(err)
	require.Len(records, 1)
	require.Equal(record, records[0])

	err = wal.Truncate()
	require.NoError(err)

	records, err = wal.ReadAll()
	require.NoError(err)
	require.Len(records, 0)

	err = wal.Append(&record)
	require.NoError(err)

	records, err = wal.ReadAll()
	require.NoError(err)
	require.Len(records, 1)
	require.Equal(record, records[0])
}

// deleteWal removes the wal file. It is used for testing purposes to ensure
// the wal file is empty before each test.
func deleteWal() error {
	return os.Remove(WalFilename + WalExtension)
}

package wal

import (
	"simplex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWalSingleRw(t *testing.T) {
	require := require.New(t)

	record := simplex.Record{
		Version: 1,
		Type:    2,
		Size:    3,
		Payload: []byte{3, 4, 5},
	}

	// writes and reads from wal
	wal, err := New()
	require.NoError(err)

	defer func(){
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

	defer func(){
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

func TestWalMultipleReads(t *testing.T) {
	require := require.New(t)

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

	defer func(){
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

	records, err = wal.ReadAll()
	require.NoError(err)
	require.Len(records, 2)
	require.Equal(record1, records[0])
	require.Equal(record2, records[1])
}

func TestWalAppendAfterRead(t *testing.T) {
	require := require.New(t)

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

	defer func(){
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

// write -> corrupt -> read -> write -> read
func TestCorruptedFile(t *testing.T) {
	require := require.New(t)

	record := simplex.Record{
		Version: 1,
		Type:    2,
		Size:    3,
		Payload: []byte{3, 4, 5},
	}

	wal, err := New()
	require.NoError(err)

	defer func(){
		err := wal.Close()
		require.NoError(err)
	}()


	err = wal.Append(&record)
	require.NoError(err)

	records, err := wal.ReadAll()
	require.NoError(err)
	require.Len(records, 1)
	require.Equal(record, records[0])
		

	// Corrupt the file
	err = wal.file.Truncate(1)
	require.NoError(err)

	records, err = wal.ReadAll()
	require.ErrorIs(err, ErrReadingRecord)
	require.Len(records, 0)

	// Append a new record
	err = wal.Append(&record)
	require.NoError(err)

	// Should still fail
	records, err = wal.ReadAll()
	require.ErrorIs(err, ErrReadingRecord)
	require.Len(records, 0)
}
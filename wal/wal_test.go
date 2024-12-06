package wal

import (
	"simplex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWaL(t *testing.T) {
	require := require.New(t)

	record := simplex.Record{
		Version: 1,
		Type:    2,
		Size:    3,
		Payload: []byte{3, 4, 5},
	}

	// writes and reads from wal
	var wal WriteAheadLog
	err := wal.Append(&record)
	require.NoError(err)

	records, err := wal.ReadAll()
	require.NoError(err)
	require.Len(records, 1)
	require.Equal(record, records[0])
}
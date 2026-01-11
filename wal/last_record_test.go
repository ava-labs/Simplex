// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ava-labs/simplex/record"
	"github.com/ava-labs/simplex/testutil"
	"github.com/ava-labs/simplex/wal"
	"github.com/stretchr/testify/require"
)

type mockTruncateableWAL struct {
	t *testing.T
	*testutil.TestWAL
}

func (m *mockTruncateableWAL) Truncate(uint64) error {
	m.TestWAL = testutil.NewTestWAL(m.t)
	return nil
}

func TestLastRecord(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)

	path := filepath.Join(dir, "last-record")

	testWAL, err := wal.NewLastRecordStoringWAL(path,
		&mockTruncateableWAL{
			t:       t,
			TestWAL: testutil.NewTestWAL(t),
		},
		record.NotarizationRecordType,
	)
	require.NoError(t, err)

	entry1 := []byte{0, byte(record.NotarizationRecordType), 1, 2, 3, 4, 5}
	entry2 := []byte{0, byte(record.EmptyNotarizationRecordType), 6, 7, 8, 9, 10}
	entry3 := []byte{0, byte(record.NotarizationRecordType), 11, 12, 13, 14, 15}

	require.NoError(t, testWAL.Append(entry1))
	require.NoError(t, testWAL.Append(entry2))

	lastRecord := testWAL.LastRecord()
	require.Equal(t, entry1, lastRecord)

	require.NoError(t, testWAL.Append(entry3))

	lastRecord = testWAL.LastRecord()
	require.Equal(t, entry3, lastRecord)

	entries, err := testWAL.ReadAll()
	require.NoError(t, err)
	require.Equal(t, [][]byte{entry1, entry2, entry3}, entries)

	require.NoError(t, testWAL.Truncate(0))

	testWAL, err = wal.NewLastRecordStoringWAL(path,
		&mockTruncateableWAL{
			t:       t,
			TestWAL: testutil.NewTestWAL(t),
		},
		record.NotarizationRecordType,
	)
	require.NoError(t, err)

	entries, err = testWAL.ReadAll()
	require.NoError(t, err)
	require.Empty(t, entries)

	require.Equal(t, entry3, testWAL.LastRecord())
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFileStoreDiscardBefore asserts that DiscardBefore removes only the logs of
// earlier epochs, keeps the records of the surviving ones, and leaves unrelated files alone.
func TestFileStoreDiscardBefore(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	store, err := NewFileStore(dir)
	require.NoError(t, err)

	for _, epoch := range []uint64{3, 5, 7} {
		w, err := store.Open(epoch)
		require.NoError(t, err)
		require.NoError(t, w.Append([]byte{byte(epoch)}))
		require.NoError(t, w.Close())
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("keep"), 0600))

	require.NoError(t, store.DiscardBefore(5))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	require.ElementsMatch(t, []string{"5.wal", "7.wal", "notes.txt"}, names)

	w, err := store.Open(5)
	require.NoError(t, err)
	records, err := w.ReadAll()
	require.NoError(t, err)
	require.Equal(t, [][]byte{{5}}, records)
	require.NoError(t, w.Close())

	// A discarded epoch reopens as an empty log.
	w, err = store.Open(3)
	require.NoError(t, err)
	records, err = w.ReadAll()
	require.NoError(t, err)
	require.Empty(t, records)
	require.NoError(t, w.Close())
}

// TestFileStorePermissions asserts that the directory and the logs it holds
// grant no access to group or others, whatever the umask.
func TestFileStorePermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	store, err := NewFileStore(dir)
	require.NoError(t, err)

	w, err := store.Open(1)
	require.NoError(t, err)
	require.NoError(t, w.Append([]byte{1}))
	require.NoError(t, w.Close())

	dirInfo, err := os.Stat(dir)
	require.NoError(t, err)
	require.Zero(t, dirInfo.Mode().Perm()&0077, "directory mode %v grants group or other access", dirInfo.Mode())

	fileInfo, err := os.Stat(filepath.Join(dir, "1.wal"))
	require.NoError(t, err)
	require.Zero(t, fileInfo.Mode().Perm()&0077, "file mode %v grants group or other access", fileInfo.Mode())
}

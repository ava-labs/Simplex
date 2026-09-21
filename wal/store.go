// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ava-labs/simplex/common"
)

const (
	fileSuffix     = ".wal"
	dirPermissions = 0700
)

// Store hands out one write ahead log per epoch.
// An epoch only ever replays its own log, so the logs of earlier epochs
// are garbage once the ledger has moved past them.
type Store interface {
	// Open opens the write ahead log of the given epoch, creating it if it does not exist.
	Open(epoch uint64) (common.WriteAheadLog, error)
	// DiscardBefore deletes the write ahead logs of all epochs before the given one.
	DiscardBefore(epoch uint64) error
}

// FileStore keeps the write ahead log of each epoch in its own file,
// named after the epoch, inside a single directory.
type FileStore struct {
	dir string
}

// NewFileStore creates the directory if it does not exist.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, dirPermissions); err != nil {
		return nil, fmt.Errorf("error creating WAL directory %s: %w", dir, err)
	}
	return &FileStore{dir: dir}, nil
}

func (fs *FileStore) Open(epoch uint64) (common.WriteAheadLog, error) {
	return New(filepath.Join(fs.dir, strconv.FormatUint(epoch, 10)+fileSuffix)), nil
}

func (fs *FileStore) DiscardBefore(epoch uint64) error {
	entries, err := os.ReadDir(fs.dir)
	if err != nil {
		return fmt.Errorf("error reading WAL directory %s: %w", fs.dir, err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, fileSuffix) {
			continue
		}
		fileEpoch, err := strconv.ParseUint(strings.TrimSuffix(name, fileSuffix), 10, 64)
		if err != nil || fileEpoch >= epoch {
			continue
		}
		if err := os.Remove(filepath.Join(fs.dir, name)); err != nil {
			return fmt.Errorf("error removing WAL %s: %w", name, err)
		}
	}
	return nil
}

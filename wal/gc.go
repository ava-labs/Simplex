// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"encoding/base64"
	"fmt"

	"github.com/ava-labs/simplex"
)

type Reader interface {
	RetentionTerm([]byte) (uint64, error)
}

type TruncateableWAL interface {
	DeleteableWAL
	Truncate(retentionTerm uint64) error
}

type DeleteableWAL interface {
	simplex.WriteAheadLog
	Delete() error
}

type GarbageCollectedWAL struct {
	WALs      []DeleteableWAL
	CreateWAL func() (DeleteableWAL, error)

	walReader      Reader
	retentionTerms []uint64
}

func (gcw *GarbageCollectedWAL) Init(reader Reader) error {
	gcw.walReader = reader

	for i, wal := range gcw.WALs {
		entries, err := wal.ReadAll()
		if err != nil {
			return fmt.Errorf("error reading entries from WAL %d: %v", i, err)
		}

		var highestRetentionTerm uint64

		for _, entry := range entries {
			rt, err := gcw.walReader.RetentionTerm(entry)
			if err != nil {
				data := base64.StdEncoding.EncodeToString(entry)
				return fmt.Errorf("error reading retention term of entry %s: %v", data, err)
			}
			if rt > highestRetentionTerm {
				highestRetentionTerm = rt
			}
		}
		gcw.retentionTerms = append(gcw.retentionTerms, highestRetentionTerm)
	}

	return nil
}

func (gcw *GarbageCollectedWAL) Append(payload []byte) error {
	if gcw.walReader == nil {
		return fmt.Errorf("WAL reader not initialized")
	}

	rt, err := gcw.walReader.RetentionTerm(payload)
	if err != nil {
		data := base64.StdEncoding.EncodeToString(payload)
		return fmt.Errorf("error reading retention term of entry %s: %v", data, err)
	}

	if len(gcw.WALs) == 0 {
		newWAL, err := gcw.CreateWAL()
		if err != nil {
			return fmt.Errorf("error creating new WAL: %v", err)
		}
		gcw.WALs = append(gcw.WALs, newWAL)
		gcw.retentionTerms = append(gcw.retentionTerms, rt)
	}

	last := len(gcw.retentionTerms) - 1
	if rt > gcw.retentionTerms[last] {
		gcw.retentionTerms[last] = rt
	}

	gcw.WALs[last].Append(payload)

	return nil
}

func (gcw *GarbageCollectedWAL) ReadAll() ([][]byte, error) {
	if gcw.walReader == nil {
		return nil, fmt.Errorf("WAL reader not initialized")
	}

	var allEntries [][]byte

	for i, wal := range gcw.WALs {
		entries, err := wal.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("error reading entries from WAL %d: %v", i, err)
		}

		allEntries = append(allEntries, entries...)
	}

	return allEntries, nil
}

func (gcw *GarbageCollectedWAL) Truncate(retentionTerm uint64) error {
	if gcw.walReader == nil {
		return fmt.Errorf("WAL reader not initialized")
	}

	// First, a sanity check - we ensure that len(gcw.WALs) == len(gcw.retentionTerms)
	if len(gcw.WALs) != len(gcw.retentionTerms) {
		return fmt.Errorf("internal error: number of WALs %d does not match number of retention terms %d", len(gcw.WALs), len(gcw.retentionTerms))
	}

	newWALs := make([]DeleteableWAL, 0, len(gcw.WALs))
	newRetentionTerms := make([]uint64, 0, len(gcw.retentionTerms))

	for i, wal := range gcw.WALs {
		if gcw.retentionTerms[i] < retentionTerm {
			// This WAL is below the retention term, we can delete it
			if err := wal.Delete(); err != nil {
				return fmt.Errorf("error deleting WAL %d: %v", i, err)
			}
		} else {
			// This WAL is still needed
			newWALs = append(newWALs, wal)
			newRetentionTerms = append(newRetentionTerms, gcw.retentionTerms[i])
		}
	}

	gcw.WALs = newWALs
	gcw.retentionTerms = newRetentionTerms

	return nil
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"encoding/base64"
	"fmt"

	"github.com/ava-labs/simplex"
)

// Creator creates a DeletableWAL. Returns an error upon failure.
type Creator func() (DeletableWAL, error)

// Reader reads the retention term of a WAL payload.
// Returns an error upon failure.
type Reader interface {
	RetentionTerm([]byte) (uint64, error)
}

// TruncateableWAL is a DeletableWAL that can be recentlyTruncated to a given retention term.
// It removes all entries below the given retention term.
type TruncateableWAL interface {
	DeletableWAL
	Truncate(retentionTerm uint64) error
}

// DeletableWAL is a WAL that can be deleted.
type DeletableWAL interface {
	simplex.WriteAheadLog
	// Delete deletes the WAL file and after it is called,
	// it should no longer be used.
	Delete() error
}

type garbageCollectedWAL struct {
	retentionTerm uint64
	size          int
	DeletableWAL
}

type GarbageCollectedWAL struct {
	wals       []garbageCollectedWAL
	createWAL  Creator
	maxWalSize int
	reader     Reader
}

// NewGarbageCollectedWAL creates a new GarbageCollectedWAL.
// It takes in a list of existing WALs to load, a Creator to create new WALs,
// a Reader to read the retention term of entries, and a maximum WAL size.
// A new WAL is created when the given maximum WAL size of a WAL is reached.
func NewGarbageCollectedWAL(WALs []DeletableWAL, creator Creator, reader Reader, maxWALSize int) (*GarbageCollectedWAL, error) {
	wals := make([]garbageCollectedWAL, 0, len(WALs))
	for _, wal := range WALs {
		wal, err := loadGarbageCollectedWAL(wal, reader)
		if err != nil {
			return nil, err
		}
		wals = append(wals, wal)
	}
	return &GarbageCollectedWAL{
		maxWalSize: maxWALSize,
		reader:     reader,
		createWAL:  creator,
		wals:       wals,
	}, nil
}

func loadGarbageCollectedWAL(wal DeletableWAL, reader Reader) (garbageCollectedWAL, error) {
	entries, err := wal.ReadAll()
	if err != nil {
		return garbageCollectedWAL{}, fmt.Errorf("error reading entries from WAL: %v", err)
	}

	var highestRetentionTerm uint64
	var size int

	for _, entry := range entries {
		size += len(entry)
		rt, err := reader.RetentionTerm(entry)
		if err != nil {
			data := base64.StdEncoding.EncodeToString(entry)
			return garbageCollectedWAL{}, fmt.Errorf("error reading retention term of entry %s: %v", data, err)
		}
		if rt > highestRetentionTerm {
			highestRetentionTerm = rt
		}
	}
	return garbageCollectedWAL{
		retentionTerm: highestRetentionTerm,
		DeletableWAL:  wal,
		size:          size,
	}, nil
}

func (gcw *GarbageCollectedWAL) Append(payload []byte) error {
	rt, err := gcw.reader.RetentionTerm(payload)
	if err != nil {
		data := base64.StdEncoding.EncodeToString(payload)
		return fmt.Errorf("error reading retention term of entry %s: %v", data, err)
	}

	if gcw.shouldWeCreateNewWAL(payload) {
		// Close the last WAL in use because we won't be needing it anymore
		if len(gcw.wals) > 0 {
			gcw.wals[len(gcw.wals)-1].Close()
		}
		newWAL, err := gcw.createWAL()
		if err != nil {
			return fmt.Errorf("error creating new WAL: %v", err)
		}
		gcw.wals = append(gcw.wals, garbageCollectedWAL{
			DeletableWAL:  newWAL,
			retentionTerm: rt,
			size:          len(payload),
		})
	}

	last := len(gcw.wals) - 1
	if rt > gcw.wals[last].retentionTerm {
		gcw.wals[last].retentionTerm = rt
	}

	gcw.wals[last].Append(payload)

	return nil
}

func (gcw *GarbageCollectedWAL) shouldWeCreateNewWAL(payload []byte) bool {
	var shouldCreateNewWAL bool

	if len(gcw.wals) == 0 {
		shouldCreateNewWAL = true
	} else {
		lastWALSize := gcw.wals[len(gcw.wals)-1].size
		shouldCreateNewWAL = lastWALSize+len(payload) > gcw.maxWalSize
	}
	return shouldCreateNewWAL
}

func (gcw *GarbageCollectedWAL) ReadAll() ([][]byte, error) {
	var allEntries [][]byte

	for i, wal := range gcw.wals {
		entries, err := wal.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("error reading entries from WAL %d: %v", i, err)
		}
		if err := wal.Close(); err != nil {
			return nil, fmt.Errorf("error closing WAL %d: %v", i, err)
		}

		allEntries = append(allEntries, entries...)
	}

	return allEntries, nil
}

func (gcw *GarbageCollectedWAL) Truncate(retentionTerm uint64) error {
	newWALs := make([]garbageCollectedWAL, 0, len(gcw.wals))

	for i, wal := range gcw.wals {
		if wal.retentionTerm < retentionTerm {
			// This WAL is below the retention term, we can delete it
			if err := wal.Delete(); err != nil {
				return fmt.Errorf("error deleting WAL %d: %v", i, err)
			}
			continue
		}
		// Else, this WAL is still needed
		newWALs = append(newWALs, wal)
	}

	gcw.wals = newWALs

	return nil
}

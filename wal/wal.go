// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"fmt"
	"io"
	"os"
	"simplex"
	"sync"
)

const (
	WalExtension = ".wal"
	WalFilename = "temp"
	WalFlags       = os.O_APPEND | os.O_CREATE | os.O_RDWR 
	WalPermissions = 0666
)

var (
	_ simplex.WriteAheadLog = &WriteAheadLog{}
)

type WriteAheadLog struct {
	file *os.File

	// allow one writer multiple readers
	rwMutex sync.RWMutex
}

// Ensure to call Close() on the WriteAheadLog to ensure the file is closed
func New() (*WriteAheadLog, error) {
	filename := WalFilename + WalExtension
	file, err := os.OpenFile(filename, WalFlags, WalPermissions)
	if err != nil {
		return nil, err
	}

	return &WriteAheadLog{
		file:    file,
		rwMutex: sync.RWMutex{},
	}, nil
}

// Appends a record to the write ahead log
// Must flush the OS cache on every append to ensure consistency
func (w *WriteAheadLog) Append(r *simplex.Record) error {
	bytes := r.Bytes()

	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()

	// write will append
	_, err := w.file.Write(bytes)
	if err != nil {
		return err
	}

	// ensure file gets written to SSD
	return w.file.Sync()
}

func (w *WriteAheadLog) ReadAll() ([]simplex.Record, error) {
	err := w.seekToStart()
	if err != nil {
		return []simplex.Record{}, fmt.Errorf("error seeking to start %w", err)
	}

	w.rwMutex.RLock()
	defer w.rwMutex.RUnlock()

	records := []simplex.Record{}
	for {
		var record simplex.Record
		_, err := record.FromBytes(w.file)

		// finished reading
		if err == io.EOF {
			break
		} else if err != nil {
			return []simplex.Record{}, ErrReadingRecord
		}

		records = append(records, record)
	}

	return records, nil
}

// Truncate truncates the write ahead log
func (w *WriteAheadLog) Truncate() error {
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()

	err := w.file.Truncate(0)
	if err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WriteAheadLog) Close() error {
	return w.file.Close()
}

func (w *WriteAheadLog) seekToStart() error {
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()

	_, err := w.file.Seek(0, io.SeekStart)
	return err
}

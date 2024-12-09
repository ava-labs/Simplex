// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"fmt"
	"io"
	"os"
	"simplex"
)

const (
	WalExtension   = ".wal"
	WalFilename    = "temp"
	WalFlags       = os.O_APPEND | os.O_CREATE | os.O_RDWR
	WalPermissions = 0666
)

var (
	_ simplex.WriteAheadLog = &WriteAheadLog{}
)

type WriteAheadLog struct {
	file *os.File
}

// New opens a write ahead log file, creating one if necessary.
// Call Close() on the WriteAheadLog to ensure the file is closed after use.
func New() (*WriteAheadLog, error) {
	filename := WalFilename + WalExtension
	file, err := os.OpenFile(filename, WalFlags, WalPermissions)
	if err != nil {
		return nil, err
	}

	return &WriteAheadLog{
		file: file,
	}, nil
}

// Appends a record to the write ahead log
// Must flush the OS cache on every append to ensure consistency
func (w *WriteAheadLog) Append(r *simplex.Record) error {
	bytes := r.Bytes()

	// write will append
	_, err := w.file.Write(bytes)
	if err != nil {
		return err
	}

	// ensure file gets written to persistent storage
	return w.file.Sync()
}

func (w *WriteAheadLog) ReadAll() ([]simplex.Record, error) {
	_, err := w.file.Seek(0, io.SeekStart)
	if err != nil {
		return []simplex.Record{}, fmt.Errorf("error seeking to start %w", err)
	}

	records := []simplex.Record{}
	fileInfo, err := w.file.Stat()
	if err != nil {
		return []simplex.Record{}, fmt.Errorf("error getting file info %w", err)
	}
	bytesToRead := fileInfo.Size()

	for bytesToRead > 0 {
		var record simplex.Record
		bytesRead, err := record.FromBytes(w.file)
		if err != nil {
			return records, err
		}

		bytesToRead -= int64(bytesRead)
		records = append(records, record)
	}

	// should never happen
	if bytesToRead != 0 {
		return records, fmt.Errorf("read more bytes than expected")
	}

	return records, nil
}

// Truncate truncates the write ahead log
func (w *WriteAheadLog) Truncate() error {
	err := w.file.Truncate(0)
	if err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WriteAheadLog) Close() error {
	return w.file.Close()
}

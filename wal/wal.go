// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	// DefaultCompactionThreshold is how many bytes may be appended before Compact rewrites the log.
	DefaultCompactionThreshold = 100 * 1024 * 1024

	WalFlags = os.O_APPEND | os.O_CREATE | os.O_RDWR
	// The log holds signed consensus state that is replayed as is on startup,
	// so only its owner may read or write it.
	WalPermissions = 0600
)

type WriteAheadLog struct {
	file     *os.File
	fileName string
	// uncompactedBytes counts what was appended since the last compaction. It starts at the
	// file size on open, so a log left bloated by a crash is compacted at the next opportunity.
	uncompactedBytes int64
	compactAt        int64
}

// New opens a write ahead log file, creating one if necessary.
// Call Close() on the WriteAheadLog to ensure the file is closed after use.
func New(fileName string) *WriteAheadLog {
	return &WriteAheadLog{
		fileName:  fileName,
		compactAt: DefaultCompactionThreshold,
	}
}

func (w *WriteAheadLog) maybeOpenFile() error {
	if w.file != nil {
		return nil
	}
	file, err := os.OpenFile(w.fileName, WalFlags, WalPermissions)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}
	w.file = file
	w.uncompactedBytes = info.Size()
	return nil
}

// Appends a record to the write ahead log
// Must flush the OS cache on every append to ensure consistency
func (w *WriteAheadLog) Append(b []byte) error {
	if err := w.maybeOpenFile(); err != nil {
		return err
	}
	// writeRecord will append
	if err := writeRecord(w.file, b); err != nil {
		return err
	}
	w.uncompactedBytes += recordSizeLen + int64(len(b)) + recordChecksumLen

	// ensure file gets written to persistent storage
	return w.file.Sync()
}

func (w *WriteAheadLog) ReadAll() ([][]byte, error) {
	if err := w.maybeOpenFile(); err != nil {
		return nil, err
	}
	_, err := w.file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("error seeking to start %w", err)
	}

	fileInfo, err := w.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("error getting file info %w", err)
	}
	bytesToRead := fileInfo.Size()

	var payloads [][]byte
	for bytesToRead > 0 {
		payload, bytesRead, err := readRecord(w.file, bytesToRead)
		// record was corrupted in wal
		if err != nil {
			return payloads, w.truncateAt(fileInfo.Size() - bytesToRead)
		}

		bytesToRead -= bytesRead
		payloads = append(payloads, payload)
	}

	// should never happen
	if bytesToRead != 0 {
		return payloads, fmt.Errorf("read more bytes than expected")
	}

	return payloads, nil
}

// Compact rewrites the log with only the records keep accepts.
// The rewritten log replaces the old one by rename, so a crash leaves one of the two intact.
func (w *WriteAheadLog) Compact(keep func([]byte) bool) error {
	if err := w.maybeOpenFile(); err != nil {
		return err
	}
	if w.uncompactedBytes < w.compactAt {
		return nil
	}

	records, err := w.ReadAll()
	if err != nil {
		return err
	}

	tmpName := w.fileName + ".tmp"
	tmp, err := os.OpenFile(tmpName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, WalPermissions)
	if err != nil {
		return fmt.Errorf("error creating compacted WAL %s: %w", tmpName, err)
	}
	for _, record := range records {
		if !keep(record) {
			continue
		}
		if err := writeRecord(tmp, record); err != nil {
			tmp.Close()
			return fmt.Errorf("error writing compacted WAL %s: %w", tmpName, err)
		}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("error syncing compacted WAL %s: %w", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("error closing compacted WAL %s: %w", tmpName, err)
	}

	if err := w.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, w.fileName); err != nil {
		return fmt.Errorf("error replacing WAL %s: %w", w.fileName, err)
	}
	if err := syncDir(filepath.Dir(w.fileName)); err != nil {
		return err
	}

	// The next append or read reopens the compacted file and counts its bytes as compacted.
	if err := w.maybeOpenFile(); err != nil {
		return err
	}
	w.uncompactedBytes = 0
	return nil
}

// syncDir makes a rename in the directory durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("error opening WAL directory %s: %w", dir, err)
	}
	defer d.Close()
	return d.Sync()
}

func (w *WriteAheadLog) truncateAt(offset int64) error {
	// truncate call is atomic. Ref https://cgi.cse.unsw.edu.au/~cs3231/18s1/os161/man/syscall/ftruncate.html
	err := w.file.Truncate(offset)
	if err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WriteAheadLog) Close() error {
	if w.file == nil {
		return nil
	}
	defer func() {
		w.file = nil
	}()
	return w.file.Close()
}

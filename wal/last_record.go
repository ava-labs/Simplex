// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"encoding/binary"
	"fmt"
	"os"
)

type LastRecordStoringWAL struct {
	recordIndexFilePath string
	TruncateableWAL
	recordType uint16
	lastRecord []byte
}

func NewLastRecordStoringWAL(lastRecordFilePath string, innerWAL TruncateableWAL, recordType uint16) (*LastRecordStoringWAL, error) {
	// We first check if we can open our last record file path
	payload, err := validateLastRecordFile(lastRecordFilePath)
	if err != nil {
		// If we cannot open the last record file, we just remove it.
		if removeErr := os.Remove(lastRecordFilePath); removeErr != nil {
			return nil, fmt.Errorf("could not remove invalid last record file %s: %w", lastRecordFilePath, removeErr)
		}
	}

	var offset int64

	entries, err := innerWAL.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("could not read all wal entries: %w", err)
	}

	var lastRecord []byte

	for i, entry := range entries {
		if len(entry) < 2 {
			return nil, fmt.Errorf("record %d too short to determine type", i)
		}

		offset += recordSizeLen + recordChecksumLen + int64(len(entry))

		if binary.BigEndian.Uint16(entry[0:2]) == recordType {
			lastRecord = entry
		}
	}

	if len(payload) > 0 {
		lastRecord = payload
	}

	ri := &LastRecordStoringWAL{
		lastRecord:          lastRecord,
		recordIndexFilePath: lastRecordFilePath,
		recordType:          recordType,
		TruncateableWAL:     innerWAL,
	}

	return ri, nil
}

func validateLastRecordFile(lastRecordFilePath string) ([]byte, error) {
	lastRecordFile, err := os.OpenFile(lastRecordFilePath, os.O_CREATE|os.O_RDWR, WalPermissions)
	if err != nil {
		return nil, fmt.Errorf("could not open record index file %s: %w", lastRecordFilePath, err)
	}

	stat, err := lastRecordFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("could not stat record index file %s: %w", lastRecordFilePath, err)
	}

	if stat.Size() == 0 {
		// This is a new file we have just created, so nothing more to do.
		return nil, nil
	}

	payload, _, err := readRecord(lastRecordFile, uint32(stat.Size()))
	if err != nil && stat.Size() > 0 {
		return nil, fmt.Errorf("could not read record index file %s: %w", lastRecordFilePath, err)
	}

	if err := lastRecordFile.Close(); err != nil {
		return nil, fmt.Errorf("could not close record index file %s: %w", lastRecordFilePath, err)
	}

	return payload, nil
}

func (ri *LastRecordStoringWAL) LastRecord() []byte {
	return ri.lastRecord
}

func (ri *LastRecordStoringWAL) Truncate(retentionTerm uint64) error {
	file, err := os.OpenFile(ri.recordIndexFilePath, os.O_CREATE|os.O_RDWR, WalPermissions)
	if err != nil {
		return fmt.Errorf("could not open last record file %s: %w", ri.recordIndexFilePath, err)
	}

	if len(ri.lastRecord) > 0 {
		if err := writeRecord(file, ri.lastRecord); err != nil {
			return fmt.Errorf("could not write last record to last record file %s: %w", ri.recordIndexFilePath, err)
		}
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("could not sync last record file %s: %w", ri.recordIndexFilePath, err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("could not close last record file %s: %w", ri.recordIndexFilePath, err)
	}

	return ri.TruncateableWAL.Truncate(retentionTerm)
}

func (ri *LastRecordStoringWAL) Append(record []byte) error {
	if len(record) < 2 {
		return fmt.Errorf("record too short to determine type")
	}

	if binary.BigEndian.Uint16(record[0:2]) == ri.recordType {
		ri.lastRecord = record
	}

	return ri.TruncateableWAL.Append(record)
}

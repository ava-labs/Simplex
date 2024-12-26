// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
)

const (
	recordSizeLen     = 4
	recordChecksumLen = 8
)

var ErrInvalidCRC = errors.New("invalid CRC checksum")

// writeRecord writes a length-prefixed and check-summed record to the writer.
func writeRecord(w io.Writer, payload []byte) error {
	checksumIndex := recordSizeLen + len(payload)
	buff := make([]byte, checksumIndex+recordChecksumLen)
	crc := crc64.New(crc64.MakeTable(crc64.ECMA))

	binary.BigEndian.PutUint32(buff, uint32(len(payload)))
	copy(buff[recordSizeLen:], payload)
	if _, err := crc.Write(buff[:checksumIndex]); err != nil {
		return fmt.Errorf("CRC checksum failed: %w", err)
	}

	buff = crc.Sum(buff[:checksumIndex])
	_, err := w.Write(buff)
	return err
}

// readRecord reads a length-prefixed and check-summed record from the reader.
// If the record is read correctly, the number of bytes read is returned.
func readRecord(r io.Reader, maxSize uint32) ([]byte, uint32, error) {
	const tempSpace = max(recordSizeLen, recordChecksumLen)
	buff := make([]byte, tempSpace)
	crc := crc64.New(crc64.MakeTable(crc64.ECMA))

	sizeBuff := buff[:recordSizeLen]
	if _, err := io.ReadFull(r, sizeBuff); err != nil {
		return nil, 0, err
	}
	if _, err := crc.Write(sizeBuff); err != nil {
		return nil, 0, fmt.Errorf("CRC checksum failed: %w", err)
	}

	payloadLen := binary.BigEndian.Uint32(sizeBuff)
	if payloadLen > maxSize {
		return nil, 0, fmt.Errorf("record indicates payload is %d bytes long", payloadLen)
	}

	payloadAndChecksum := make([]byte, payloadLen+recordChecksumLen)
	if _, err := io.ReadFull(r, payloadAndChecksum); err != nil {
		return nil, 0, err
	}
	payload := payloadAndChecksum[:payloadLen]
	if _, err := crc.Write(payload); err != nil {
		return nil, 0, fmt.Errorf("CRC checksum failed: %w", err)
	}

	checksum := payloadAndChecksum[payloadLen:]
	expectedChecksum := crc.Sum(buff[:0])
	if !bytes.Equal(checksum, expectedChecksum) {
		return nil, 0, ErrInvalidCRC
	}
	return payload, recordSizeLen + payloadLen + recordChecksumLen, nil
}

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
	const tempSpace = max(recordSizeLen, recordChecksumLen)
	buff := make([]byte, tempSpace)
	crc := crc64.New(crc64.MakeTable(crc64.ECMA))

	sizeBuff := buff[:recordSizeLen]
	binary.BigEndian.PutUint32(sizeBuff, uint32(len(payload)))
	if _, err := w.Write(sizeBuff); err != nil {
		return err
	}
	if _, err := crc.Write(sizeBuff); err != nil {
		return fmt.Errorf("CRC checksum failed: %w", err)
	}

	if _, err := w.Write(payload); err != nil {
		return err
	}
	if _, err := crc.Write(payload); err != nil {
		return fmt.Errorf("CRC checksum failed: %w", err)
	}

	checksum := crc.Sum(buff[:0])
	_, err := w.Write(checksum)
	return err
}

// readRecord reads a length-prefixed and check-summed record from the reader.
// If the record is read correctly, the number of bytes read is returned.
func readRecord(r io.Reader, maxSize uint32) ([]byte, uint32, error) {
	const tempSpace = max(recordSizeLen, 2*recordChecksumLen)
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

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 0, err
	}
	if _, err := crc.Write(payload); err != nil {
		return nil, 0, fmt.Errorf("CRC checksum failed: %w", err)
	}

	checksum := buff[:recordChecksumLen]
	if _, err := io.ReadFull(r, checksum); err != nil {
		return nil, 0, err
	}

	expectedChecksum := crc.Sum(buff[recordChecksumLen:recordChecksumLen])
	if !bytes.Equal(checksum, expectedChecksum) {
		return nil, 0, ErrInvalidCRC
	}
	return payload, recordSizeLen + payloadLen + recordChecksumLen, nil
}

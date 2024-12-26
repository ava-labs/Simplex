// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package record

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
)

const (
	recordVersionLen  = 1
	recordTypeLen     = 2
	recordSizeLen     = 4
	recordChecksumLen = 8

	recordHeaderLen = recordVersionLen + recordTypeLen + recordSizeLen

	recordVersionIndex  = 0
	recordTypeOffset    = recordVersionIndex + recordVersionLen
	recordSizeOffset    = recordTypeOffset + recordTypeLen
	recordPayloadOffset = recordSizeOffset + recordSizeLen

	maxPayloadSize = 100_000_000 // ~ 100MB
)

var ErrInvalidCRC = errors.New("invalid CRC checksum")

type Record struct {
	Version uint8
	Type    uint16
	Payload []byte
}

func (r *Record) Bytes() []byte {
	payloadLen := len(r.Payload)
	buff := make([]byte, recordHeaderLen+payloadLen+recordChecksumLen)

	buff[recordVersionIndex] = r.Version
	binary.BigEndian.PutUint16(buff[recordTypeOffset:], r.Type)
	binary.BigEndian.PutUint32(buff[recordSizeOffset:], uint32(payloadLen))
	copy(buff[recordPayloadOffset:], r.Payload)

	crc := crc64.New(crc64.MakeTable(crc64.ECMA))
	checksumOffset := recordPayloadOffset + payloadLen
	if _, err := crc.Write(buff[:checksumOffset]); err != nil {
		panic(fmt.Sprintf("CRC checksum failed: %v", err))
	}
	return crc.Sum(buff[:checksumOffset])
}

func (r *Record) FromBytes(in io.Reader) (int, error) {
	versionTypeSizeBuff := make([]byte, recordHeaderLen)
	if _, err := io.ReadFull(in, versionTypeSizeBuff); err != nil {
		return 0, err
	}

	version := versionTypeSizeBuff[recordVersionIndex]
	recType := binary.BigEndian.Uint16(versionTypeSizeBuff[recordTypeOffset:])
	payloadLen := binary.BigEndian.Uint32(versionTypeSizeBuff[recordSizeOffset:])
	if payloadLen > maxPayloadSize {
		return 0, fmt.Errorf("record indicates payload is %d bytes long", payloadLen)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(in, payload); err != nil {
		return 0, err
	}

	checksum := make([]byte, recordChecksumLen)
	if _, err := io.ReadFull(in, checksum); err != nil {
		return 0, err
	}

	crc := crc64.New(crc64.MakeTable(crc64.ECMA))
	if _, err := crc.Write(versionTypeSizeBuff); err != nil {
		return 0, fmt.Errorf("CRC checksum failed: %w", err)
	}
	if _, err := crc.Write(payload); err != nil {
		return 0, fmt.Errorf("CRC checksum failed: %w", err)
	}

	expectedChecksum := make([]byte, 0, recordChecksumLen)
	expectedChecksum = crc.Sum(expectedChecksum)
	if !bytes.Equal(checksum, expectedChecksum) {
		return 0, ErrInvalidCRC
	}

	r.Version = version
	r.Type = recType
	r.Payload = payload

	totalSize := recordHeaderLen + len(payload) + recordChecksumLen
	return totalSize, nil
}

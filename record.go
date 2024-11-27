// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
)

const (
	recordChecksumLen = 8
	recordSizeLen     = 4
	recordVersionLen  = 1
	recordTypeLen     = 2

	errInvalidCRC = "invalid CRC checksum"

	maxBlockSize = 100_000_000 // ~ 100MB
)

type Record struct {
	Version uint8
	Type    uint16
	Size    uint32
	Payload []byte
}

func (r *Record) Bytes() []byte {

	payloadLen := len(r.Payload)

	buff := make([]byte, recordVersionLen+recordTypeLen+payloadLen+recordSizeLen+recordChecksumLen)

	var pos int

	buff[pos] = r.Version
	pos++

	binary.BigEndian.PutUint16(buff[pos:], r.Type)
	pos += 2

	binary.BigEndian.PutUint32(buff[pos:], r.Size)
	pos += 4

	copy(buff[pos:], r.Payload)
	pos += len(r.Payload)

	crc := crc64.New(crc64.MakeTable(crc64.ECMA))
	if _, err := crc.Write(buff[:pos]); err != nil {
		panic(fmt.Sprintf("CRC checksum failed: %v", err))
	}
	copy(buff[pos:pos+8], crc.Sum(nil))

	return buff
}

func (r *Record) FromBytes(in io.Reader) (int, error) {
	versionTypeSizeBuff := make([]byte, recordVersionLen+recordTypeLen+recordSizeLen)

	n, err := io.ReadFull(in, versionTypeSizeBuff)
	if err != nil {
		return n, err
	}

	var pos int

	version := versionTypeSizeBuff[0]
	pos++

	recType := binary.BigEndian.Uint16(versionTypeSizeBuff[pos : pos+2])
	pos += 2

	dataLen := binary.BigEndian.Uint32(versionTypeSizeBuff[pos : pos+4])
	pos += 4

	if dataLen > maxBlockSize {
		return 0, fmt.Errorf("record indicates payload is %d bytes long", dataLen)
	}

	if dataLen == 0 {
		return 0, fmt.Errorf("record indicates payload is 0 bytes")
	}

	payload := make([]byte, dataLen)
	n, err = io.ReadFull(in, payload)
	if err != nil {
		return n, err
	}

	checksum := make([]byte, recordChecksumLen)
	n, err = io.ReadFull(in, checksum)
	if err != nil {
		return n, err
	}

	crc := crc64.New(crc64.MakeTable(crc64.ECMA))
	if _, err := crc.Write(versionTypeSizeBuff); err != nil {
		panic(fmt.Sprintf("CRC checksum failed: %v", err))
	}
	if _, err := crc.Write(payload); err != nil {
		panic(fmt.Sprintf("CRC checksum failed: %v", err))
	}
	expectedChecksum := crc.Sum(nil)

	if !bytes.Equal(checksum, expectedChecksum) {
		return 0, fmt.Errorf(errInvalidCRC)
	}

	r.Version = version
	r.Type = recType
	r.Size = dataLen
	r.Payload = payload

	totalSize := len(versionTypeSizeBuff) + len(payload) + len(checksum)

	return totalSize, nil
}

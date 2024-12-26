// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteRecord(t *testing.T) {
	require := require.New(t)

	r := []byte{3, 4, 5}
	var buff bytes.Buffer
	require.NoError(writeRecord(&buff, r))
	require.Equal(
		[]byte{
			0, 0, 0, 3, // Length of the payload
			3, 4, 5, // Payload
			0x8f, 0x9, 0x7f, 0xad, 0x19, 0xf9, 0xac, 0x8f, // CRC
		},
		buff.Bytes(),
	)
}

func TestReadRecord(t *testing.T) {
	require := require.New(t)

	r := []byte{3, 4, 5}
	var buff bytes.Buffer
	require.NoError(writeRecord(&buff, r))

	validBuff := bytes.NewBuffer(buff.Bytes())
	readPayload, _, err := readRecord(validBuff, math.MaxUint32)
	require.NoError(err)
	require.Equal(r, readPayload)

	buffBytes := buff.Bytes()
	buffBytes[len(buffBytes)-1]++ // Corrupt the CRC of the buffer
	invalidBuff := bytes.NewBuffer(buffBytes)
	_, _, err = readRecord(invalidBuff, math.MaxUint32)
	require.ErrorIs(err, ErrInvalidCRC)
}

func FuzzRecord(f *testing.F) {
	f.Fuzz(func(t *testing.T, payload []byte, badCRCVal uint64) {
		require := require.New(t)

		var buff bytes.Buffer
		require.NoError(writeRecord(&buff, payload))

		validBuff := bytes.NewBuffer(buff.Bytes())
		readPayload, n, err := readRecord(validBuff, math.MaxUint32)
		require.NoError(err)
		require.Equal(payload, readPayload)
		require.Equal(recordSizeLen+len(payload)+recordChecksumLen, n)

		recordBytes := buff.Bytes()
		crc := recordBytes[len(recordBytes)-recordChecksumLen:]

		badCRC := make([]byte, recordChecksumLen)
		binary.BigEndian.PutUint64(badCRC, badCRCVal)
		if bytes.Equal(crc, badCRC) {
			return
		}

		// Corrupt the CRC of the buffer
		copy(crc, badCRC)

		invalidBuff := bytes.NewBuffer(buff.Bytes())
		_, _, err = readRecord(invalidBuff, math.MaxUint32)
		require.ErrorIs(err, ErrInvalidCRC)
	})
}

func BenchmarkWriteRecord(b *testing.B) {
	payload := []byte{3, 4, 5}
	for range b.N {
		_ = writeRecord(io.Discard, payload)
	}
}

func BenchmarkReadRecord(b *testing.B) {
	payload := []byte{3, 4, 5}
	var originalBuffer bytes.Buffer
	require.NoError(b, writeRecord(&originalBuffer, payload))

	var loopBuffer bytes.Buffer
	for range b.N {
		loopBuffer = originalBuffer // reset the buffer
		_, _, _ = readRecord(&loopBuffer, math.MaxUint32)
	}
}

// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package record

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecord(t *testing.T) {
	r := Record{
		Version: 1,
		Type:    2,
		Payload: []byte{3, 4, 5},
	}

	buff := r.Bytes()

	var r2 Record
	_, err := r2.FromBytes(bytes.NewBuffer(buff))
	require.NoError(t, err)
	require.Equal(t, r, r2)

	// Corrupt the CRC of the buffer
	copy(buff[len(buff)-recordChecksumLen:], []byte{0, 1, 2, 3, 4, 5, 6, 7})
	_, err = r2.FromBytes(bytes.NewBuffer(buff))
	require.ErrorIs(t, err, ErrInvalidCRC)
}

func FuzzRecord(f *testing.F) {
	f.Fuzz(func(t *testing.T, version uint8, recType uint16, payload []byte, badCRC uint64) {
		r := Record{
			Version: version,
			Type:    recType,
			Payload: payload,
		}

		buff := r.Bytes()

		var r2 Record
		_, err := r2.FromBytes(bytes.NewBuffer(buff))
		require.NoError(t, err)
		require.Equal(t, r, r2)

		crc := make([]byte, recordChecksumLen)
		binary.BigEndian.PutUint64(crc, badCRC)

		buffCRC := buff[len(buff)-recordChecksumLen:]
		if bytes.Equal(crc, buffCRC) {
			return
		}

		// Corrupt the CRC of the buffer
		copy(buffCRC, crc)

		_, err = r2.FromBytes(bytes.NewBuffer(buff))
		require.ErrorIs(t, err, ErrInvalidCRC)
	})
}

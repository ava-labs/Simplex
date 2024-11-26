// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"encoding/binary"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRecord(t *testing.T) {
	r := Record{
		Version: 1,
		Type:    2,
		Size:    3,
		Payload: []byte{3, 4, 5},
	}

	buff := r.Bytes()

	var r2 Record
	require.NoError(t, r2.FromBytes(buff))
	require.Equal(t, r, r2)

	// Corrupt the CRC of the buffer
	copy(buff[len(buff)-8:], []byte{0, 1, 2, 3, 4, 5, 6, 7})
	require.EqualError(t, r2.FromBytes(buff), errInvalidCRC)
}

func FuzzRecord(f *testing.F) {
	f.Fuzz(func(t *testing.T, version uint8, recType uint16, recSize uint32, payload []byte, badCRC uint64) {
		r := Record{
			Version: version,
			Type:    recType,
			Size:    recSize,
			Payload: payload,
		}

		buff := r.Bytes()

		if len(payload) == 0 {
			return
		}

		var r2 Record
		require.NoError(t, r2.FromBytes(buff))
		require.Equal(t, r, r2)

		crc := make([]byte, 8)
		binary.BigEndian.PutUint64(crc, badCRC)

		if bytes.Equal(crc, buff[len(buff)-8:]) {
			return
		}

		// Corrupt the CRC of the buffer
		copy(buff[len(buff)-8:], crc)
		require.EqualError(t, r2.FromBytes(buff), errInvalidCRC)
	})
}

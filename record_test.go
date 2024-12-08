// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

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
		Size:    3,
		Payload: []byte{3, 4, 5},
	}

	buff := r.Bytes()

	var r2 Record
	_, err := r2.FromBytes(bytes.NewBuffer(buff))
	require.NoError(t, err)
	require.Equal(t, r, r2)

	// Corrupt the CRC of the buffer
	copy(buff[len(buff)-8:], []byte{0, 1, 2, 3, 4, 5, 6, 7})
	_, err = r2.FromBytes(bytes.NewBuffer(buff))
	require.EqualError(t, err, errInvalidCRC)
}

func TestMultipleFromBytes(t *testing.T) {
	r1 := Record{
		Version: 1,
		Type:    2,
		Size:    3,
		Payload: []byte{3, 4, 5},
	}

	r2 := Record{
		Version: 3,
		Type:    3,
		Size:    3,
		Payload: []byte{1, 2, 3},
	}

	buff := append(r1.Bytes(), r2.Bytes()...)
	buffer := bytes.NewBuffer(buff)

	var r Record
	_, err := r.FromBytes(buffer)
	require.NoError(t, err)
	require.Equal(t, r1, r)

	_, err = r.FromBytes(buffer)
	require.NoError(t, err)
	require.Equal(t, r2, r)
}

func FuzzRecord(f *testing.F) {
	f.Fuzz(func(t *testing.T, version uint8, recType uint16, payload []byte, badCRC uint64) {
		recSize := len(payload)
		r := Record{
			Version: version,
			Type:    recType,
			Size:    uint32(recSize),
			Payload: payload,
		}

		buff := r.Bytes()

		if len(payload) == 0 {
			return
		}

		var r2 Record
		_, err := r2.FromBytes(bytes.NewBuffer(buff))
		require.NoError(t, err)
		require.Equal(t, r, r2)

		crc := make([]byte, 8)
		binary.BigEndian.PutUint64(crc, badCRC)

		if bytes.Equal(crc, buff[len(buff)-8:]) {
			return
		}

		// Corrupt the CRC of the buffer
		copy(buff[len(buff)-8:], crc)

		_, err = r2.FromBytes(bytes.NewBuffer(buff))
		require.EqualError(t, err, errInvalidCRC)
	})
}

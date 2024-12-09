// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
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

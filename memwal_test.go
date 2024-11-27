// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInMemWAL(t *testing.T) {
	r1 := Record{
		Version: 1,
		Type:    2,
		Size:    3,
		Payload: []byte{4, 5, 6},
	}

	r2 := Record{
		Version: 7,
		Type:    8,
		Size:    3,
		Payload: []byte{10, 11, 12},
	}

	var wal InMemWAL
	wal.Append(&r1)
	wal.Append(&r2)

	require.Equal(t, []Record{r1, r2}, wal.ReadAll())
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSafeAdd(t *testing.T) {
	for _, tc := range []struct {
		name string
		a, b uint64
		sum  uint64
		err  string
	}{
		{
			name: "zero plus zero",
			a:    0, b: 0,
			sum: 0,
		},
		{
			name: "normal addition",
			a:    10, b: 20,
			sum: 30,
		},
		{
			name: "max uint64 plus zero",
			a:    math.MaxUint64, b: 0,
			sum: math.MaxUint64,
		},
		{
			name: "zero plus max uint64",
			a:    0, b: math.MaxUint64,
			sum: math.MaxUint64,
		},
		{
			name: "overflow by one",
			a:    math.MaxUint64, b: 1,
			err: "overflow",
		},
		{
			name: "overflow both large",
			a:    math.MaxUint64 - 5, b: 10,
			err: "overflow",
		},
		{
			name: "max uint64 boundary no overflow",
			a:    math.MaxUint64 - 5, b: 5,
			sum: math.MaxUint64,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := safeAdd(tc.a, tc.b)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.sum, result)
			}
		})
	}
}

func TestBitmask(t *testing.T) {
	t.Run("empty bitmask", func(t *testing.T) {
		bm := bitmaskFromBytes(nil)
		require.Equal(t, 0, bm.Len())
		require.False(t, bm.Contains(0))
		require.False(t, bm.Contains(5))
	})

	t.Run("from bytes and Contains", func(t *testing.T) {
		// 0b00000111 = 7 → bits 0, 1, 2 are set
		bm := bitmaskFromBytes([]byte{7})
		require.True(t, bm.Contains(0))
		require.True(t, bm.Contains(1))
		require.True(t, bm.Contains(2))
		require.False(t, bm.Contains(3))
		require.Equal(t, 3, bm.Len())
	})

	t.Run("Add", func(t *testing.T) {
		bm := bitmaskFromBytes([]byte{1}) // bit 0
		require.True(t, bm.Contains(0))
		require.False(t, bm.Contains(3))

		bm.Add(3)
		require.True(t, bm.Contains(3))
		require.Equal(t, 2, bm.Len())
	})

	t.Run("Bytes round-trip", func(t *testing.T) {
		bm := bitmaskFromBytes([]byte{0xAB})
		bm2 := bitmaskFromBytes(bm.Bytes())
		require.Equal(t, bm.Len(), bm2.Len())
		for i := 0; i < 8; i++ {
			require.Equal(t, bm.Contains(i), bm2.Contains(i))
		}
	})

	t.Run("Difference", func(t *testing.T) {
		// bm1 = bits 0,1,2 (0b111 = 7)
		// bm2 = bits 0,1   (0b011 = 3)
		// bm1.Difference(bm2) should leave only bit 2
		bm1 := bitmaskFromBytes([]byte{7})
		bm2 := bitmaskFromBytes([]byte{3})
		bm1.Difference(&bm2)
		require.False(t, bm1.Contains(0))
		require.False(t, bm1.Contains(1))
		require.True(t, bm1.Contains(2))
		require.Equal(t, 1, bm1.Len())
	})

	t.Run("Len with multiple bytes", func(t *testing.T) {
		// 0xFF = 8 bits set, 0x01 = 1 bit set → 9 total
		bm := bitmaskFromBytes([]byte{0x01, 0xFF})
		require.Equal(t, 9, bm.Len())
	})
}

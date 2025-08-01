// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"crypto/rand"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlacklist(t *testing.T) {
	bl := Blacklist{
		NodeCount: 7,
	}

	for i := uint16(0); i < bl.NodeCount; i++ {
		require.False(t, bl.IsBlacklisted(i), "Node %d should not be blacklisted initially", i)
	}

	require.NoError(t, bl.BlacklistNode(2))
	require.True(t, bl.IsBlacklisted(2))

	require.NoError(t, bl.BlacklistNode(5))
	require.True(t, bl.IsBlacklisted(5))

	require.False(t, bl.IsBlacklisted(0))
	require.False(t, bl.IsBlacklisted(1))
	require.False(t, bl.IsBlacklisted(3))
	require.False(t, bl.IsBlacklisted(4))
	require.False(t, bl.IsBlacklisted(6))

	bl.RedeemNode(2, 1)
	require.True(t, bl.IsBlacklisted(2))
	require.Len(t, bl.Bytes(), 8)

	bl.RedeemNode(2, 3)
	require.True(t, bl.IsBlacklisted(2))
	require.Len(t, bl.Bytes(), 8)

	bl.RedeemNode(3, 1)
	require.Len(t, bl.Bytes(), 8)

	bl.RedeemNode(2, 4)
	require.False(t, bl.IsBlacklisted(2))
	require.Len(t, bl.Bytes(), 4)
}

func TestBlacklistRedeemRandomNode(t *testing.T) {
	bl := Blacklist{
		NodeCount: 7,
	}

	bl.BlacklistNode(1)
	require.Equal(t, bl.blacklistSize(), 1)
	bl.BlacklistNode(3)
	require.Equal(t, bl.blacklistSize(), 2)
	bl.BlacklistNode(5)
	require.Equal(t, bl.blacklistSize(), 2)
	require.True(t, bl.IsBlacklisted(5))
	require.True(t, bl.IsBlacklisted(1) || bl.IsBlacklisted(3))
	require.False(t, bl.IsBlacklisted(1) && bl.IsBlacklisted(3))
}

func TestBitVec(t *testing.T) {
	var bv bitVec
	bv.Set(0)
	require.Equal(t, bitVec(1), bv)

	bv.Set(1)
	require.Equal(t, bitVec(3), bv)

	bv.Set(15)
	require.Equal(t, bitVec(1+2+32768), bv)

	require.Panics(t, func() {
		bv.Set(16)
	})

	bv.Clear(1)
	require.Equal(t, bitVec(1+32768), bv)

	require.True(t, bv.IsSet(15))

	bv.Clear(15)
	require.Equal(t, bitVec(1), bv)
	require.False(t, bv.IsSet(15))

	require.Panics(t, func() {
		bv.Clear(16)
	})
}

func TestBlacklistString(t *testing.T) {
	var reporters bitVec = 1 // 0001 in binary
	bl := Blacklist{
		NodeCount:        5,
		BlacklistedNodes: 9, // 1001 in binary
		RedeemedNodes: []struct {
			redeemedNode uint16
			reporters    *bitVec
		}{
			{redeemedNode: 4, reporters: &reporters}, // 0001 in binary
		},
	}

	expectedString := "NodeCount: 5, BlacklistedNodes: 9, RedeemedNodes: [{4 1}]"

	require.Equal(t, expectedString, bl.String())
}

func TestBlacklistBytes(t *testing.T) {
	randInts := newRandUint16(t, 6)
	bl := Blacklist{
		NodeCount:        uint16(randInts[0]),
		BlacklistedNodes: randInts[1],
		RedeemedNodes: []struct {
			redeemedNode uint16
			reporters    *bitVec
		}{
			{redeemedNode: uint16(randInts[2]), reporters: &randInts[3]},
			{redeemedNode: uint16(randInts[4]), reporters: &randInts[5]},
		},
	}

	bytes := bl.Bytes()

	expectedBytes := make([]byte, 12)
	binary.LittleEndian.PutUint16(expectedBytes[:2], bl.NodeCount)
	binary.LittleEndian.PutUint16(expectedBytes[2:4], uint16(bl.BlacklistedNodes))
	binary.LittleEndian.PutUint16(expectedBytes[4:6], bl.RedeemedNodes[0].redeemedNode)
	binary.LittleEndian.PutUint16(expectedBytes[6:8], uint16(*bl.RedeemedNodes[0].reporters))
	binary.LittleEndian.PutUint16(expectedBytes[8:10], bl.RedeemedNodes[1].redeemedNode)
	binary.LittleEndian.PutUint16(expectedBytes[10:12], uint16(*bl.RedeemedNodes[1].reporters))

	require.Equal(t, expectedBytes, bytes)

	var newBl Blacklist
	require.NoError(t, newBl.FromBytes(bytes))
	require.Equal(t, bl.NodeCount, newBl.NodeCount)
	require.Equal(t, bl.BlacklistedNodes, newBl.BlacklistedNodes)
	require.Equal(t, bl.RedeemedNodes, newBl.RedeemedNodes)
}

func newRandUint16(t *testing.T, n int) []bitVec {
	buff := make([]byte, 2*n)
	_, err := rand.Read(buff)
	require.NoError(t, err)

	nums := make([]bitVec, n)
	for i := 0; i < n; i++ {
		nums[i] = bitVec(binary.LittleEndian.Uint16(buff[i*2 : i*2+2]))
	}
	return nums
}

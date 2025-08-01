// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/big"
	"math/bits"
)

type Blacklist struct {
	NodeCount        uint16
	BlacklistedNodes bitVec
	RedeemedNodes    []struct {
		redeemedNode uint16
		reporters    *bitVec
	}
}

func (bl *Blacklist) IsZero() bool {
	return bl.NodeCount == 0 && bl.BlacklistedNodes == 0 && len(bl.RedeemedNodes) == 0
}

func (bl *Blacklist) blacklistSize() int {
	return bits.OnesCount16(uint16(bl.BlacklistedNodes))
}

func (bl *Blacklist) IsBlacklisted(i uint16) bool {
	// If the blacklist is uninitialized, nothing is blacklisted.
	if bl.IsZero() {
		return false
	}

	if i < 0 || i >= bl.NodeCount {
		panic(fmt.Sprintf("index out of range [0, %d): %d", bl.NodeCount, i))
	}

	return bl.BlacklistedNodes.IsSet(i)
}

func (bl *Blacklist) BlacklistNode(i uint16) error {
	if i < 0 || i >= bl.NodeCount {
		return fmt.Errorf("index out of range [0, %d): %d", bl.NodeCount, i)
	}

	bl.BlacklistedNodes.Set(i)

	threshold := (bl.NodeCount - 1) / 3 // f

	// If we have too many blacklisted nodes, we should not allow more,
	// as it would lead to a situation where we cannot reach consensus.
	// So we pick a pseudo-random node to redeem.
	if bits.OnesCount16(uint16(bl.BlacklistedNodes)) > int(threshold) {
		bl.redeemRandomNode()
	}

	return nil
}

func (bl *Blacklist) redeemRandomNode() {
	digest := sha256.Sum256(bl.Bytes())
	for {
		n := big.NewInt(0).SetBytes(digest[:])
		n.Mod(n, big.NewInt(int64(bl.NodeCount)))
		if bl.BlacklistedNodes.IsSet(uint16(n.Int64())) {
			bl.BlacklistedNodes.Clear(uint16(n.Int64()))
			bl.compactRedeemedNodes()
			break
		}
		// Else, the node is not blacklisted, so we need to pick a different one.
		digest = sha256.Sum256(digest[:])
	}
}

func (bl *Blacklist) RedeemNode(redeemedNodeIndex uint16, reporterNodeIndex uint16) error {
	if (bl.NodeCount) == 0 {
		return fmt.Errorf("blacklist node count is zero")
	}

	threshold := (bl.NodeCount-1)/3 + 1 // f + 1

	if redeemedNodeIndex < 0 || redeemedNodeIndex >= bl.NodeCount {
		return fmt.Errorf("redeemed node index out of range [0, %d): %d", bl.NodeCount, redeemedNodeIndex)
	}

	if reporterNodeIndex < 0 || reporterNodeIndex >= bl.NodeCount {
		return fmt.Errorf("reporter node index out of range [0, %d): %d", bl.NodeCount, reporterNodeIndex)
	}

	if !bl.BlacklistedNodes.IsSet(redeemedNodeIndex) {
		return nil
	}

	bl.markNodeReportedRedeemed(redeemedNodeIndex, reporterNodeIndex)

	if bl.isNodeRedeemed(int(threshold), redeemedNodeIndex) {
		bl.BlacklistedNodes.Clear(redeemedNodeIndex)
		bl.compactRedeemedNodes()
	}

	return nil
}

func (bl *Blacklist) isNodeRedeemed(threshold int, redeemedNodeIndex uint16) bool {
	for _, rn := range bl.RedeemedNodes {
		if rn.redeemedNode == redeemedNodeIndex {
			if bits.OnesCount16(uint16(*rn.reporters)) >= threshold {
				return true
			}
		}
	}
	return false
}

func (bl *Blacklist) markNodeReportedRedeemed(redeemedNodeIndex uint16, reporterNodeIndex uint16) {
	for _, rn := range bl.RedeemedNodes {
		if rn.redeemedNode == redeemedNodeIndex {
			rn.reporters.Set(reporterNodeIndex)
			return
		}
	}

	var zeroVec bitVec
	bl.RedeemedNodes = append(bl.RedeemedNodes, struct {
		redeemedNode uint16
		reporters    *bitVec
	}{
		redeemedNode: redeemedNodeIndex,
		reporters:    &zeroVec,
	})

	bl.RedeemedNodes[len(bl.RedeemedNodes)-1].reporters.Set(reporterNodeIndex)
}

func (bl *Blacklist) compactRedeemedNodes() {
	newRedeemedNodes := make([]struct {
		redeemedNode uint16
		reporters    *bitVec
	}, 0, len(bl.RedeemedNodes))
	for _, rn := range bl.RedeemedNodes {
		if bl.BlacklistedNodes.IsSet(rn.redeemedNode) {
			newRedeemedNodes = append(newRedeemedNodes, rn)
		}
	}
	bl.RedeemedNodes = newRedeemedNodes
}

func (bl *Blacklist) String() string {
	redeemed := bytes.Buffer{}
	for _, rn := range bl.RedeemedNodes {
		redeemed.WriteString(fmt.Sprintf("{%d %d},", rn.redeemedNode, *rn.reporters))
	}
	redeemedStr := redeemed.String()
	return fmt.Sprintf("NodeCount: %d, BlacklistedNodes: %d, RedeemedNodes: [%s]",
		bl.NodeCount, bl.BlacklistedNodes, redeemedStr[0:len(redeemedStr)-1]) // remove trailing comma
}

func (bl *Blacklist) Bytes() []byte {
	bytes := make([]byte, 4+4*len(bl.RedeemedNodes))
	bytes[0] = byte(bl.NodeCount)
	bytes[1] = byte(bl.NodeCount >> 8)
	bytes[2] = byte(bl.BlacklistedNodes)
	bytes[3] = byte(bl.BlacklistedNodes >> 8)

	pos := 4

	for _, bv := range bl.RedeemedNodes {
		bytes[pos] = byte(bv.redeemedNode)
		bytes[pos+1] = byte(bv.redeemedNode >> 8)
		bytes[pos+2] = byte(*bv.reporters)
		bytes[pos+3] = byte(*bv.reporters >> 8)
		pos += 4
	}

	return bytes
}

func (bl *Blacklist) FromBytes(bytes []byte) error {
	if len(bytes) < 4 {
		return fmt.Errorf("blacklist bytes too short: %d < 4", len(bytes))
	}

	if len(bytes)%4 != 0 {
		return fmt.Errorf("blacklist bytes length must be a factor of 4, got %d", len(bytes))
	}

	bl.NodeCount = uint16(bytes[0]) | uint16(bytes[1])<<8
	bl.BlacklistedNodes = bitVec(uint16(bytes[2]) | uint16(bytes[3])<<8)

	bytes = bytes[4:]

	bl.RedeemedNodes = make([]struct {
		redeemedNode uint16
		reporters    *bitVec
	}, 0, len(bytes)/4)

	for len(bytes) > 0 {
		reporters := bitVec(uint16(bytes[2]) | uint16(bytes[3])<<8)
		bl.RedeemedNodes = append(bl.RedeemedNodes, struct {
			redeemedNode uint16
			reporters    *bitVec
		}{
			redeemedNode: uint16(bytes[0]) | uint16(bytes[1])<<8,
			reporters:    &reporters,
		})
		bytes = bytes[4:]
	}

	return nil
}

type bitVec uint16

// IsSet checks if bit i is set to 1
func (bv *bitVec) IsSet(i uint16) bool {
	if bv == nil {
		panic("bitVec is nil")
	}
	if i < 0 || i >= 16 {
		panic(fmt.Sprintf("index out of range (0-15): %d", i))
	}

	return (*bv & (1 << i)) != 0
}

// Set sets bit i to 1
func (bv *bitVec) Set(i uint16) {
	if bv == nil {
		panic("bitVec is nil")
	}
	if i < 0 || i >= 16 {
		panic(fmt.Sprintf("index out of range (0-15): %d", i))
	}

	*bv |= 1 << i
}

// Clear sets bit i to 0
func (bv *bitVec) Clear(i uint16) {
	if bv == nil {
		panic("bitVec is nil")
	}
	if i < 0 || i >= 16 {
		panic(fmt.Sprintf("index out of range (0-15): %d", i))
	}

	*bv &= ^(1 << i)
}

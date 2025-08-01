// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

type NodeID []byte

func (node NodeID) String() string {
	var nodePrint [8]byte
	copy(nodePrint[:], node)
	return hex.EncodeToString(nodePrint[:])
}

func (node NodeID) Equals(otherNode NodeID) bool {
	return bytes.Equal(node, otherNode)
}

type NodeIDs []NodeID

func (nodes NodeIDs) String() string {
	var nodeStrings []string
	for _, node := range nodes {
		nodeStrings = append(nodeStrings, node.String())
	}
	return fmt.Sprintf("%v", nodeStrings)
}

func (nodes NodeIDs) Remove(targetNode NodeID) []NodeID {
	for i, n := range nodes {
		if n.Equals(targetNode) {
			result := make([]NodeID, 0, len(nodes)-1)
			result = append(result, nodes[:i]...)
			result = append(result, nodes[i+1:]...)
			return result
		}
	}
	return nodes
}

func (nodes NodeIDs) IndexOf(node NodeID) int {
	for i, n := range nodes {
		if n.Equals(node) {
			return i
		}
	}
	return -1
}

func (nodes NodeIDs) RandomNode(round uint64) int {
	h := sha256.New()

	roundBuff := make([]byte, 8)
	binary.BigEndian.PutUint64(roundBuff, round)
	h.Write(roundBuff)

	for _, n := range nodes {
		h.Write(n)
	}
	sum := h.Sum(nil)

	return int(binary.BigEndian.Uint16(sum[:2]))
}
// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
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

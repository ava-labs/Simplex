// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"encoding/asn1"
	"encoding/binary"
	"fmt"
	"slices"
)

func Orbit(round uint64, nodeIndex uint16, nodeCount uint16) uint64 {

	inCycle := round % uint64(nodeCount)
	cycles := round / uint64(nodeCount)

	if inCycle > uint64(nodeIndex) {
		return cycles + 1
	}

	return cycles
}

type Blacklist struct {
	// NodeCount is the configuration of the blacklist.
	NodeCount uint16

	// suspectedNodes is the list of nodes that are currently suspected.
	// it's the inner state of the blacklist.
	suspectedNodes suspectedNodes

	// Updates is the list of modifications that a block builder is proposing
	// to perform to the blacklist. It's not part of the inner state.
	// It's used here just for convenience.
	Updates []BlacklistUpdate
}

func NewBlacklist(nodeCount uint16) Blacklist {
	return Blacklist{
		NodeCount:      nodeCount,
	}
}

func (bl *Blacklist) String() string {
	var buf []byte
	buf = append(buf, fmt.Sprintf("Blacklist(nodeCount=%d, suspectedNodes=[", bl.NodeCount)...)
	for i, sn := range bl.suspectedNodes {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, fmt.Sprintf("{nodeIndex=%d, suspecting=%d, redeeming=%d, orbitSus=%d, orbitRedeem=%d}", sn.nodeIndex, sn.suspectingCount, sn.redeemingCount, sn.orbitSuspected, sn.orbitToRedeem)...)
	}
	buf = append(buf, "], updates=["...)
	for i, u := range bl.Updates {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, fmt.Sprintf("{type=%d, nodeIndex=%d}", u.Type, u.NodeIndex)...)
	}
	buf = append(buf, "])"...)
	return string(buf)
}

func (bl *Blacklist) Clone() Blacklist {
/*	clone := Blacklist{
		NodeCount:      bl.NodeCount,
		suspectedNodes: make([]suspectedNode, len(bl.suspectedNodes)),
		Updates:        make([]BlacklistUpdate, len(bl.Updates)),
	}
	copy(clone.suspectedNodes, bl.suspectedNodes)
	copy(clone.Updates, bl.Updates)*/
	var clone Blacklist
	bytes, err := bl.Bytes()
	if err != nil {
		panic(fmt.Sprintf("failed to clone blacklist: %v", err))
	}
	err = clone.FromBytes(bytes)
	if err != nil {
		panic(fmt.Sprintf("failed to clone blacklist: %v", err))
	}
	return clone
}

func (bl *Blacklist) Equals(b2 *Blacklist) bool {
	if bl.NodeCount != b2.NodeCount {
		return false
	}
	if len(bl.suspectedNodes) != len(b2.suspectedNodes) {
		return false
	}
	for i := range bl.suspectedNodes {
		sn1 := &bl.suspectedNodes[i]
		sn2 := &b2.suspectedNodes[i]
		if sn1.nodeIndex != sn2.nodeIndex ||
			sn1.suspectingCount != sn2.suspectingCount ||
			sn1.redeemingCount != sn2.redeemingCount ||
			sn1.orbitSuspected != sn2.orbitSuspected ||
			sn1.orbitToRedeem != sn2.orbitToRedeem {
			return false
		}
	}
	if len(bl.Updates) != len(b2.Updates) {
		return false
	}
	for i := range bl.Updates {
		u1 := &bl.Updates[i]
		u2 := &b2.Updates[i]
		if u1.Type != u2.Type || u1.NodeIndex != u2.NodeIndex {
			return false
		}
	}
	return true
}

// SerializedBlacklist is the on-the-wire format of the blacklist.
type SerializedBlacklist struct {
	NewBlacklist []byte
	Updates      []byte
}

type BlacklistOpType uint8

const (
	BlacklistOpType_Undefined BlacklistOpType = iota
	BlacklistOpType_NodeSuspected
	BlacklistOpType_NodeRedeemed
)

type BlacklistUpdate struct {
	Type      BlacklistOpType
	NodeIndex uint16
}

func (bu BlacklistUpdate) String() string {
	switch bu.Type {
	case BlacklistOpType_NodeSuspected:
		return fmt.Sprintf("{type=NodeSuspected, nodeIndex=%d}", bu.NodeIndex)
	case BlacklistOpType_NodeRedeemed:
		return fmt.Sprintf("{type=NodeRedeemed, nodeIndex=%d}", bu.NodeIndex)
	default:
		return fmt.Sprintf("{type=Unknown(%d), nodeIndex=%d}", bu.Type, bu.NodeIndex)
	}
}

func (bl *Blacklist) IsEmpty() bool {
	return bl.NodeCount == 0
}

func (bl *Blacklist) Update(updates []BlacklistUpdate, round uint64) {
	for _, update := range updates {
		orbit := Orbit(round, update.NodeIndex, bl.NodeCount)
		switch update.Type {
		case BlacklistOpType_NodeSuspected:
			bl.nodeSuspected(orbit, update.NodeIndex)
		case BlacklistOpType_NodeRedeemed:
			bl.nodeRedeemed(orbit, update.NodeIndex)
		}
	}

	bl.advanceRound(round)
	bl.Updates = updates
}

func (bl *Blacklist) advanceRound(round uint64) {
	for _, sn := range bl.suspectedNodes {
		orbit := Orbit(round, sn.nodeIndex, bl.NodeCount)
		bl.advanceOrbit(orbit, sn.nodeIndex)
	}
}

func (bl *Blacklist) advanceOrbit(orbit uint64, nodeIndex uint16) {
	f := (bl.NodeCount - 1) / 3
	threshold := f + 1

	// Otherwise, search for the node in the suspected list and remove it.
	newSuspectedNodes := make([]suspectedNode, 0, len(bl.suspectedNodes)-1)
	for i := range bl.suspectedNodes {
		sn := &bl.suspectedNodes[i]

		if sn.nodeIndex != nodeIndex {
			// This isn't the node we're looking for.
			// Just copy it over to the new list.
			newSuspectedNodes = append(newSuspectedNodes, *sn)
			continue
		}

		if sn.orbitSuspected >= orbit {
			// Orbit hasn't advanced, so abort early.
			// This is a sanity check.
			// This shouldn't happen, as round numbers only move forward.
			return
		}

		// Else, the orbit has advanced (sn.orbitSuspected < orbit).
		if sn.suspectingCount < threshold {
			// The node is not blacklisted, so since we've advanced to a new orbit,
			// we can just drop it as not enough nodes suspected it.
			continue
		}

		// Else, the node is blacklisted. We need to see if it has been redeemed by enough nodes.
		if sn.redeemingCount >= threshold {
			// The node has been redeemed by enough nodes, so we can remove it from the blacklist.
			continue
		}

		// Else, the node is still blacklisted. Reset the redeem counter.
		sn.redeemingCount = 0
		newSuspectedNodes = append(newSuspectedNodes, *sn)
	}

	bl.suspectedNodes = newSuspectedNodes
}

func (bl *Blacklist) nodeSuspected(orbit uint64, nodeIndex uint16) {
	// An orbit of 0 is a no-op
	if orbit == 0 {
		return
	}

	// Sanity check in case we get a too high node index.
	// This shouldn't happen, but if it does, we should just ignore it.
	if nodeIndex >= bl.NodeCount {
		return
	}
	// First, look to see if the node is already suspected.
	// If it is, then we shouldn't further increment its count.
	if bl.IsNodeSuspected(nodeIndex) {
		return
	}

	f := (bl.NodeCount - 1) / 3
	threshold := bl.NodeCount - f

	// Count how many total nodes are suspected. If we have more than f nodes suspected,
	// then abort because we don't want the blacklist to be too big.

	for _, sn := range bl.suspectedNodes {
		if sn.suspectingCount >= threshold {
			return
		}
	}

	// Else, the node isn't suspected. It may be in the suspected list,
	// or it may be a new entry.
	for i := range bl.suspectedNodes {
		sn := &bl.suspectedNodes[i]
		// Only increment the count if the orbit is the same.
		if sn.nodeIndex == nodeIndex {
			if sn.orbitSuspected == orbit {
				// The node is already there, so increment its count.
				sn.suspectingCount++
				return
			}
			// Else, the orbit is different, so just return early.
			return
		}
	}

	// If we reached here, the node isn't in the suspected list, so add it instead.
	bl.suspectedNodes = append(bl.suspectedNodes, suspectedNode{
		nodeIndex:       nodeIndex,
		suspectingCount: 1,
		orbitSuspected:  orbit,
	})
}

func (bl *Blacklist) nodeRedeemed(orbit uint64, nodeIndex uint16) {
	// An orbit of 0 is a no-op
	if orbit == 0 {
		return
	}

	f := (bl.NodeCount - 1) / 3
	threshold := f + 1

	// Sanity check in case we get a too high node index.
	// This shouldn't happen, but if it does, we should just ignore it.
	if nodeIndex >= bl.NodeCount {
		return
	}
	// First, look to see if the node is not suspected.
	// If it is not, then we shouldn't try to redeem it.
	if !bl.IsNodeSuspected(nodeIndex) {
		return
	}

	// Else, the node is suspected. Proceed to redeem it.
	for i := range bl.suspectedNodes {
		sn := &bl.suspectedNodes[i]
		// Only increment the count if the orbit is the same.
		if sn.nodeIndex == nodeIndex {
			if sn.orbitToRedeem == 0 {
				// This is the first time the node is redeemed.
				sn.orbitToRedeem = orbit
			}
			if sn.orbitToRedeem == orbit {
				sn.redeemingCount++
			}
			// If we have found a threshold of redeeming nodes, then we need to remove the node
			// from the suspected list.
			if sn.redeemingCount >= threshold {
				bl.suspectedNodes = slices.Delete(bl.suspectedNodes, i, i+1)
				return
			}
			// Else, the orbit is different, so just return early.
			return
		}
	}
}

func (bl *Blacklist) IsNodeSuspected(nodeIndex uint16) bool {
	f := (bl.NodeCount - 1) / 3
	threshold := f + 1

	for _, sn := range bl.suspectedNodes {
		if sn.nodeIndex == nodeIndex && sn.suspectingCount >= threshold {
			return true
		}
	}

	return false
}

func (bl *Blacklist) Bytes() ([]byte, error) {
	newBlacklistBytes := make([]byte, 2+22*len(bl.suspectedNodes))
	binary.BigEndian.PutUint16(newBlacklistBytes[0:2], bl.NodeCount)
	for i, sn := range bl.suspectedNodes {
		pos := 2 + i*22
		binary.BigEndian.PutUint16(newBlacklistBytes[pos:pos+2], sn.nodeIndex)
		binary.BigEndian.PutUint16(newBlacklistBytes[pos+2:pos+4], sn.suspectingCount)
		binary.BigEndian.PutUint16(newBlacklistBytes[pos+4:pos+6], sn.redeemingCount)
		binary.BigEndian.PutUint64(newBlacklistBytes[pos+6:pos+14], sn.orbitSuspected)
		binary.BigEndian.PutUint64(newBlacklistBytes[pos+14:pos+22], sn.orbitToRedeem)
	}

	updatesBytes := make([]byte, len(bl.Updates)*3)
	for i, update := range bl.Updates {
		pos := i * 3
		updatesBytes[pos] = byte(update.Type)
		binary.BigEndian.PutUint16(updatesBytes[pos+1:pos+3], update.NodeIndex)
	}

	if bl.IsEmpty() {
		newBlacklistBytes = nil
		updatesBytes = nil
	}

	sb := SerializedBlacklist{
		NewBlacklist: newBlacklistBytes,
		Updates:      updatesBytes,
	}

	return asn1.Marshal(sb)
}

func (bl *Blacklist) FromBytes(buff []byte) error {
	var sb SerializedBlacklist
	_, err := asn1.Unmarshal(buff, &sb)
	if err != nil {
		return fmt.Errorf("failed to unmarshal SerializedBlacklist: %w", err)
	}

	if len(sb.NewBlacklist) == 0 {
		// Empty blacklist
		*bl = Blacklist{}
		return nil
	}

	buff = sb.NewBlacklist

	if len(buff) < 2 {
		return fmt.Errorf("give buffer too short (%d) to contain node suspectingCount", len(buff))
	}
	bl.NodeCount = binary.BigEndian.Uint16(buff[0:2])
	buff = buff[2:]

	if len(buff)%22 != 0 {
		return fmt.Errorf("remaining buffer is not a multiple of 4")
	}
	numSuspected := len(buff) / 22

	for i := 0; i < numSuspected; i++ {
		pos := i * 22
		bl.suspectedNodes = append(bl.suspectedNodes, suspectedNode{
			nodeIndex:       binary.BigEndian.Uint16(buff[pos : pos+2]),
			suspectingCount: binary.BigEndian.Uint16(buff[pos+2 : pos+4]),
			redeemingCount:  binary.BigEndian.Uint16(buff[pos+4 : pos+6]),
			orbitSuspected:  binary.BigEndian.Uint64(buff[pos+6 : pos+14]),
			orbitToRedeem:   binary.BigEndian.Uint64(buff[pos+14 : pos+22]),
		})
	}

	buff = sb.Updates
	if len(buff)%3 != 0 {
		return fmt.Errorf("remaining updates buffer is not a multiple of 3")
	}
	numUpdates := len(buff) / 3

	if numUpdates == 0 {
		bl.Updates = nil
		return nil
	}

	bl.Updates = make([]BlacklistUpdate, numUpdates)
	for i := 0; i < numUpdates; i++ {
		pos := i * 3
		bl.Updates[i] = BlacklistUpdate{
			Type:      BlacklistOpType(buff[pos]),
			NodeIndex: binary.BigEndian.Uint16(buff[pos+1 : pos+3]),
		}
	}

	return nil
}


func verifyProposedBlacklist(prevBlacklist Blacklist, candidateBlacklist Blacklist, nodeCount int, round uint64) error {
	if candidateBlacklist.NodeCount != uint16(nodeCount) {
		return fmt.Errorf("block contains an invalid blacklist node count, expected %d, got %d", nodeCount, candidateBlacklist.NodeCount)
	}
	// 1) First thing we check that the updates even make sense.
	if err := prevBlacklist.verifyBlacklistUpdates(candidateBlacklist.Updates, nodeCount); err != nil {
		return fmt.Errorf("block contains invalid blacklist updates: %w", err)
	}
	updates := candidateBlacklist.Updates
	// 2) Next, we reset the updates from the previous round.
	prevBlacklist.Updates = nil
	// 3) We then proceed by applying the updates to the blacklist of the previous round.
	expectedBlacklist := prevBlacklist.Clone()
	expectedBlacklist.Update(updates, round)

	if !candidateBlacklist.Equals(&expectedBlacklist) {
		return fmt.Errorf("block contains an invalid blacklist, expected %s, got %s", expectedBlacklist.String(), candidateBlacklist.String())
	}

	return nil
}

func (bl *Blacklist) verifyBlacklistUpdates(updates []BlacklistUpdate, nodeCount int) error {
	used := make(map[uint16]struct{})
	if len(updates) > nodeCount {
		return fmt.Errorf("too many blacklist updates: %d, only %d nodes exist", len(updates), nodeCount)
	}
	for _, update := range updates {
		if update.Type != BlacklistOpType_NodeSuspected && update.Type != BlacklistOpType_NodeRedeemed {
			return fmt.Errorf("invalid blacklist update type: %d", update.Type)
		}
		if int(update.NodeIndex) < 0 || int(update.NodeIndex) >= nodeCount {
			return fmt.Errorf("invalid node index in blacklist update: %d, needs to be in [%d, %d]",
				update.NodeIndex, 0, nodeCount-1)
		}
		if _, exists := used[update.NodeIndex]; exists {
			return fmt.Errorf("node index %d was already updated", update.NodeIndex)
		}
		used[update.NodeIndex] = struct{}{}
		if update.Type == BlacklistOpType_NodeSuspected {
			// A node cannot be blacklisted if it is already suspected.
			if bl.IsNodeSuspected(update.NodeIndex) {
				return fmt.Errorf("node index %d is already blacklisted", update.NodeIndex)
			}
		}
		if update.Type == BlacklistOpType_NodeRedeemed {
			// A node cannot be redeemed if it is not currently suspected.
			if !bl.IsNodeSuspected(update.NodeIndex) {
				return fmt.Errorf("node index %d is not blacklisted, cannot be redeemed", update.NodeIndex)
			}
		}
	}

	return nil
}

func (bl *Blacklist) ComputeBlacklistUpdates(nodeCount uint16, timedOut, redeemed map[uint16]uint64) []BlacklistUpdate {
	updates := make([]BlacklistUpdate, 0, nodeCount)

	for nodeIndex := uint16(0); nodeIndex < nodeCount; nodeIndex++ {
		timedOutInRound, isTimedOut := timedOut[nodeIndex]
		redeemedInRound, isRedeemed := redeemed[nodeIndex]

		if bl.IsNodeSuspected(nodeIndex) {
			// If the node wasn't redeemed, then we cannot redeem it.
			if !isRedeemed {
				continue
			}

			// If the round in which the node was lastly redeemed is not later than the round in which it timed out in,
			// then we cannot redeem the node.
			if redeemedInRound <= timedOutInRound {
				continue
			}

			updates = append(updates, BlacklistUpdate{
				NodeIndex: nodeIndex,
				Type:      BlacklistOpType_NodeRedeemed,
			})
		} else {
			if !isTimedOut {
				continue
			}

			if redeemedInRound > timedOutInRound {
				continue
			}

			updates = append(updates, BlacklistUpdate{
				NodeIndex: nodeIndex,
				Type:      BlacklistOpType_NodeSuspected,
			})
		}
	}
	return updates
}

type suspectedNodes []suspectedNode

type suspectedNode struct {
	nodeIndex       uint16
	suspectingCount uint16
	redeemingCount  uint16
	orbitToRedeem   uint64
	orbitSuspected  uint64
}

func (sn *suspectedNode) String() string {
	return fmt.Sprintf("{nodeIndex=%d, suspectingCount=%d, redeemingCount=%d, orbitSuspected=%d, orbitToRedeem=%d}",
		sn.nodeIndex, sn.suspectingCount, sn.redeemingCount, sn.orbitSuspected, sn.orbitToRedeem)
}
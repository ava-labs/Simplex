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

func (b *Blacklist) String() string {
	var buf []byte
	buf = append(buf, fmt.Sprintf("Blacklist(nodeCount=%d, suspectedNodes=[", b.NodeCount)...)
	for i, sn := range b.suspectedNodes {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, fmt.Sprintf("{nodeIndex=%d, suspectingCount=%d, redeemingCount=%d, orbitSuspected=%d, orbitToRedeem=%d}", sn.nodeIndex, sn.suspectingCount, sn.redeemingCount, sn.orbitSuspected, sn.orbitToRedeem)...)
	}
	buf = append(buf, "], updates=["...)
	for i, u := range b.Updates {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, fmt.Sprintf("{type=%d, nodeIndex=%d}", u.Type, u.NodeIndex)...)
	}
	buf = append(buf, "])"...)
	return string(buf)
}

func (b *Blacklist) Equals(b2 *Blacklist) bool {
	if b.NodeCount != b2.NodeCount {
		return false
	}
	if len(b.suspectedNodes) != len(b2.suspectedNodes) {
		return false
	}
	for i := range b.suspectedNodes {
		sn1 := &b.suspectedNodes[i]
		sn2 := &b2.suspectedNodes[i]
		if sn1.nodeIndex != sn2.nodeIndex ||
			sn1.suspectingCount != sn2.suspectingCount ||
			sn1.redeemingCount != sn2.redeemingCount ||
			sn1.orbitSuspected != sn2.orbitSuspected ||
			sn1.orbitToRedeem != sn2.orbitToRedeem {
			return false
		}
	}
	if len(b.Updates) != len(b2.Updates) {
		return false
	}
	for i := range b.Updates {
		u1 := &b.Updates[i]
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

func (bl *Blacklist) IsEmpty() bool {
	return bl.NodeCount == 0
}

func (bl *Blacklist) Update(updates []BlacklistUpdate, round uint64) {
	for _, update := range updates {
		orbit := Orbit(round, update.NodeIndex, bl.NodeCount)
		switch update.Type {
		case BlacklistOpType_NodeSuspected:
			bl.NodeSuspected(orbit, update.NodeIndex)
		case BlacklistOpType_NodeRedeemed:
			bl.NodeRedeemed(orbit, update.NodeIndex)
		}
	}

	bl.AdvanceRound(round)
	bl.Updates = updates
}

func (bl *Blacklist) AdvanceRound(round uint64) {
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

func (bl *Blacklist) NodeSuspected(orbit uint64, nodeIndex uint16) {
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

func (bl *Blacklist) NodeRedeemed(orbit uint64, nodeIndex uint16) {
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

type suspectedNodes []suspectedNode

type suspectedNode struct {
	nodeIndex       uint16
	suspectingCount uint16
	redeemingCount  uint16
	orbitToRedeem   uint64
	orbitSuspected  uint64
}

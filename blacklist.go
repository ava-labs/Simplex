// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"encoding/asn1"
	"encoding/binary"
	"fmt"
	"slices"
)

// Orbit computes the number of rounds since genesis until and including the given round a node was the leader.
func Orbit(round uint64, nodeIndex uint16, nodeCount uint16) uint64 {
	inCycle := round % uint64(nodeCount)
	cycles := round / uint64(nodeCount)

	if inCycle >= uint64(nodeIndex) {
		return cycles + 1
	}

	return cycles
}

// Blacklist stores the state of the blacklist, as well as the updates that by applying them
// on the parent block's blacklist we get this blacklist.

type Blacklist struct {
	// NodeCount is the configuration of the blacklist.
	NodeCount uint16

	// SuspectedNodes is the list of nodes that are currently suspected.
	// it's the inner state of the blacklist.
	SuspectedNodes SuspectedNodes

	// Updates is the list of modifications that a block builder is proposing
	// to perform to the blacklist.
	Updates []BlacklistUpdate
}

func NewBlacklist(nodeCount uint16) Blacklist {
	return Blacklist{
		NodeCount: nodeCount,
	}
}

func (bl *Blacklist) String() string {
	var buf []byte
	buf = append(buf, fmt.Sprintf("Blacklist(nodeCount=%d, SuspectedNodes=[", bl.NodeCount)...)
	for i, sn := range bl.SuspectedNodes {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, sn.String()...)
	}
	buf = append(buf, "], updates=["...)
	for i, u := range bl.Updates {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, fmt.Sprintf("{type=%d, NodeIndex=%d}", u.Type, u.NodeIndex)...)
	}
	buf = append(buf, "])"...)
	return string(buf)
}

func (bl *Blacklist) Equals(b2 *Blacklist) bool {
	if bl.NodeCount != b2.NodeCount {
		return false
	}
	if len(bl.SuspectedNodes) != len(b2.SuspectedNodes) {
		return false
	}
	for i := range bl.SuspectedNodes {
		sn1 := &bl.SuspectedNodes[i]
		sn2 := &b2.SuspectedNodes[i]
		if sn1.NodeIndex != sn2.NodeIndex ||
			sn1.SuspectingCount != sn2.SuspectingCount ||
			sn1.RedeemingCount != sn2.RedeemingCount ||
			sn1.OrbitSuspected != sn2.OrbitSuspected ||
			sn1.OrbitToRedeem != sn2.OrbitToRedeem {
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

// SerializedBlacklist is used to transmit a blacklist over the wire.
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
		return fmt.Sprintf("{type=NodeSuspected, NodeIndex=%d}", bu.NodeIndex)
	case BlacklistOpType_NodeRedeemed:
		return fmt.Sprintf("{type=NodeRedeemed, NodeIndex=%d}", bu.NodeIndex)
	default:
		return fmt.Sprintf("{type=Unknown(%d), NodeIndex=%d}", bu.Type, bu.NodeIndex)
	}
}

func (bl *Blacklist) IsEmpty() bool {
	return bl.NodeCount == 0
}

// ApplyUpdates applies the given updates in the given round to the current blacklist and returns a new blacklist.
// The current blacklist is not modified.
func (bl *Blacklist) ApplyUpdates(updates []BlacklistUpdate, round uint64) Blacklist {
	newBlacklist := Blacklist{
		NodeCount:      bl.NodeCount,
		SuspectedNodes: make(SuspectedNodes, len(bl.SuspectedNodes)),
		Updates:        make([]BlacklistUpdate, len(updates)),
	}

	copy(newBlacklist.SuspectedNodes, bl.SuspectedNodes)
	newBlacklist.advanceRound(round)

	for _, update := range updates {
		orbit := Orbit(round, update.NodeIndex, bl.NodeCount)
		switch update.Type {
		case BlacklistOpType_NodeSuspected:
			newBlacklist.nodeSuspected(orbit, update.NodeIndex)
		case BlacklistOpType_NodeRedeemed:
			newBlacklist.nodeRedeemed(orbit, update.NodeIndex)
		}
	}

	copy(newBlacklist.Updates, updates)

	return newBlacklist
}

// advanceRound advances the blacklist to the given round.
// It will remove nodes that are no longer suspected or have been redeemed from the blacklist.
// It will also garbage-collect any votes from past orbits to blacklist or redeem nodes, unless
// they have surpassed the threshold of f+1.
func (bl *Blacklist) advanceRound(round uint64) {
	for _, sn := range bl.SuspectedNodes {
		orbit := Orbit(round, sn.NodeIndex, bl.NodeCount)
		bl.advanceOrbit(orbit, sn.NodeIndex)
	}
}

// advanceOrbit advances the blacklist to the given orbit for the given node.
// It will remove the node from the blacklist if it is no longer suspected or has been redeemed.
func (bl *Blacklist) advanceOrbit(orbit uint64, nodeIndex uint16) {
	f := (bl.NodeCount - 1) / 3
	threshold := f + 1

	// Search for the node in the suspected list and remove it if it wasn't blacklisted in the previous orbit.
	newSuspectedNodes := make([]SuspectedNode, 0, len(bl.SuspectedNodes)-1)
	for i := range bl.SuspectedNodes {
		sn := &bl.SuspectedNodes[i]

		if sn.NodeIndex != nodeIndex {
			// This isn't the node we're looking for.
			// Just copy it over to the new list.
			newSuspectedNodes = append(newSuspectedNodes, *sn)
			continue
		}

		if sn.OrbitSuspected >= orbit {
			// Orbit hasn't advanced.
			// Just copy it over to the new list as nothing has changed.
			newSuspectedNodes = append(newSuspectedNodes, *sn)
			continue
		}

		// Else, the orbit has advanced (sn.OrbitSuspected < orbit).
		if sn.SuspectingCount < threshold {
			// The node is not blacklisted, so since we've advanced to a new orbit,
			// we can just drop it as not enough nodes suspected it.
			continue
		}

		// Else, the node is blacklisted. We need to see if it has been redeemed by enough nodes.
		if sn.RedeemingCount >= threshold {
			// The node has been redeemed by enough nodes, so we can remove it from the blacklist.
			continue
		}

		// Else, the node is still blacklisted. Reset the redeem counter and orbit to redeem if it was a past orbit.
		newSN := *sn
		if sn.OrbitToRedeem < orbit {
			newSN.RedeemingCount = 0
			newSN.OrbitToRedeem = 0
		}
		newSuspectedNodes = append(newSuspectedNodes, newSN)
	}

	bl.SuspectedNodes = newSuspectedNodes
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
	threshold := f + 1

	// Count how many total nodes are suspected. If we have f or more nodes suspected,
	// then abort because we don't want the blacklist to be too big.

	var suspectedCount int
	for _, sn := range bl.SuspectedNodes {
		// Node is already suspected.
		if sn.SuspectingCount >= threshold {
			suspectedCount++
		}
	}

	if suspectedCount >= int(f) {
		// We already have f or more nodes suspected, so we cannot suspect any more nodes.
		return
	}

	// Else, the node isn't suspected. It may be in the suspected list,
	// or it may be a new entry.
	for i := range bl.SuspectedNodes {
		sn := &bl.SuspectedNodes[i]
		// Only increment the count if the orbit is the same.
		if sn.NodeIndex == nodeIndex {
			if sn.OrbitSuspected == orbit {
				// The node is already there, so increment its count.
				sn.SuspectingCount++
				return
			}
			// Else, the orbit is different, so just return early.
			return
		}
	}

	// If we reached here, the node isn't in the suspected list, so add it instead.
	bl.SuspectedNodes = append(bl.SuspectedNodes, SuspectedNode{
		NodeIndex:       nodeIndex,
		SuspectingCount: 1,
		OrbitSuspected:  orbit,
	})
}

func (bl *Blacklist) nodeRedeemed(orbit uint64, nodeIndex uint16) {
	// An orbit of 0 is a no-op because a node cannot be suspected in orbit 0.
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
	for i := range bl.SuspectedNodes {
		sn := &bl.SuspectedNodes[i]
		// Only increment the count if the orbit is the same.
		if sn.NodeIndex == nodeIndex {
			if sn.OrbitToRedeem == 0 {
				// This is the first time the node is redeemed.
				sn.OrbitToRedeem = orbit
			}
			if sn.OrbitToRedeem == orbit {
				sn.RedeemingCount++
			}
			// If we have found a threshold of redeeming nodes, then we need to remove the node
			// from the suspected list.
			if sn.RedeemingCount >= threshold {
				bl.SuspectedNodes = slices.Delete(bl.SuspectedNodes, i, i+1)
				return
			}
			// Else, the orbit is different, so just return early.
			// Else, the orbit is different, so just return early.
			return
		}
	}
}

func (bl *Blacklist) IsNodeSuspected(nodeIndex uint16) bool {
	f := (bl.NodeCount - 1) / 3
	threshold := f + 1

	for _, sn := range bl.SuspectedNodes {
		if sn.NodeIndex == nodeIndex && sn.SuspectingCount >= threshold && sn.RedeemingCount < threshold {
			return true
		}
	}

	return false
}

func (bl *Blacklist) Bytes() ([]byte, error) {
	newBlacklistBytes := make([]byte, 2+22*len(bl.SuspectedNodes))
	binary.BigEndian.PutUint16(newBlacklistBytes[0:2], bl.NodeCount)
	for i, sn := range bl.SuspectedNodes {
		pos := 2 + i*22
		binary.BigEndian.PutUint16(newBlacklistBytes[pos:pos+2], sn.NodeIndex)
		binary.BigEndian.PutUint16(newBlacklistBytes[pos+2:pos+4], sn.SuspectingCount)
		binary.BigEndian.PutUint16(newBlacklistBytes[pos+4:pos+6], sn.RedeemingCount)
		binary.BigEndian.PutUint64(newBlacklistBytes[pos+6:pos+14], sn.OrbitSuspected)
		binary.BigEndian.PutUint64(newBlacklistBytes[pos+14:pos+22], sn.OrbitToRedeem)
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

	// Reset the blacklist before populating it.
	*bl = Blacklist{
		NodeCount:      0,
		Updates:        []BlacklistUpdate{},
		SuspectedNodes: SuspectedNodes{},
	}

	// If we have no data, just return early.
	if len(sb.NewBlacklist) == 0 {
		return nil
	}

	buff = sb.NewBlacklist

	if len(buff) < 2 {
		return fmt.Errorf("give buffer too short (%d) to contain node SuspectingCount", len(buff))
	}
	bl.NodeCount = binary.BigEndian.Uint16(buff[0:2])
	buff = buff[2:]

	if len(buff)%22 != 0 {
		return fmt.Errorf("remaining buffer is not a multiple of 4")
	}
	numSuspected := len(buff) / 22

	for i := 0; i < numSuspected; i++ {
		pos := i * 22
		bl.SuspectedNodes = append(bl.SuspectedNodes, SuspectedNode{
			NodeIndex:       binary.BigEndian.Uint16(buff[pos : pos+2]),
			SuspectingCount: binary.BigEndian.Uint16(buff[pos+2 : pos+4]),
			RedeemingCount:  binary.BigEndian.Uint16(buff[pos+4 : pos+6]),
			OrbitSuspected:  binary.BigEndian.Uint64(buff[pos+6 : pos+14]),
			OrbitToRedeem:   binary.BigEndian.Uint64(buff[pos+14 : pos+22]),
		})
	}

	buff = sb.Updates
	if len(buff)%3 != 0 {
		return fmt.Errorf("remaining updates buffer is not a multiple of 3")
	}
	numUpdates := len(buff) / 3

	for i := 0; i < numUpdates; i++ {
		pos := i * 3
		bl.Updates = append(bl.Updates, BlacklistUpdate{
			Type:      BlacklistOpType(buff[pos]),
			NodeIndex: binary.BigEndian.Uint16(buff[pos+1 : pos+3]),
		})
	}

	return nil
}

func (bl *Blacklist) VerifyProposedBlacklist(candidateBlacklist Blacklist, nodeCount int, round uint64) error {
	if candidateBlacklist.NodeCount != uint16(nodeCount) {
		return fmt.Errorf("block contains an invalid blacklist node count, expected %d, got %d", nodeCount, candidateBlacklist.NodeCount)
	}
	// 1) First thing we check that the updates even make sense.
	if err := bl.verifyBlacklistUpdates(candidateBlacklist.Updates, nodeCount); err != nil {
		return fmt.Errorf("block contains invalid blacklist updates: %w", err)
	}
	updates := candidateBlacklist.Updates
	// 2) We then proceed by applying the updates to the blacklist of the previous round.
	expectedBlacklist := bl.ApplyUpdates(updates, round)

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

func (bl *Blacklist) ComputeBlacklistUpdates(round uint64, nodeCount uint16, timedOut, redeemed map[uint16]uint64) []BlacklistUpdate {
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

			if round < redeemedInRound {
				// This is a sanity check. This shouldn't happen, as round numbers only move forward.
				continue
			}

			roundsSinceLastRedeem := round - redeemedInRound

			if roundsSinceLastRedeem > uint64(nodeCount) {
				// The last time the node has redeemed was in an older orbit,
				// so ignore it. We can only redeem the node if it has redeemed in the current orbit.
				continue
			}

			updates = append(updates, BlacklistUpdate{
				NodeIndex: nodeIndex,
				Type:      BlacklistOpType_NodeRedeemed,
			})
		} else {
			// The node has yet to time out, so nothing suspicious here.
			if !isTimedOut {
				continue
			}

			// The node has redeemed itself after timing out, do not suspect it.
			if redeemedInRound > timedOutInRound {
				continue
			}

			if round < timedOutInRound {
				// This is a sanity check. This shouldn't happen, as round numbers only move forward.
				continue
			}

			roundsSinceLastTimeout := round - timedOutInRound

			if roundsSinceLastTimeout > uint64(nodeCount) {
				// The node has not timed out for a full orbit yet, so do not suspect it,
				// because it has lastly timed out in a past orbit.
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

type SuspectedNodes []SuspectedNode

type SuspectedNode struct {
	NodeIndex       uint16
	SuspectingCount uint16
	RedeemingCount  uint16
	OrbitToRedeem   uint64
	OrbitSuspected  uint64
}

func (sn *SuspectedNode) String() string {
	return fmt.Sprintf("{NodeIndex=%d, SuspectingCount=%d, RedeemingCount=%d, OrbitSuspected=%d, OrbitToRedeem=%d}",
		sn.NodeIndex, sn.SuspectingCount, sn.RedeemingCount, sn.OrbitSuspected, sn.OrbitToRedeem)
}

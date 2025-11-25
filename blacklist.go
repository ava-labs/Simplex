// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
)

var (
	errBlacklistTooManyUpdates          = fmt.Errorf("too many blacklist updates")
	errBlacklistInvalidNodeIndex        = fmt.Errorf("invalid node index in blacklist update")
	errBlacklistNodeIndexAlreadyUpdated = fmt.Errorf("node index already updated")
	errBlacklistNodeAlreadyBlacklisted  = fmt.Errorf("node index already blacklisted")
	errBlacklistNodeNotBlacklisted      = fmt.Errorf("node index not blacklisted, cannot be redeemed")
	errBlacklistInvalidUpdateType       = fmt.Errorf("invalid blacklist update type")
	errBlacklistInvalidNodeCount        = fmt.Errorf("block contains an invalid blacklist node count")
	errBlacklistInvalidUpdates          = fmt.Errorf("block contains invalid blacklist updates")
	errBlacklistInvalidBlacklist        = fmt.Errorf("block contains an invalid blacklist")
)

// Orbit returns the total number of times the given node has been selected as leader
// from genesis up to and including the specified round.
func Orbit(round uint64, nodeIndex uint16, nodeCount uint16) uint64 {
	inCycle := round % uint64(nodeCount)
	cycles := round / uint64(nodeCount)

	if inCycle >= uint64(nodeIndex) {
		return cycles + 1
	}

	return cycles
}

// Blacklist stores the state of the blacklist.
// It can be derived by applying the recorded Updates to the parent block's blacklist.
type Blacklist struct {
	// NodeCount is the configuration of the blacklist.
	NodeCount uint16

	// SuspectedNodes is the list of nodes that are currently suspected.
	// it's the inner state of the blacklist.
	SuspectedNodes SuspectedNodes

	// Updates is the list of modifications that a block builder is proposing
	// to perform to the blacklist.
	Updates BlacklistUpdates
}

func NewBlacklist(nodeCount uint16) Blacklist {
	return Blacklist{
		NodeCount: nodeCount,
	}
}

func (bl *Blacklist) String() string {
	sn := make([]string, len(bl.SuspectedNodes))
	for i, n := range bl.SuspectedNodes {
		sn[i] = n.String()
	}

	return fmt.Sprintf(
		"Blacklist(nodeCount=%d, SuspectedNodes=[%s], updates=[%s])",
		bl.NodeCount,
		strings.Join(sn, ","),
		blacklistUpdatesAsString(bl.Updates), // helper receives array
	)
}

func blacklistUpdatesAsString(updates []BlacklistUpdate) string {
	out := make([]string, len(updates))
	for i, u := range updates {
		out[i] = u.String()
	}
	return strings.Join(out, ",")
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
		if !sn1.Equals(sn2) {
			return false
		}
	}
	if len(bl.Updates) != len(b2.Updates) {
		return false
	}
	for i := range bl.Updates {
		u1 := &bl.Updates[i]
		u2 := &b2.Updates[i]
		if !u1.Equals(u2) {
			return false
		}
	}
	return true
}

type BlacklistOpType uint8

const (
	BlacklistOpType_Undefined BlacklistOpType = iota
	BlacklistOpType_NodeSuspected
	BlacklistOpType_NodeRedeemed
)

type BlacklistUpdates []BlacklistUpdate

func (updates BlacklistUpdates) Len() int {
	return len(updates) * 3
}

func (updates BlacklistUpdates) Bytes() []byte {
	// Each update is encoded as:
	// 1 byte for the type
	// 2 bytes for the node index
	if len(updates) == 0 {
		return []byte{}
	}

	buff := make([]byte, len(updates)*3)
	for i, blu := range updates {
		pos := i * 3
		buff[pos] = byte(blu.Type)
		binary.BigEndian.PutUint16(buff[pos+1:pos+3], blu.NodeIndex)
	}
	return buff
}

func (updates *BlacklistUpdates) FromBytes(buff []byte) error {
	if len(buff) == 0 {
		return nil
	}
	if len(buff)%3 != 0 {
		return fmt.Errorf("buffer length is not a multiple of 3")
	}
	numUpdates := len(buff) / 3
	*updates = make([]BlacklistUpdate, numUpdates)
	for i := 0; i < numUpdates; i++ {
		pos := i * 3
		(*updates)[i] = BlacklistUpdate{
			Type:      BlacklistOpType(buff[pos]),
			NodeIndex: binary.BigEndian.Uint16(buff[pos+1 : pos+3]),
		}
	}
	return nil
}

type BlacklistUpdate struct {
	Type      BlacklistOpType
	NodeIndex uint16
}

func (bu *BlacklistUpdate) Equals(bu2 *BlacklistUpdate) bool {
	return bu.Type == bu2.Type && bu.NodeIndex == bu2.NodeIndex
}

func (bu *BlacklistUpdate) String() string {
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

	newBlacklist.SuspectedNodes = bl.garbageCollectSuspectedNodes(round)

	for _, update := range updates {
		orbit := Orbit(round, update.NodeIndex, bl.NodeCount)
		switch update.Type {
		case BlacklistOpType_NodeSuspected:
			newBlacklist.setNodeSuspected(orbit, update.NodeIndex)
		case BlacklistOpType_NodeRedeemed:
			newBlacklist.setNodeRedeemed(orbit, update.NodeIndex)
		}
	}

	copy(newBlacklist.Updates, updates)

	return newBlacklist
}

// garbageCollectSuspectedNodes returns a new list of suspected nodes for the given round.
// Nodes that are no longer suspected or have been redeemed, will not be included in the returned suspected nodes.
// It will also garbage-collect any redeem votes from past orbits, unless hey have surpassed the threshold of f+1.
// It does not modify the current blacklist.
func (bl *Blacklist) garbageCollectSuspectedNodes(round uint64) SuspectedNodes {
	newSuspectedNodes := make([]SuspectedNode, 0, len(bl.SuspectedNodes))
	threshold := bl.computeThreshold()

	for _, sn := range bl.SuspectedNodes {
		orbit := Orbit(round, sn.NodeIndex, bl.NodeCount)
		// Node is no longer suspected, just skip it.
		if !sn.isStillSuspected(threshold, orbit) {
			continue
		}

		newSuspectedNode := sn

		//  Reset the redeemingCount and orbitToRedeem if it was a past orbit.
		if sn.OrbitToRedeem < orbit {
			newSuspectedNode.RedeemingCount = 0
			newSuspectedNode.OrbitToRedeem = 0
		}
		newSuspectedNodes = append(newSuspectedNodes, newSuspectedNode)
	}

	return newSuspectedNodes
}

func (bl *Blacklist) setNodeSuspected(orbit uint64, nodeIndex uint16) {
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

	threshold := bl.computeThreshold()

	var suspectedCount uint16

	// Iterate through the suspected nodes to see if the node is already there.
	// If it is, increment its count and return.
	// If the node isn't there, we count the total number of suspected nodes.
	// If the total number of suspected nodes is >= f, we cannot suspect any more nodes.
	// Otherwise, we add the node to the suspected list with a count of 1.

	for i := range bl.SuspectedNodes {
		sn := &bl.SuspectedNodes[i]

		// Count how many total nodes are suspected.
		if sn.SuspectingCount >= threshold {
			suspectedCount++
		}
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

	f := (bl.NodeCount - 1) / 3

	if suspectedCount >= f {
		// We already have f or more nodes suspected, abort because we cannot suspect any more nodes.
		return
	}

	// If we reached here, the node isn't in the suspected list, and it is small enough, so add it instead.
	bl.SuspectedNodes = append(bl.SuspectedNodes, SuspectedNode{
		NodeIndex:       nodeIndex,
		SuspectingCount: 1,
		OrbitSuspected:  orbit,
	})
}

func (bl *Blacklist) setNodeRedeemed(orbit uint64, nodeIndex uint16) {
	// An orbit of 0 is a no-op because a node cannot be suspected in orbit 0.
	if orbit == 0 {
		return
	}

	threshold := bl.computeThreshold()

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
			if sn.OrbitSuspected == orbit {
				// It is too early to redeem a node that was suspected in the current orbit.
				return
			}
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
			return
		}
	}
}

func (bl *Blacklist) computeThreshold() uint16 {
	f := (bl.NodeCount - 1) / 3
	threshold := f + 1
	return threshold
}

func (bl *Blacklist) IsNodeSuspected(nodeIndex uint16) bool {
	threshold := bl.computeThreshold()

	for _, sn := range bl.SuspectedNodes {

		if sn.NodeIndex == nodeIndex {
			return sn.SuspectingCount >= threshold && sn.RedeemingCount < threshold
		}
	}

	return false
}

// Bytes returns the byte representation of the blacklist.
// The bytes of a blacklist are encoded as follows:
// 2 bytes for the node count
// 2 bytes for the number of updates
// N bytes for the suspected nodes section
// The rest of the bytes encode the updates section.
func (bl *Blacklist) Bytes() []byte {
	buff := make([]byte, 2+2+bl.Updates.Len()+bl.SuspectedNodes.Len())
	binary.BigEndian.PutUint16(buff[0:2], bl.NodeCount)
	if len(bl.SuspectedNodes) == 0 && len(bl.Updates) == 0 {
		buff = buff[:2]
		return buff
	}
	binary.BigEndian.PutUint16(buff[2:4], uint16(len(bl.Updates)))
	if bl.SuspectedNodes.Len() > 0 {
		copy(buff[4:4+bl.SuspectedNodes.Len()], bl.SuspectedNodes.Bytes())
	}
	if bl.Updates.Len() > 0 {
		copy(buff[4+bl.SuspectedNodes.Len():], bl.Updates.Bytes())
	}
	return buff
}

// FromBytes populates the blacklist from the given bytes.
func (bl *Blacklist) FromBytes(buff []byte) error {
	if len(buff) == 2 {
		bl.NodeCount = binary.BigEndian.Uint16(buff[0:2])
		bl.SuspectedNodes = SuspectedNodes{}
		bl.Updates = BlacklistUpdates{}
		return nil
	}

	if len(buff) < 4 {
		return fmt.Errorf("buffer too short (%d) to contain blacklist", len(buff))
	}

	bl.NodeCount = binary.BigEndian.Uint16(buff[0:2])
	numUpdates := binary.BigEndian.Uint16(buff[2:4])

	originalBuffSize := len(buff)
	buff = buff[4:]

	updateLen := int(numUpdates) * 3
	if len(buff) < updateLen {
		return fmt.Errorf("buffer too short (%d) to contain %d bytes for updates", len(buff), 4+3*numUpdates)
	}

	suspectedNodesLen := len(buff) - updateLen

	if numUpdates == 0 && suspectedNodesLen == 0 {
		return fmt.Errorf("buffer too large (%d) to contain no updates and no suspected nodes", originalBuffSize)
	}

	var suspectedNodes SuspectedNodes
	if err := suspectedNodes.FromBytes(buff[:suspectedNodesLen]); err != nil {
		return fmt.Errorf("failed to parse suspected nodes: %w", err)
	}

	bl.SuspectedNodes = suspectedNodes

	if err := bl.Updates.FromBytes(buff[suspectedNodesLen:]); err != nil {
		return fmt.Errorf("failed to parse blacklist updates: %w", err)
	}

	if updateLen != len(buff[suspectedNodesLen:]) {
		return fmt.Errorf("expected %d bytes for updates, but got %d", updateLen, len(buff[suspectedNodesLen:]))
	}

	return nil
}

func (bl *Blacklist) VerifyProposedBlacklist(candidateBlacklist Blacklist, round uint64) error {
	if candidateBlacklist.NodeCount != bl.NodeCount {
		return fmt.Errorf("%s, expected %d, got %d", errBlacklistInvalidNodeCount, bl.NodeCount, candidateBlacklist.NodeCount)
	}
	// 1) First thing we check that the updates even make sense.
	if err := bl.verifyBlacklistUpdates(candidateBlacklist.Updates); err != nil {
		return fmt.Errorf("%s: %w", errBlacklistInvalidUpdates, err)
	}
	updates := candidateBlacklist.Updates
	// 2) We then proceed by applying the updates to the blacklist of the previous round.
	expectedBlacklist := bl.ApplyUpdates(updates, round)

	if !candidateBlacklist.Equals(&expectedBlacklist) {
		return fmt.Errorf("%s, expected %s, got %s", errBlacklistInvalidBlacklist, expectedBlacklist.String(), candidateBlacklist.String())
	}

	return nil
}

func (bl *Blacklist) verifyBlacklistUpdates(updates []BlacklistUpdate) error {
	seen := make(map[uint16]struct{})
	if len(updates) > int(bl.NodeCount) {
		return fmt.Errorf("%w: %d, only %d nodes exist", errBlacklistTooManyUpdates, len(updates), bl.NodeCount)
	}
	for _, update := range updates {
		if update.NodeIndex >= bl.NodeCount {
			return fmt.Errorf("%w: %d, needs to be in [%d, %d]",
				errBlacklistInvalidNodeIndex, update.NodeIndex, 0, bl.NodeCount-1)
		}

		if _, exists := seen[update.NodeIndex]; exists {
			return fmt.Errorf("%w: (%d)", errBlacklistNodeIndexAlreadyUpdated, update.NodeIndex)
		}
		seen[update.NodeIndex] = struct{}{}

		switch update.Type {
		case BlacklistOpType_NodeSuspected:
			// A node cannot be blacklisted if it is already suspected.
			if bl.IsNodeSuspected(update.NodeIndex) {
				return fmt.Errorf("%w: (%d)", errBlacklistNodeAlreadyBlacklisted, update.NodeIndex)
			}
		case BlacklistOpType_NodeRedeemed:
			// A node cannot be redeemed if it is not currently suspected.
			if !bl.IsNodeSuspected(update.NodeIndex) {
				return fmt.Errorf("%w: (%d)", errBlacklistNodeNotBlacklisted, update.NodeIndex)
			}
		default:
			return fmt.Errorf("%w: (%d)", errBlacklistInvalidUpdateType, update.Type)
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

func (sns *SuspectedNodes) FromBytes(buff []byte) error {
	pos := 0
	for pos < len(buff) {
		var sn SuspectedNode
		bytesRead, err := sn.Read(buff[pos:])
		if err != nil {
			return fmt.Errorf("failed to read suspected node at position %d: %w", pos, err)
		}
		*sns = append(*sns, sn)
		pos += bytesRead
	}
	return nil
}

func (sns *SuspectedNodes) Len() int {
	var totalLen int
	for _, sn := range *sns {
		totalLen += sn.Len()
	}
	return totalLen
}

func (sns *SuspectedNodes) Bytes() []byte {
	// Suspected nodes are encoded by concatenating all suspected nodes one after the other.

	if len(*sns) == 0 {
		return []byte{}
	}

	var buffSize int
	for _, sn := range *sns {
		buffSize += sn.Len()
	}

	buff := make([]byte, buffSize)

	var pos int
	for _, sn := range *sns {
		bytesWritten := sn.write(buff[pos:])
		pos += bytesWritten
	}

	return buff
}

// SuspectedNode is the information we keep for each suspected node.
// A suspected node records the number of accusations and when it was accused in.
// A suspected node that has above f+1 accusations is considered as blacklisted.
// A suspected node that is blacklisted can be redeemed by gathering f+1 redeeming votes.
// A suspected node that has been redeemed by f+1 votes or more is removed from the blacklist.
// Each suspected node is encoded in the following manner:
// A bitmask byte for the fields except the node index that are non-zero.
// 4 top bits of the bitmask are the bitmask for { suspectingCount, redeemingCount, orbitSuspected, orbitToRedeem }.
// A bit is set to 1 if the corresponding field is non-zero.
// The next 2 bytes are the node index.
// The next bytes correspond to the non-zero fields in the order of the bitmask.
// [bitmask byte]
// [node index (2 bytes)]
// [suspectingCount (2 bytes, if non-zero)]
// [redeemingCount (2 bytes, if non-zero)]
// [orbitSuspected (8 bytes, if non-zero)]
// [orbitToRedeem (8 bytes, if non-zero)]
type SuspectedNode struct {
	// NodeIndex is the index of the suspected node among the nodes of the validator set.
	NodeIndex uint16
	// SuspectingCount is the number of nodes that have suspected this node in the current orbit denoted by OrbitSuspected.
	// If this count is >= f+1, then the node is considered blacklisted.
	SuspectingCount uint16
	// RedeemingCount is the number of nodes that have redeemed this node in the current orbit denoted by OrbitToRedeem.
	// If this count reaches >= f+1, the node is removed from the blacklist.
	RedeemingCount uint16
	// OrbitSuspected is the orbit in which the node was last suspected.
	OrbitSuspected uint64
	// OrbitToRedeem is the orbit in which the node was last redeemed.
	OrbitToRedeem uint64
}

func (sn *SuspectedNode) Equals(sn2 *SuspectedNode) bool {
	return sn.NodeIndex == sn2.NodeIndex &&
		sn.SuspectingCount == sn2.SuspectingCount &&
		sn.RedeemingCount == sn2.RedeemingCount &&
		sn.OrbitSuspected == sn2.OrbitSuspected &&
		sn.OrbitToRedeem == sn2.OrbitToRedeem
}

func (sn *SuspectedNode) String() string {
	return fmt.Sprintf("{NodeIndex=%d, SuspectingCount=%d, RedeemingCount=%d, OrbitSuspected=%d, OrbitToRedeem=%d}",
		sn.NodeIndex, sn.SuspectingCount, sn.RedeemingCount, sn.OrbitSuspected, sn.OrbitToRedeem)
}

// Write writes the suspected node to the given buffer.
// It returns the number of bytes written.
// Assumes the buffer is large enough to hold the suspected node.
func (sn *SuspectedNode) write(buff []byte) int {
	var pos int

	var bitmask byte

	pos++ // Reserve space for the bitmask.

	binary.BigEndian.PutUint16(buff[pos:pos+2], sn.NodeIndex)
	pos += 2

	if sn.SuspectingCount > 0 {
		bitmask |= 128
		binary.BigEndian.PutUint16(buff[pos:pos+2], sn.SuspectingCount)
		pos += 2
	}
	if sn.RedeemingCount > 0 {
		bitmask |= 64
		binary.BigEndian.PutUint16(buff[pos:pos+2], sn.RedeemingCount)
		pos += 2
	}
	if sn.OrbitSuspected > 0 {
		bitmask |= 32
		binary.BigEndian.PutUint64(buff[pos:pos+8], sn.OrbitSuspected)
		pos += 8
	}
	if sn.OrbitToRedeem > 0 {
		bitmask |= 16
		binary.BigEndian.PutUint64(buff[pos:pos+8], sn.OrbitToRedeem)
		pos += 8
	}

	buff[0] = bitmask

	return pos
}

// Read reads the suspected node from the given buffer.
// It returns the number of bytes read and an error if occurs.
func (sn *SuspectedNode) Read(buff []byte) (int, error) {
	if len(buff) < 3 {
		return 0, fmt.Errorf("given buffer too short (%d) to contain bitmask and node index", len(buff))
	}

	var pos int

	bitmask := buff[pos]

	// Check bitmask validity.
	// Lower 4 bites must be zero.
	if bitmask&15 != 0 {
		return 0, fmt.Errorf("invalid bitmask: lower 4 bits must be zero, got %08b", bitmask)
	}

	pos++

	sn.NodeIndex = binary.BigEndian.Uint16(buff[pos : pos+2])
	pos += 2

	if bitmask&128 != 0 {
		if len(buff[pos:]) < 2 {
			return 0, fmt.Errorf("suspecting count field is missing")
		}
		sn.SuspectingCount = binary.BigEndian.Uint16(buff[pos : pos+2])
		if sn.SuspectingCount == 0 {
			return 0, fmt.Errorf("suspecting count cannot be zero if its bitmask is set")
		}
		pos += 2
	}
	if bitmask&64 != 0 {
		if len(buff[pos:]) < 2 {
			return 0, fmt.Errorf("redeeming count field is missing")
		}
		sn.RedeemingCount = binary.BigEndian.Uint16(buff[pos : pos+2])
		if sn.RedeemingCount == 0 {
			return 0, fmt.Errorf("redeeming count cannot be zero if its bitmask is set")
		}
		pos += 2
	}
	if bitmask&32 != 0 {
		if len(buff[pos:]) < 8 {
			return 0, fmt.Errorf("orbit suspected field is missing")
		}
		sn.OrbitSuspected = binary.BigEndian.Uint64(buff[pos : pos+8])
		if sn.OrbitSuspected == 0 {
			return 0, fmt.Errorf("orbit suspected cannot be zero if its bitmask is set")
		}
		pos += 8
	}
	if bitmask&16 != 0 {
		if len(buff[pos:]) < 8 {
			return 0, fmt.Errorf("orbit redeemed field is missing")
		}
		sn.OrbitToRedeem = binary.BigEndian.Uint64(buff[pos : pos+8])
		if sn.OrbitToRedeem == 0 {
			return 0, fmt.Errorf("orbit to redeem cannot be zero if its bitmask is set")
		}
		pos += 8
	}

	return pos, nil
}

func (sn *SuspectedNode) Len() int {
	var buffSize int

	if sn.SuspectingCount > 0 {
		buffSize += 2
	}
	if sn.RedeemingCount > 0 {
		buffSize += 2
	}
	if sn.OrbitSuspected > 0 {
		buffSize += 8
	}
	if sn.OrbitToRedeem > 0 {
		buffSize += 8
	}

	return buffSize + 1 + 2 // +1 for bitmask, +2 for node index
}

// isStillSuspected returns true if the suspected node is still suspected in the given orbit.
func (sn *SuspectedNode) isStillSuspected(threshold uint16, orbit uint64) bool {
	if sn.OrbitSuspected >= orbit {
		// Orbit hasn't advanced.
		return true
	}

	// Else, the orbit has advanced (sn.OrbitSuspected < orbit).
	if sn.SuspectingCount < threshold {
		// The node is not blacklisted, so since we've advanced to a new orbit,
		// we can just drop it as not enough nodes suspected it.
		return false
	}

	// Else, the node is blacklisted. We need to see if it has been redeemed by enough nodes.
	if sn.RedeemingCount >= threshold {
		return false
	}

	// Else, the node is still blacklisted.
	return true
}

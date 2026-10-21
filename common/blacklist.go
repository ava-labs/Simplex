// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package common

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

var (
	errBlacklistTooManyUpdates              = fmt.Errorf("too many blacklist updates")
	errBlacklistInvalidNodeIndex            = fmt.Errorf("invalid node index in blacklist update")
	errBlacklistNodeIndexAlreadyUpdated     = fmt.Errorf("node index already updated")
	errBlacklistNodeAlreadyBlacklisted      = fmt.Errorf("node index already blacklisted")
	errBlacklistNodeNotBlacklisted          = fmt.Errorf("node index not blacklisted, cannot be redeemed")
	errBlacklistInvalidUpdateType           = fmt.Errorf("invalid blacklist update type")
	errBlacklistInvalidNodeCount            = fmt.Errorf("block contains an invalid blacklist node count")
	errBlacklistInvalidMaxBlacklistedRounds = fmt.Errorf("block contains an invalid max blacklisted rounds")
	errBlacklistInvalidUpdates              = fmt.Errorf("block contains invalid blacklist updates")
	errBlacklistInvalidBlacklist            = fmt.Errorf("block contains an invalid blacklist")
)

// nodeOrbit returns the total number of times the given node has been selected as leader
// from genesis up to and including the specified round.
func nodeOrbit(round uint64, nodeIndex uint16, nodeCount uint16) uint64 {
	inCycle := round % uint64(nodeCount)
	cycles := round / uint64(nodeCount)

	if inCycle >= uint64(nodeIndex) {
		return cycles + 1
	}

	return cycles
}

// Blacklist stores the state of the blacklist.
// It can be derived by applying the recorded updates to the parent block's blacklist.
type Blacklist struct {
	// Configuration: Below are configuration fields that
	// are present in every Blacklist.

	// nodeCount is the configuration of the blacklist.
	nodeCount uint16 `canoto:"uint,1"`

	// maxBlacklistedRounds is the maximum number of rounds a node can be blacklisted for.
	// Each block builder may move it by at most nodeCount rounds relative to the parent block,
	// towards the value in its local configuration, which allows it to be changed via a rolling upgrade.
	maxBlacklistedRounds uint16 `canoto:"uint,2"`

	// State: Below are fields corresponding to state.
	// They are optional fields and may not be present
	// in a Blacklist.

	// suspectedNodes is the list of nodes that are currently suspected.
	// it's the inner state of the blacklist.
	suspectedNodes []suspectedNode `canoto:"repeated value,3"`

	// updates is the list of modifications that a block builder is proposing
	// to perform to the blacklist.
	updates []blacklistUpdate `canoto:"repeated value,4"`

	canotoData canotoData_Blacklist
}

func NewBlacklist(nodeCount uint16, maxBlacklistedRounds uint16) Blacklist {
	return Blacklist{
		nodeCount:            nodeCount,
		maxBlacklistedRounds: max(maxBlacklistedRounds, 1),
	}
}

// ForEpoch returns the blacklist that a child of a block with this blacklist builds on and is verified against.
// A parent from an earlier epoch, or without a blacklist, yields a blacklist without suspected nodes for nodeCount nodes.
// The maxBlacklistedRounds carries over, or is taken from the given maxBlacklistedRounds if the parent has none.
func (bl *Blacklist) ForEpoch(parentEpoch, epoch uint64, nodeCount, maxBlacklistedRounds uint16) Blacklist {
	if parentEpoch >= epoch && !bl.isEmpty() {
		return *bl
	}
	return NewBlacklist(nodeCount, cmp.Or(bl.maxBlacklistedRounds, maxBlacklistedRounds))
}

// Next returns the blacklist of a block proposed in the given round on top of this blacklist.
// It accuses and redeems nodes according to the given rounds in which they timed out and were redeemed,
// and moves maxBlacklistedRounds by at most nodeCount rounds towards the given maxBlacklistedRounds.
func (bl *Blacklist) Next(round uint64, timedOut, redeemed map[uint16]uint64, maxBlacklistedRounds uint16) Blacklist {
	updates := bl.computeBlacklistUpdates(round, timedOut, redeemed)
	next := bl.applyUpdates(updates, round)

	current, target := bl.maxBlacklistedRounds, max(maxBlacklistedRounds, 1)
	if current < target {
		next.maxBlacklistedRounds = current + min(target-current, bl.nodeCount)
	} else {
		next.maxBlacklistedRounds = current - min(current-target, bl.nodeCount)
	}
	return next
}

func (bl *Blacklist) NodeCount() uint16 {
	return bl.nodeCount
}

func (bl *Blacklist) MaxBlacklistedRounds() uint16 {
	return bl.maxBlacklistedRounds
}

func (bl *Blacklist) String() string {
	sn := make([]string, len(bl.suspectedNodes))
	for i := range bl.suspectedNodes {
		sn[i] = bl.suspectedNodes[i].String()
	}

	return fmt.Sprintf(
		"Blacklist(nodeCount=%d, maxBlacklistedRounds=%d, suspectedNodes=[%s], updates=[%s])",
		bl.nodeCount,
		bl.maxBlacklistedRounds,
		strings.Join(sn, ","),
		blacklistUpdatesAsString(bl.updates), // helper receives array
	)
}

func blacklistUpdatesAsString(updates []blacklistUpdate) string {
	out := make([]string, len(updates))
	for i := range updates {
		out[i] = updates[i].String()
	}
	return strings.Join(out, ",")
}

func (bl *Blacklist) Equals(b2 *Blacklist) bool {
	if bl.nodeCount != b2.nodeCount {
		return false
	}
	if bl.maxBlacklistedRounds != b2.maxBlacklistedRounds {
		return false
	}
	if len(bl.suspectedNodes) != len(b2.suspectedNodes) {
		return false
	}
	for i := range bl.suspectedNodes {
		sn1 := &bl.suspectedNodes[i]
		sn2 := &b2.suspectedNodes[i]
		if !sn1.Equals(sn2) {
			return false
		}
	}
	if len(bl.updates) != len(b2.updates) {
		return false
	}
	for i := range bl.updates {
		u1 := &bl.updates[i]
		u2 := &b2.updates[i]
		if !u1.Equals(u2) {
			return false
		}
	}
	return true
}

type blacklistOpType uint8

const (
	blacklistOpType_Undefined blacklistOpType = iota
	blacklistOpType_NodeSuspected
	blacklistOpType_NodeRedeemed
)

type blacklistUpdate struct {
	Type      blacklistOpType `canoto:"uint,1"`
	NodeIndex uint16          `canoto:"uint,2"`

	canotoData canotoData_blacklistUpdate
}

func (bu *blacklistUpdate) Equals(bu2 *blacklistUpdate) bool {
	return bu.Type == bu2.Type && bu.NodeIndex == bu2.NodeIndex
}

func (bu *blacklistUpdate) Clone() blacklistUpdate {
	return blacklistUpdate{
		Type:      bu.Type,
		NodeIndex: bu.NodeIndex,
	}
}

func (bu *blacklistUpdate) String() string {
	switch bu.Type {
	case blacklistOpType_NodeSuspected:
		return fmt.Sprintf("{type=NodeSuspected, NodeIndex=%d}", bu.NodeIndex)
	case blacklistOpType_NodeRedeemed:
		return fmt.Sprintf("{type=NodeRedeemed, NodeIndex=%d}", bu.NodeIndex)
	default:
		return fmt.Sprintf("{type=Unknown(%d), NodeIndex=%d}", bu.Type, bu.NodeIndex)
	}
}

func (bl *Blacklist) isEmpty() bool {
	return bl.nodeCount == 0
}

// applyUpdates applies the given updates in the given round to the current blacklist and returns a new blacklist.
// The current blacklist is not modified.
func (bl *Blacklist) applyUpdates(updates []blacklistUpdate, round uint64) Blacklist {
	newBlacklist := Blacklist{
		nodeCount:            bl.nodeCount,
		maxBlacklistedRounds: bl.maxBlacklistedRounds,
		suspectedNodes:       make([]suspectedNode, len(bl.suspectedNodes)),
		updates:              make([]blacklistUpdate, len(updates)),
	}

	newBlacklist.suspectedNodes = bl.garbageCollectSuspectedNodes(round)

	for i := range updates {
		update := &updates[i]
		orbit := nodeOrbit(round, update.NodeIndex, bl.nodeCount)
		switch update.Type {
		case blacklistOpType_NodeSuspected:
			newBlacklist.setNodeSuspected(orbit, update.NodeIndex)
		case blacklistOpType_NodeRedeemed:
			newBlacklist.setNodeRedeemed(orbit, update.NodeIndex)
		}
	}

	copy(newBlacklist.updates, updates)

	return newBlacklist
}

// garbageCollectSuspectedNodes returns a new list of suspected nodes for the given round.
// NodeIDs that are no longer suspected or have been redeemed, will not be included in the returned suspected nodes.
// It will also garbage-collect any redeem votes from past orbits, unless hey have surpassed the threshold of f+1.
// It does not modify the current blacklist.
func (bl *Blacklist) garbageCollectSuspectedNodes(round uint64) []suspectedNode {
	newSuspectedNodes := make([]suspectedNode, 0, len(bl.suspectedNodes))
	threshold := bl.computeThreshold()

	for i := range bl.suspectedNodes {
		sn := &bl.suspectedNodes[i]
		orbit := nodeOrbit(round, sn.NodeIndex, bl.nodeCount)
		// Node is no longer suspected, just skip it.
		if !sn.isStillSuspected(threshold, orbit) {
			continue
		}

		if orbit > sn.OrbitSuspected && (orbit-sn.OrbitSuspected)*uint64(bl.nodeCount) > uint64(bl.maxBlacklistedRounds) {
			// The node has been blacklisted for more than maxBlacklistedRounds, so we remove it from the suspected list.
			continue
		}

		newSuspectedNode := sn.Clone()

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
	if nodeIndex >= bl.nodeCount {
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

	// Count how many total nodes are suspected.
	for i := range bl.suspectedNodes {
		sn := &bl.suspectedNodes[i]

		if sn.SuspectingCount >= threshold {
			suspectedCount++
		}
	}

	f := (bl.nodeCount - 1) / 3

	if suspectedCount >= f {
		// We already have f or more nodes suspected, abort because we cannot suspect any more nodes.
		return
	}

	for i := range bl.suspectedNodes {
		sn := &bl.suspectedNodes[i]
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

	// If we reached here, the node isn't in the suspected list, and it is small enough, so add it instead.
	bl.suspectedNodes = append(bl.suspectedNodes, suspectedNode{
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
	if nodeIndex >= bl.nodeCount {
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
				bl.suspectedNodes = slices.Delete(bl.suspectedNodes, i, i+1)
				return
			}
			// Else, the orbit is different, so just return early.
			return
		}
	}
}

func (bl *Blacklist) computeThreshold() uint16 {
	f := (bl.nodeCount - 1) / 3
	threshold := f + 1
	return threshold
}

func (bl *Blacklist) IsNodeSuspected(nodeIndex uint16) bool {
	threshold := bl.computeThreshold()

	for i := range bl.suspectedNodes {
		sn := &bl.suspectedNodes[i]
		if sn.NodeIndex == nodeIndex {
			return sn.SuspectingCount >= threshold && sn.RedeemingCount < threshold
		}
	}

	return false
}

func (bl *Blacklist) Clone() Blacklist {
	clone := Blacklist{
		nodeCount:            bl.nodeCount,
		maxBlacklistedRounds: bl.maxBlacklistedRounds,
	}
	clone.suspectedNodes = make([]suspectedNode, 0, len(bl.suspectedNodes))
	for i := range bl.suspectedNodes {
		clone.suspectedNodes = append(clone.suspectedNodes, bl.suspectedNodes[i].Clone())
	}
	clone.updates = make([]blacklistUpdate, 0, len(bl.updates))
	for i := range bl.updates {
		clone.updates = append(clone.updates, bl.updates[i].Clone())
	}
	return clone
}

// Bytes returns the byte representation of the blacklist.
func (bl *Blacklist) Bytes() []byte {
	clone := bl.Clone()
	return clone.MarshalCanoto()
}

// FromBytes populates the blacklist from the given bytes.
func (bl *Blacklist) FromBytes(buff []byte) error {
	return bl.UnmarshalCanoto(buff)
}

func (bl *Blacklist) VerifyProposedBlacklist(candidateBlacklist Blacklist, round uint64) error {
	if candidateBlacklist.nodeCount != bl.nodeCount {
		return fmt.Errorf("%s, expected %d, got %d", errBlacklistInvalidNodeCount, bl.nodeCount, candidateBlacklist.nodeCount)
	}
	prevRounds, nextRounds := bl.maxBlacklistedRounds, candidateBlacklist.maxBlacklistedRounds
	if nextRounds == 0 || max(prevRounds, nextRounds)-min(prevRounds, nextRounds) > bl.nodeCount {
		return fmt.Errorf("%s, expected a non-zero value within %d rounds of %d, got %d", errBlacklistInvalidMaxBlacklistedRounds, bl.nodeCount, prevRounds, nextRounds)
	}
	// 1) First thing we check that the updates even make sense.
	if err := bl.verifyBlacklistUpdates(candidateBlacklist.updates); err != nil {
		return fmt.Errorf("%s: %w", errBlacklistInvalidUpdates, err)
	}
	updates := candidateBlacklist.updates
	// 2) We then proceed by applying the updates to the blacklist of the previous round.
	// Garbage collection of expired suspected nodes uses the maxBlacklistedRounds of the previous round,
	// and the new maxBlacklistedRounds only takes effect in the next round.
	expectedBlacklist := bl.applyUpdates(updates, round)
	expectedBlacklist.maxBlacklistedRounds = candidateBlacklist.maxBlacklistedRounds

	if !candidateBlacklist.Equals(&expectedBlacklist) {
		return fmt.Errorf("%s, expected %s, got %s", errBlacklistInvalidBlacklist, expectedBlacklist.String(), candidateBlacklist.String())
	}

	return nil
}

func (bl *Blacklist) verifyBlacklistUpdates(updates []blacklistUpdate) error {
	seen := make(map[uint16]struct{})
	if len(updates) > int(bl.nodeCount) {
		return fmt.Errorf("%w: %d, only %d nodes exist", errBlacklistTooManyUpdates, len(updates), bl.nodeCount)
	}
	for i := range updates {
		update := &updates[i]
		if update.NodeIndex >= bl.nodeCount {
			return fmt.Errorf("%w: %d, needs to be in [%d, %d]",
				errBlacklistInvalidNodeIndex, update.NodeIndex, 0, bl.nodeCount-1)
		}

		if _, exists := seen[update.NodeIndex]; exists {
			return fmt.Errorf("%w: (%d)", errBlacklistNodeIndexAlreadyUpdated, update.NodeIndex)
		}
		seen[update.NodeIndex] = struct{}{}

		switch update.Type {
		case blacklistOpType_NodeSuspected:
			// A node cannot be blacklisted if it is already suspected.
			if bl.IsNodeSuspected(update.NodeIndex) {
				return fmt.Errorf("%w: (%d)", errBlacklistNodeAlreadyBlacklisted, update.NodeIndex)
			}
		case blacklistOpType_NodeRedeemed:
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

func (bl *Blacklist) computeBlacklistUpdates(round uint64, timedOut, redeemed map[uint16]uint64) []blacklistUpdate {
	updates := make([]blacklistUpdate, 0, bl.nodeCount)

	for nodeIndex := uint16(0); nodeIndex < bl.nodeCount; nodeIndex++ {
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

			if roundsSinceLastRedeem > uint64(bl.nodeCount) {
				// The last time the node has redeemed was in an older orbit,
				// so ignore it. We can only redeem the node if it has redeemed in the current orbit.
				continue
			}

			updates = append(updates, blacklistUpdate{
				NodeIndex: nodeIndex,
				Type:      blacklistOpType_NodeRedeemed,
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

			if roundsSinceLastTimeout > uint64(bl.nodeCount) {
				// The node has not timed out for a full orbit yet, so do not suspect it,
				// because it has lastly timed out in a past orbit.
				continue
			}

			updates = append(updates, blacklistUpdate{
				NodeIndex: nodeIndex,
				Type:      blacklistOpType_NodeSuspected,
			})
		}
	}
	return updates
}

// suspectedNode is the information we keep for each suspected node.
// A suspected node records the number of accusations and when it was accused in.
// A suspected node that has above f+1 accusations is considered as blacklisted.
// A suspected node that is blacklisted can be redeemed by gathering f+1 redeeming votes.
// A suspected node that has been redeemed by f+1 votes or more is removed from the blacklist.
type suspectedNode struct {
	// NodeIndex is the index of the suspected node among the nodes of the validator set.
	NodeIndex uint16 `canoto:"uint,1"`
	// SuspectingCount is the number of nodes that have suspected this node in the current orbit denoted by OrbitSuspected.
	// If this count is >= f+1, then the node is considered blacklisted.
	SuspectingCount uint16 `canoto:"uint,2"`
	// RedeemingCount is the number of nodes that have redeemed this node in the current orbit denoted by OrbitToRedeem.
	// If this count reaches >= f+1, the node is removed from the blacklist.
	RedeemingCount uint16 `canoto:"uint,3"`
	// OrbitSuspected is the orbit in which the node was last suspected.
	OrbitSuspected uint64 `canoto:"uint,4"`
	// OrbitToRedeem is the orbit in which the node was last redeemed.
	OrbitToRedeem uint64 `canoto:"uint,5"`

	canotoData canotoData_suspectedNode
}

func (sn *suspectedNode) Clone() suspectedNode {
	return suspectedNode{
		NodeIndex:       sn.NodeIndex,
		SuspectingCount: sn.SuspectingCount,
		RedeemingCount:  sn.RedeemingCount,
		OrbitSuspected:  sn.OrbitSuspected,
		OrbitToRedeem:   sn.OrbitToRedeem,
	}
}

func (sn *suspectedNode) Equals(sn2 *suspectedNode) bool {
	return sn.NodeIndex == sn2.NodeIndex &&
		sn.SuspectingCount == sn2.SuspectingCount &&
		sn.RedeemingCount == sn2.RedeemingCount &&
		sn.OrbitSuspected == sn2.OrbitSuspected &&
		sn.OrbitToRedeem == sn2.OrbitToRedeem
}

func (sn *suspectedNode) String() string {
	return fmt.Sprintf("{NodeIndex=%d, SuspectingCount=%d, RedeemingCount=%d, OrbitSuspected=%d, OrbitToRedeem=%d}",
		sn.NodeIndex, sn.SuspectingCount, sn.RedeemingCount, sn.OrbitSuspected, sn.OrbitToRedeem)
}

// isStillSuspected returns true if the suspected node is still suspected in the given orbit.
func (sn *suspectedNode) isStillSuspected(threshold uint16, orbit uint64) bool {
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

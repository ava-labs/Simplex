// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"go.uber.org/zap"
)

type finalizedQuorumRound struct {
	block        Block
	finalization *Finalization
}

type ReplicationState struct {
	enabled  bool
	logger   Logger
	myNodeID NodeID

	// seqs maps sequences to a block and its associated finalization
	seqs map[uint64]*finalizedQuorumRound

	// rounds maps round numbers to QuorumRounds
	rounds map[uint64]*QuorumRound

	// digestTimeouts handles timeouts for fetching missing block digests.
	// When a notarization depends on a block we haven’t received yet,
	// it means a prior notarization for that block exists but is missing.
	// Since we may not know which round that dependency belongs to,
	// digestTimeouts ensures we re-request the missing digest until it arrives.
	digestTimeouts *TimeoutHandler[Digest]

	// emptyRoundTimeouts handles timeouts for fetching missing empty round notarizations.
	// When replication encounters a notarized block that depends on an empty round we haven't received,
	// emptyRoundTimeouts ensures we re-request those empty rounds until they are received.
	emptyRoundTimeouts *TimeoutHandler[uint64]

	roundRequestor        *requestor
	finalizationRequestor *requestor
}

func NewReplicationState(logger Logger, comm Communication, myNodeID NodeID, maxRoundWindow uint64, enabled bool, start time.Time, lock *sync.Mutex) *ReplicationState {
	if !enabled {
		return &ReplicationState{
			enabled: enabled,
			logger:  logger,
		}
	}

	r := &ReplicationState{
		enabled:  enabled,
		myNodeID: myNodeID,
		logger:   logger,

		// seq replication
		seqs:                  make(map[uint64]*finalizedQuorumRound),
		finalizationRequestor: newRequestor(logger, start, lock, maxRoundWindow, comm, true),

		// round replication
		rounds:         make(map[uint64]*QuorumRound),
		roundRequestor: newRequestor(logger, start, lock, maxRoundWindow, comm, false),
	}

	r.digestTimeouts = NewTimeoutHandler(logger, "digest", start, DefaultReplicationRequestTimeout, r.requestDigests)
	r.emptyRoundTimeouts = NewTimeoutHandler(logger, "empty", start, DefaultReplicationRequestTimeout, r.requestEmptyRounds)

	return r
}

func (r *ReplicationState) AdvanceTime(now time.Time) {
	if !r.enabled {
		return
	}

	r.finalizationRequestor.advanceTime(now)
	r.roundRequestor.advanceTime(now)
	r.digestTimeouts.Tick(now)
	r.emptyRoundTimeouts.Tick(now)
}

// deleteOldRounds cleans up the replication state for round replication after receiving a finalized round.
func (r *ReplicationState) deleteOldRounds(finalizedRound uint64) {
	for round := range r.rounds {
		if round <= finalizedRound {
			r.logger.Debug("Replication State Deleting Old Round", zap.Uint64("round", round))
			delete(r.rounds, round)
		}
	}

	r.roundRequestor.removeOldTasks(finalizedRound)
	r.emptyRoundTimeouts.RemoveOldTasks(func(r uint64, _ struct{}) bool {
		return r <= finalizedRound
	})
}

// storeSequence stores a block and finalization into the replication state
func (r *ReplicationState) storeSequence(block Block, finalization *Finalization) {
	if _, exists := r.seqs[finalization.Finalization.Seq]; exists {
		return
	}

	r.seqs[finalization.Finalization.Seq] = &finalizedQuorumRound{
		block:        block,
		finalization: finalization,
	}

	r.finalizationRequestor.removeTask(finalization.Finalization.Seq)
	r.digestTimeouts.RemoveTask(block.BlockHeader().Digest)
}

// storeRound adds or updates a quorum round in the replication state.
// If the round already exists, it merges any missing notarizations or empty notarizations
// from the provided quorum round. Otherwise, it stores the new round as is.
func (r *ReplicationState) storeRound(qr *QuorumRound) {
	if qr.Block != nil {
		r.digestTimeouts.RemoveTask(qr.Block.BlockHeader().Digest)
	}

	existing, exists := r.rounds[qr.GetRound()]
	if !exists {
		r.rounds[qr.GetRound()] = qr
		return
	}

	if qr.EmptyNotarization != nil && existing.EmptyNotarization == nil {
		existing.EmptyNotarization = qr.EmptyNotarization
	}

	if (qr.Notarization != nil && qr.Block != nil) && existing.Block == nil {
		existing.Notarization = qr.Notarization
		existing.Block = qr.Block
	}
}

// StoreQuorumRound stores the quorum round into the replication state.
func (r *ReplicationState) StoreQuorumRound(round *QuorumRound) {
	if !r.enabled {
		return
	}

	r.logger.Debug("Replication State Storing Quorum Round", zap.Stringer("QR", round))

	if round.Finalization != nil {
		r.storeSequence(round.Block, round.Finalization)
		r.finalizationRequestor.receivedSignedQuorum(newSignedQuorum(round, r.myNodeID))
		r.deleteOldRounds(round.Finalization.Finalization.Round)
		return
	}

	// otherwise we are storing a round without finalization
	// don't bother storing rounds that are older than the highest finalized round we know
	if observed := r.finalizationRequestor.getHighestObserved(); observed != nil && observed.round >= round.GetRound() {
		r.logger.Debug("Replication State received a finalized quorum round for a round we know is finalized.")
		return
	}

	r.storeRound(round)
	r.roundRequestor.receivedSignedQuorum(newSignedQuorum(round, r.myNodeID))
	if round.EmptyNotarization != nil {
		r.emptyRoundTimeouts.RemoveTask(round.GetRound())
	}
}

// receivedFutureFinalization notifies the replication state a finalization was created in a future round.
func (r *ReplicationState) ReceivedFutureFinalization(finalization *Finalization, nextSeqToCommit uint64) {
	if !r.enabled {
		return
	}

	signedSequence := newSignedQuorumFromFinalization(finalization, r.myNodeID)

	// maybe this finalization was for a round that we initially thought only had notarizations
	// remove from the round replicator since we now have a finalization for this round
	r.deleteOldRounds(finalization.Finalization.BlockHeader.Round)

	// potentially send out requests for blocks/finalizations in between
	r.finalizationRequestor.observedSignedQuorum(signedSequence, nextSeqToCommit)
}

// receivedFutureRound notifies the replication state of a future round.
func (r *ReplicationState) ReceivedFutureRound(round, seq, currentRound uint64, signers []NodeID) {
	if !r.enabled {
		return
	}

	if observed := r.finalizationRequestor.getHighestObserved(); observed != nil && observed.round >= round {
		r.logger.Debug("Ignoring round replication for a future round since we have a finalization for a higher round", zap.Uint64("round", round))
		return
	}

	sq := newSignedQuorumFromRound(round, seq, signers, r.myNodeID)
	r.roundRequestor.observedSignedQuorum(sq, currentRound)
}

// ResendFinalizationRequest notifies the replication state that `seq` should be re-requested.
func (r *ReplicationState) ResendFinalizationRequest(seq uint64, signers []NodeID) error {
	if !r.enabled {
		return nil
	}

	signers = NodeIDs(signers).Remove(r.myNodeID)
	numSigners := int64(len(signers))
	index, err := rand.Int(rand.Reader, big.NewInt(numSigners))
	if err != nil {
		return err
	}

	// because we are resending because the block failed to verify, we should remove the stored quorum round
	// so that we can try to get a new block & finalization
	delete(r.seqs, seq)
	r.finalizationRequestor.sendRequestToNode(seq, seq, signers[index.Int64()])
	return nil
}

// CreateDependencyTasks creates tasks to refetch the given parent digest and empty rounds. If there are no
// dependencies, no tasks are created.
// TODO: in a future PR, these requests will be sent as specific digest requests.
func (r *ReplicationState) CreateDependencyTasks(parent *Digest, parentSeq uint64, emptyRounds []uint64) {
	if parent != nil {
		r.digestTimeouts.AddTask(*parent)
	}

	if len(emptyRounds) > 0 {
		for _, round := range emptyRounds {
			r.emptyRoundTimeouts.AddTask(round)
		}
	}
}

func (r *ReplicationState) clearDependencyTasks(parent *Digest) {
	// TODO: for a future PR
}

// MaybeAdvanceState attempts to collect future sequences if
// there are more to be collected and the round has caught up for us to send the request.
func (r *ReplicationState) MaybeAdvanceState(nextSequenceToCommit uint64, currentRound uint64) {
	if !r.enabled {
		return
	}

	if nextSequenceToCommit > 0 {
		r.deleteOldRounds(nextSequenceToCommit - 1)
		r.finalizationRequestor.removeOldTasks(nextSequenceToCommit- 1)
	}

	// update the requestors in case they need to send more requests
	r.finalizationRequestor.updateState(nextSequenceToCommit)
	r.roundRequestor.updateState(currentRound)
}

func (r *ReplicationState) GetFinalizedBlockForSequence(seq uint64) (Block, *Finalization, bool) {
	blockWithFinalization, exists := r.seqs[seq]
	if !exists {
		return nil, nil, false
	}

	return blockWithFinalization.block, blockWithFinalization.finalization, true
}

func (r *ReplicationState) GetLowestRound() *QuorumRound {
	var lowestRound *QuorumRound

	for round, qr := range r.rounds {
		if lowestRound == nil {
			lowestRound = qr
			continue
		}

		if lowestRound.GetRound() > round {
			lowestRound = qr
		}
	}

	return lowestRound
}

func (r *ReplicationState) GetBlockWithSeq(seq uint64) Block {
	block, _, _ := r.GetFinalizedBlockForSequence(seq)
	if block != nil {
		return block
	}

	// check rounds replicator.
	// note: this is not deterministic since we can have multiple blocks notarized with the same seq
	// its fine to return since the caller can still optimistically advance the round
	for _, round := range r.rounds {
		if round.GetSequence() == seq && round.Block != nil {
			return round.Block
		}
	}

	return nil
}

func (r *ReplicationState) requestDigests(digests []Digest) {
	// TODO: In a future PR, I will add a message that requests a specific digest.
	r.logger.Debug("Not implemented yet", zap.Stringers("Digests", digests))
}

func (r *ReplicationState) requestEmptyRounds(emptyRounds []uint64) {
	r.logger.Debug("Replication State requesting empty rounds", zap.Uint64s("empty rounds", emptyRounds))
	r.roundRequestor.resendReplicationRequests(emptyRounds)
}

func (r *ReplicationState) DeleteRound(round uint64) {
	if !r.enabled {
		return
	}

	r.logger.Debug("Replication State Removing Round", zap.Uint64("round", round))
	r.emptyRoundTimeouts.RemoveTask(round)
	r.roundRequestor.removeTask(round)
	delete(r.rounds, round)
}

func (r *ReplicationState) DeleteSeq(seq uint64) {
	if !r.enabled {
		return
	}

	delete(r.seqs, seq)
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"math/rand/v2"
	"sync"
	"time"

	"go.uber.org/zap"
)

type seqAndDigest struct {
	digest Digest
	seq    uint64
}

func seqAndDigestFromBlock(block Block) seqAndDigest {
	return seqAndDigest{
		digest: block.BlockHeader().Digest,
		seq:    block.BlockHeader().Seq,
	}
}

type finalizedQuorumRound struct {
	block        Block
	finalization *Finalization
}

type ReplicationState struct {
	enabled  bool
	logger   Logger
	myNodeID NodeID
	rand     *rand.Rand // Random number generator

	// seqs maps sequences to a block and its associated finalization
	seqs map[uint64]*finalizedQuorumRound

	// rounds maps round numbers to QuorumRounds
	rounds map[uint64]*QuorumRound

	// digestTimeouts handles timeouts for fetching missing block digests.
	// When a notarization depends on a block we haven't received yet,
	// it means a prior notarization for that block exists but is missing.
	// Since we may not know which round that dependency belongs to,
	// digestTimeouts ensures we re-request the missing digest until it arrives.
	digestTimeouts *TimeoutHandler[seqAndDigest]

	// emptyRoundTimeouts handles timeouts for fetching missing empty round notarizations.
	// When replication encounters a notarized block that depends on an empty round we haven't received,
	// emptyRoundTimeouts ensures we re-request those empty rounds until they are received.
	emptyRoundTimeouts *TimeoutHandler[uint64]

	roundRequestor        *requestor
	finalizationRequestor *requestor

	sender    sender
	epochLock *sync.Mutex
}

func NewReplicationState(logger Logger, comm Communication, myNodeID NodeID, maxRoundWindow uint64, enabled bool, start time.Time, lock *sync.Mutex, rng *rand.Rand) *ReplicationState {
	if !enabled {
		return &ReplicationState{
			enabled: enabled,
			logger:  logger,
			rand:    rng,
		}
	}

	r := &ReplicationState{
		enabled:  enabled,
		myNodeID: myNodeID,
		logger:   logger,
		rand:     rng,

		// seq replication
		seqs:                  make(map[uint64]*finalizedQuorumRound),
		finalizationRequestor: newRequestor(logger, start, lock, maxRoundWindow, comm, true),

		// round replication
		rounds:         make(map[uint64]*QuorumRound),
		roundRequestor: newRequestor(logger, start, lock, maxRoundWindow, comm, false),

		sender:    comm,
		epochLock: lock,
	}

	r.digestTimeouts = NewTimeoutHandler(logger, "digest", start, DefaultReplicationRequestTimeout, r.requestDigests)
	r.emptyRoundTimeouts = NewTimeoutHandler(logger, "empty round replication", start, DefaultReplicationRequestTimeout, r.requestEmptyRounds)

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
func (r *ReplicationState) storeSequence(block Block, finalization *Finalization) bool {
	if _, exists := r.seqs[finalization.Finalization.Seq]; exists {
		return false
	}

	r.seqs[finalization.Finalization.Seq] = &finalizedQuorumRound{
		block:        block,
		finalization: finalization,
	}

	r.finalizationRequestor.removeTask(finalization.Finalization.Seq)
	r.digestTimeouts.RemoveTask(seqAndDigestFromBlock(block))
	return true
}

// storeRound adds or updates a quorum round in the replication state.
// If the round already exists, it merges any missing notarizations or empty notarizations
// from the provided quorum round. Otherwise, it stores the new round as is.
func (r *ReplicationState) storeRound(qr *QuorumRound) {
	if qr.Block != nil {
		r.digestTimeouts.RemoveTask(seqAndDigestFromBlock(qr.Block))
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

	if round.Finalization != nil {
		stored := r.storeSequence(round.Block, round.Finalization)
		if !stored {
			return
		}
		r.finalizationRequestor.receivedSignedQuorum(newSignedQuorum(round, r.myNodeID))
		r.deleteOldRounds(round.Finalization.Finalization.Round)
		r.logger.Debug("Replication State Storing Quorum Round", zap.Stringer("QR", round))
		return
	}

	// otherwise we are storing a round without finalization
	// don't bother storing rounds that are older than the highest finalized round we know
	if observed := r.finalizationRequestor.getHighestObserved(); observed != nil && observed.round >= round.GetRound() {
		r.logger.Debug("Replication State received a finalized quorum round for a round we know is finalized.")
		return
	}

	r.logger.Debug("Replication State Storing Quorum Round", zap.Stringer("QR", round))
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
	index := r.rand.Int64N(numSigners)

	// because we are resending because the block failed to verify, we should remove the stored quorum round
	// so that we can try to get a new block & finalization
	r.DeleteSeq(seq)
	r.finalizationRequestor.sendRequestToNode(seq, seq, signers[index])
	return nil
}

// CreateDependencyTasks creates tasks to refetch the given parent digest and empty rounds. If there are no
// dependencies, no tasks are created.
func (r *ReplicationState) CreateDependencyTasks(parent *Digest, parentSeq uint64, emptyRounds []uint64) {
	if parent != nil {
		r.digestTimeouts.AddTask(seqAndDigest{digest: *parent, seq: parentSeq})
	}

	if len(emptyRounds) > 0 {
		for _, round := range emptyRounds {
			r.emptyRoundTimeouts.AddTask(round)
		}
	}
}

func (r *ReplicationState) clearBlockDependencyTasks(digest Digest, seq uint64, finalizationPersisted bool) {
	if !r.enabled {
		return
	}

	// if the finalization is persisted, we can clear all tasks for sequences up to and including seq
	if finalizationPersisted {
		r.digestTimeouts.RemoveOldTasks(func(missingBlock seqAndDigest, _ struct{}) bool {
			return missingBlock.seq <= seq
		})

		return
	}

	r.digestTimeouts.RemoveTask(seqAndDigest{
		digest: digest,
		seq:    seq,
	})
}

// MaybeAdvanceState attempts to collect future sequences if
// there are more to be collected and the round has caught up for us to send the request.
func (r *ReplicationState) MaybeAdvanceState(nextSequenceToCommit uint64, currentRound uint64, lastCommittedRound uint64) {
	if !r.enabled {
		return
	}

	if lastCommittedRound > 0 {
		r.deleteOldRounds(lastCommittedRound)
	}

	if nextSequenceToCommit > 0 {
		r.finalizationRequestor.removeOldTasks(nextSequenceToCommit - 1)
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

// GetHighestRound returns the highest round known to the replicator.
func (r *ReplicationState) GetHighestRound() uint64 {
	var highestRound uint64

	for round, qr := range r.rounds {
		if highestRound < round {
			highestRound = qr.GetRound()
		}
	}

	for _, qr := range r.seqs {
		if qr.block.BlockHeader().Round > highestRound {
			highestRound = qr.block.BlockHeader().Round
		}
	}

	return highestRound
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

func (r *ReplicationState) requestDigests(missingBlocks []seqAndDigest) {
	// grab the lock since this is called in the timeout handler goroutine
	r.epochLock.Lock()
	defer r.epochLock.Unlock()

	signedQuorum := r.roundRequestor.getHighestObserved()
	if signedQuorum == nil {
		signedQuorum = r.finalizationRequestor.getHighestObserved()
	}
	if signedQuorum == nil {
		r.logger.Warn("Replication State cannot request missing block digests, no known nodes to request from")
		return
	}

	if len(signedQuorum.signers) == 0 {
		r.logger.Error("Replication State cannot request missing block digests, no known nodes to request from")
		return
	}

	startingIndex := int(r.rand.Int64N(int64(len(signedQuorum.signers))))

	for i, mb := range missingBlocks {
		// grab the node to send it to
		index := (i + startingIndex) % len(signedQuorum.signers)
		node := signedQuorum.signers[index]

		r.logger.Debug("Replication State requesting missing block digest", zap.Uint64("seq", mb.seq), zap.Stringer("digest", &mb.digest))
		blockRequest := &BlockDigestRequest{
			Seq:    mb.seq,
			Digest: mb.digest,
		}
		r.sender.Send(&Message{
			BlockDigestRequest: blockRequest,
		}, node)
	}
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

func (r *ReplicationState) Close() {
	if !r.enabled {
		return
	}

	r.digestTimeouts.Close()
	r.emptyRoundTimeouts.Close()
}

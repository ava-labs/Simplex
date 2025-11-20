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
	enabled            bool
	logger             Logger
	myNodeID           NodeID


	// receivedFinalizations maps either sequences or rounds to quorum rounds
	seqs map[uint64]*finalizedQuorumRound
	finalizationRequestor *requestor

	// for rounds
	rounds         map[uint64]*QuorumRound
	digestTimeouts *TimeoutHandler[Digest]
	// used to remove old Digests from the digest timeout
	seqsToDigests map[uint64]Digest
	roundTimeouts  *TimeoutHandler[uint64]
	roundRequestor *requestor
}

func NewReplicationState(logger Logger, comm Communication, myNodeID NodeID, maxRoundWindow uint64, enabled bool, start time.Time, lock *sync.Mutex) *ReplicationState {
	if !enabled {
		return &ReplicationState{
			enabled: enabled,
			logger:  logger,
		}
	}

	r := &ReplicationState{
		enabled:            enabled,
		myNodeID:           myNodeID,
		
		// seq replication
		seqs: 			 make(map[uint64]*finalizedQuorumRound),
		finalizationRequestor: newRequestor(logger, start, lock, maxRoundWindow, comm, myNodeID, true),

		// round replication
		rounds: make(map[uint64]*QuorumRound),
	}

	r.digestTimeouts = NewTimeoutHandler(logger, start, DefaultReplicationRequestTimeout, r.requestDigests, alwaysFalseRemover[Digest])
	r.roundTimeouts = NewTimeoutHandler(logger, start, DefaultReplicationRequestTimeout, r.requestEmptyRounds, alwaysFalseRemover[uint64])

	return r
}

func (r *ReplicationState) AdvanceTime(now time.Time) {
	if !r.enabled {
		return
	}

	r.finalizationRequestor.advanceTime(now)
	r.roundRequestor.advanceTime(now)
	r.digestTimeouts.Tick(now)
	r.roundTimeouts.Tick(now)
}

func (r *ReplicationState) deleteOldRounds(finalizedRound uint64) {
	for round := range r.rounds {
		if round <= finalizedRound {
			delete(r.rounds, round)
		}
	}

	// we don't want to call RemoveOldTasks since we still may need empty rounds < finalized round
	r.roundTimeouts.RemoveTask(finalizedRound)
	r.roundRequestor.removeOldTasks(finalizedRound)
}

func (r *ReplicationState) storeSequence(block Block, finalization *Finalization) {
	if _, exists := r.seqs[finalization.Finalization.Seq]; exists {
		// we've already stored this round
		return
	}

	r.seqs[finalization.Finalization.Seq] = &finalizedQuorumRound{
		block:        block,
		finalization: finalization,
	}
}

func (r *ReplicationState) storeRound(qr *QuorumRound) {
	// check if a round exists already. if exists we may need to merge the notarizations and empty notarizations
	if existingQR, exists := r.rounds[qr.GetRound()]; exists {
		if qr.EmptyNotarization != nil && existingQR.EmptyNotarization == nil {
			existingQR.EmptyNotarization = qr.EmptyNotarization
		}

		if (qr.Block == nil && qr.Notarization == nil) && (existingQR.Notarization != nil && existingQR.EmptyNotarization != nil) {
			existingQR.Notarization = qr.Notarization
			existingQR.Block = qr.Block
		}

		return
	}

	r.rounds[qr.GetRound()] = qr
}

func (r *ReplicationState) StoreQuorumRound(round *QuorumRound, from NodeID) {
	if round.Finalization != nil {
		r.storeSequence(round.Block, round.Finalization)
		r.finalizationRequestor.receivedSignedQuorum(newSignedQuorum(round, r.myNodeID))

		r.deleteOldRounds(round.Finalization.Finalization.Round)
		return
	}

	// otherwise we are storing a round without finalization
	// don't bother storing rounds that are older than the highest finalized round we know
	// todo: grab a lock for sequence replicator
	if r.finalizationRequestor.getHighestRound() >= round.GetRound() {
		return
	}


	r.storeRound(round)
	r.roundRequestor.receivedSignedQuorum(newSignedQuorum(round, r.myNodeID))
}

// receivedFutureFinalization processes a finalization that was created in a future round.
func (r *ReplicationState) ReceivedFutureFinalization(finalization *Finalization, nextSeqToCommit uint64) {
	if !r.enabled {
		return
	}

	signedSequence := newSingedQuorumFromFinalization(finalization, r.myNodeID)

	// maybe this finalization was for a round that we initially thought only had notarizations
	// remove from the round replicator since we now have a finalization for this round
	r.deleteOldRounds(finalization.Finalization.BlockHeader.Round)

	// potentially send out requests for blocks/finalizations in between
	r.finalizationRequestor.maybeSendMoreReplicationRequests(signedSequence, nextSeqToCommit)
}

func (r *ReplicationState) ReceivedFutureRound(round, seq, currentRound uint64, signers []NodeID) {
	if !r.enabled {
		return
	}

	if r.finalizationRequestor.getHighestRound() >= round {
		r.logger.Debug("Ignoring round replication for a future round since we have a finalization for a higher round", zap.Uint64("round", round))
		return
	}

	sq := newSignedQuorumFromRound(round, seq, signers, r.myNodeID)
	r.roundRequestor.maybeSendMoreReplicationRequests(sq, currentRound)
}

func (r *ReplicationState) ResendFinalizationRequest(seq uint64, signers []NodeID) error {
	if !r.enabled {
		return nil
	}

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

func (r *ReplicationState) CreateDependencyTasks(parent *Digest, parentSeq uint64, emptyRounds []uint64) {
	if parent != nil {
		r.digestTimeouts.AddTask(*parent)
		r.seqsToDigests[parentSeq] = *parent
	}

	if len(emptyRounds) > 0 {
		for _, round := range emptyRounds {
			r.roundTimeouts.AddTask(round)
		}
	}
}

func (r *ReplicationState) clearDependencyTasks(parent *Digest) {}


// maybeSendFutureRequests attempts to collect future sequences if
// there are more to be collected and the round has caught up for us to send the request.
func (r *ReplicationState) MaybeAdvancedState(nextSequenceToCommit uint64, currentRound uint64) {
	if !r.enabled {
		return
	}

	r.deleteOldRounds(nextSequenceToCommit - 1)

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

func (r *ReplicationState) requestDigests(digest []Digest) {
	// TODO: create a specific message for this
}

func (r *ReplicationState) requestEmptyRounds(emptyRounds []uint64) {
	// we can just request quorum rounds for this.
	// we can optimize the reuqest by adding a flag for just emoty notarization but for now we don't

	for _, emptyRound := range emptyRounds {
		// we could use sendRequestToNode, but this is nice since it distributes
		// we could also group them
		r.roundRequestor.sendReplicationRequests(emptyRound, emptyRound)
	}
}
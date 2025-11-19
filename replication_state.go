// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"sync"
	"time"
)

type ReplicationState struct {
	enabled            bool
	logger             Logger
	sequenceReplicator *finalizationReplicator
	roundReplicator    *roundReplicator
	myNodeID           NodeID
}

func NewReplicationState(logger Logger, comm Communication, id NodeID, maxRoundWindow uint64, enabled bool, start time.Time, lock *sync.Mutex) *ReplicationState {
	if !enabled {
		return &ReplicationState{
			enabled: enabled,
			logger:  logger,
		}
	}

	return &ReplicationState{
		enabled:            enabled,
		sequenceReplicator: newSeqReplicator(logger, comm, id, maxRoundWindow, start, lock),
		roundReplicator:    newReplicator(logger, comm, id, maxRoundWindow, start, lock),
		logger:   logger,
		myNodeID: id,
	}
}

func (r *ReplicationState) AdvanceTime(now time.Time) {
	if !r.enabled {
		return
	}
	r.sequenceReplicator.advanceTime(now)
	// r.roundReplicator.advanceTime(now)
}

// maybeSendFutureRequests attempts to collect future sequences if
// there are more to be collected and the round has caught up for us to send the request.
func (r *ReplicationState) maybeAdvancedState(nextSequenceToCommit uint64, currentRound uint64) {
	if !r.enabled {
		return
	}

	r.sequenceReplicator.updateState(nextSequenceToCommit)
	// r.roundReplicator.updateState(currentRound)
}

func (r *ReplicationState) storeQuorumRound(round *QuorumRound, from NodeID) {
	if round.Finalization != nil {
		r.sequenceReplicator.storeQuorumRound(round)
		r.roundReplicator.deleteOldRounds(round.Finalization.Finalization.Round)
		return
	}

	// otherwise we are storing a round without finalization
	// don't bother storing rounds that are older than the highest finalized round we know
	// todo: grab a lock for sequence replicator
	if r.sequenceReplicator.getHighestRound() >= round.GetRound() {
		return
	}

	r.roundReplicator.storeQuorumRound(round)
}

func (r *ReplicationState) getFinalizedBlockForSequence(seq uint64) (Block, *Finalization, bool) {
	return r.sequenceReplicator.retrieveBlockAndFinalization(seq)
}

func (r *ReplicationState) getBlockWithSeq(seq uint64) Block {
	block, _, _ := r.sequenceReplicator.retrieveBlockAndFinalization(seq)
	if block != nil {
		return block
	}

	// check notarization replicator.
	// note: this is not deterministic since we can have multiple blocks notarized with the same seq
	// its fine to return since the caller can still optimistically advance the round
	return r.roundReplicator.getBlockWithSeq(seq)
}

func (r *ReplicationState) resendFinalizationRequest(seq uint64, signers []NodeID) error {
	if !r.enabled {
		return nil
	}

	return r.sequenceReplicator.resendFinalizationRequest(seq, signers)
}

func (r *ReplicationState) getNonFinalizedQuorumRound(round uint64) *QuorumRound {
	// qr, ok := r.roundReplicator.retrieveQuorumRound(round)
	// if ok {
	// 	return qr
	// }
	return nil
}

// receivedFutureFinalization processes a finalization that was created in a future round.
func (r *ReplicationState) receivedFutureFinalization(finalization *Finalization, nextSeqToCommit uint64) {
	if !r.enabled {
		return
	}

	signedSequence := newSignedQuorum(finalization, r.myNodeID)

	// maybe this finalization was for a round that we initially thought only had notarizations
	// remove from the round replicator since we now have a finalization for this round
	// r.roundReplicator.removeOldValues(finalization.Finalization.BlockHeader.Round)
	r.sequenceReplicator.requestor.maybeSendMoreReplicationRequests(signedSequence, nextSeqToCommit)
}

func (r *ReplicationState) receivedFutureRound(round uint64, signers []NodeID, currentRound uint64) {
	if !r.enabled {
		return
	}

	// if r.sequenceReplicator.getHighestRound() >= round {
	// 	r.logger.Debug("Ignoring round replication for a future round since we have a finalization for a higher round", zap.Uint64("round", round))
	// 	return
	// }

	// signedSequence := newSignedRoundOrSeqFromRound(round, signers, r.roundReplicator.myNodeID)
	// r.roundReplicator.maybeSendMoreReplicationRequests(signedSequence, currentRound)
}

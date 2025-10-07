// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"
)

type ReplicationState struct {
	enabled            bool
	lock               *sync.Mutex
	logger             Logger
	sequenceReplicator *sequenceReplicator
	roundReplicator    *roundReplicator
}

func NewReplicationState(logger Logger, comm Communication, id NodeID, maxRoundWindow uint64, enabled bool, start time.Time, lock *sync.Mutex) *ReplicationState {
	if !enabled {
		return &ReplicationState{
			enabled: enabled,
			lock:    lock,
			logger:  logger,
		}
	}

	return &ReplicationState{
		lock:               lock,
		logger:             logger,
		enabled:            enabled,
		sequenceReplicator: newSequenceReplicator(logger, comm, id, maxRoundWindow, start),
		roundReplicator:    newRoundReplicator(logger, comm, id, maxRoundWindow, start),
	}
}

func (r *ReplicationState) AdvanceTime(now time.Time) {
	r.sequenceReplicator.advanceTime(now)
}

// isReplicationComplete returns true if we have finished the replication process.
// The process is considered finished once [currentRound] has caught up to the highest round received.
func (r *ReplicationState) isReplicationComplete(nextSeqToCommit uint64, currentRound uint64) bool {
	if !r.enabled {
		return true
	}

	return r.sequenceReplicator.isReplicationComplete(nextSeqToCommit) && r.roundReplicator.isReplicationComplete(currentRound)
}

// receivedFutureFinalization processes a finalization that was created in a future round.
func (r *ReplicationState) receivedFutureFinalization(finalization *Finalization, nextSeqToCommit uint64) {
	if !r.enabled {
		return
	}

	r.sequenceReplicator.receivedFutureSequence(finalization, nextSeqToCommit)
	r.roundReplicator.receivedFutureRound(finalization.Finalization.BlockHeader.Round)
}

// maybeSendFutureRequests attempts to collect future sequences if
// there are more to be collected and the round has caught up for us to send the request.
func (r *ReplicationState) maybeAdvancedState(nextSequenceToCommit uint64, currentRound uint64) {
	if !r.enabled {
		return
	}

	r.sequenceReplicator.maybeAdvancedState(nextSequenceToCommit)
	// TODO: we need to ensure that if the sequenceReplicator advances, the roundReplicator deoes not send out extra requests
	r.roundReplicator.receivedFutureRound(currentRound)
}

func (r *ReplicationState) handleQuorumRound(round QuorumRound, from NodeID) {
	if round.Finalization != nil {
		r.sequenceReplicator.storeQuorumRound(round, from)
		r.roundReplicator.receivedFutureRound(round.GetRound())
		return
	}

	// don't bother storing rounds that are older than the highest finalized round we know
	// todo: grab a lock for sequence replicator
	if r.sequenceReplicator.highestSequenceObserved.round >= round.GetRound() {
		return
	}

	r.roundReplicator.storeQuorumRound(round, from)
}

func (r *ReplicationState) getFinalizedBlockForSequence(seq uint64) (Block, Finalization, bool) {
	return r.sequenceReplicator.retrieveFinalizedBlock(seq)
}

func (r *ReplicationState) resendFinalizationRequest(seq uint64, signers []NodeID) error {
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
	delete(r.sequenceReplicator.receivedQuorumRounds, seq)
	r.sequenceReplicator.sendRequestToNode(seq, seq, signers, int(index.Int64()))
	return nil
}

func (r *ReplicationState) getNonFinalizedQuorumRound(round uint64) *QuorumRound {
	qr, ok := r.roundReplicator.rounds[round]
	if ok {
		return &qr
	}
	return nil
}
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

type ReplicationState struct {
	enabled            bool
	lock               *sync.Mutex
	logger             Logger
	sequenceReplicator *replicator
	roundReplicator    *replicator
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
		sequenceReplicator: newReplicator(logger, comm, id, maxRoundWindow, start),
		roundReplicator:    newReplicator(logger, comm, id, maxRoundWindow, start),
	}
}

func (r *ReplicationState) AdvanceTime(now time.Time) {
	r.sequenceReplicator.advanceTime(now)
	r.roundReplicator.advanceTime(now)
}

// isReplicationComplete returns true if we have finished the replication process.
// The process is considered finished once [currentRound] has caught up to the highest round received.
func (r *ReplicationState) isReplicationComplete(nextSeqToCommit uint64, currentRound uint64) bool {
	if !r.enabled {
		return true
	}

	highestSeqObserved := r.sequenceReplicator.getHighestObserved()
	if highestSeqObserved != nil && highestSeqObserved.value() >= nextSeqToCommit {
		return false
	}

	highestRoundObserved := r.roundReplicator.getHighestObserved()
	if highestRoundObserved != nil && highestRoundObserved.value() >= currentRound {
		return false
	}

	return true
}

// maybeSendFutureRequests attempts to collect future sequences if
// there are more to be collected and the round has caught up for us to send the request.
func (r *ReplicationState) maybeAdvancedState(nextSequenceToCommit uint64, currentRound uint64) {
	if !r.enabled {
		return
	}

	r.sequenceReplicator.updateState(nextSequenceToCommit)
	r.roundReplicator.updateState(currentRound)
}

func (r *ReplicationState) handleQuorumRound(round QuorumRound, from NodeID) {
	if round.Finalization != nil {
		r.sequenceReplicator.storeQuorumRound(round, from, round.Finalization.Finalization.Seq)
		r.roundReplicator.removeOldValues(round.Finalization.Finalization.Round)
		return
	}

	// otherwise we are storing a round without finalization
	// don't bother storing rounds that are older than the highest finalized round we know
	// todo: grab a lock for sequence replicator
	if r.sequenceReplicator.highestObserved.round >= round.GetRound() {
		return
	}

	r.roundReplicator.storeQuorumRound(round, from, round.GetRound())
}

func (r *ReplicationState) getFinalizedBlockForSequence(seq uint64) (Block, Finalization, bool) {
	qr, ok := r.sequenceReplicator.retrieveQuorumRound(seq)
	if !ok || qr.Finalization == nil || qr.Block == nil {
		return nil, Finalization{}, false
	}

	return qr.Block, *qr.Finalization, true
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
	qr, ok := r.sequenceReplicator.retrieveQuorumRound(round)
	if ok {
		return qr
	}
	return nil
}

// receivedFutureFinalization processes a finalization that was created in a future round.
func (r *ReplicationState) receivedFutureFinalization(finalization *Finalization, nextSeqToCommit uint64) {
	if !r.enabled {
		return
	}

	signedSequence := &signerRoundOrSeq{
		round:   finalization.Finalization.Round,
		seq:     finalization.Finalization.Seq,
		signers: finalization.QC.Signers(),
		isRound: false,
	}

	r.sequenceReplicator.receivedFutureValue(signedSequence, nextSeqToCommit)
	// maybe this finalization was for a round that we initially thought only had notarizations
	// remove from the round replicator since we now have a finalization for this round
	r.roundReplicator.removeOldValues(finalization.Finalization.BlockHeader.Round)
}

func (r *ReplicationState) receivedFutureRound(round uint64, signers []NodeID, currentRound uint64) {
	if !r.enabled {
		return
	}

	signedSequence := &signerRoundOrSeq{
		round:   round,
		seq:     0, // seq not needed for round replicator
		signers: signers,
		isRound: true,
	}

	// 	// check if we have a finalization > than this round
	if r.sequenceReplicator.highestObserved.round >= round {
		r.logger.Debug("Ignoring round replication for a future round since we have a finalization for a higher round", zap.Uint64("round", round))
		return
	}
	r.roundReplicator.receivedFutureValue(signedSequence, currentRound)
}

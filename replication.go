// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

func newSignedSequenceFromRound(round QuorumRound) (*signedSequence, error) {
	ss := &signedSequence{}
	switch {
	case round.Finalization != nil:
		ss.signers = round.Finalization.QC.Signers()
		ss.seq = round.Finalization.Finalization.Seq
	case round.Notarization != nil:
		ss.signers = round.Notarization.QC.Signers()
		ss.seq = round.Notarization.Vote.Seq
	case round.EmptyNotarization != nil:
		return nil, fmt.Errorf("should not create signed sequence from empty notarization")
	default:
		return nil, fmt.Errorf("round does not contain a finalization, empty notarization, or notarization")
	}

	return ss, nil
}

type ReplicationState struct {
	enabled        bool
	lock           *sync.Mutex
	logger         Logger
	sequenceReplicator *sequenceReplicator
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
		lock:                 lock,
		logger:               logger,
		enabled:              enabled,
		sequenceReplicator:   newSequenceReplicator(logger, comm, id, maxRoundWindow, start),
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

	// TODO: their would be a round replicator as well
	return r.sequenceReplicator.isReplicationComplete(nextSeqToCommit) && currentRound > r.highestKnownRound()
}

func (r *ReplicationState) receivedFutureFinalization(finalization *Finalization, nextSeqToCommit uint64) {
	if !r.enabled {
		return
	}

	r.sequenceReplicator.receivedFutureSequence(finalization, nextSeqToCommit)
	// TODO: also update the roundReplicator and ensure the round replicator stops timeout tasks for the rounds < finalization.Round
}

// receivedReplicationResponse notifies the task handler a response was received. If the response
// was incomplete(meaning our timeout expected more seqs), then we will create a new timeout
// for the missing sequences and send the request to a different node.
func (r *ReplicationState) receivedReplicationResponse(data []QuorumRound, node NodeID) {
	finalizedSeqs := make([]uint64, 0, len(data))
	quorumRounds := make([]QuorumRound, 0, len(data))

	for _, qr := range data {
		if qr.Finalization != nil {
			finalizedSeqs = append(finalizedSeqs, qr.GetSequence())
		} else {
			quorumRounds = append(quorumRounds, qr)
		}
	}
	
	r.sequenceReplicator.receivedFinalizationSeqs(finalizedSeqs, node)
	// todo: also notify the round replicator of the received quorum rounds
}

// maybeSendFutureRequests attempts to collect future sequences if
// there are more to be collected and the round has caught up for us to send the request.
func (r *ReplicationState) maybeAdvancedState(nextSequenceToCommit uint64, currentRound uint64) {
	if !r.enabled {
		return
	}

	r.sequenceReplicator.maybeAdvancedState(nextSequenceToCommit)
	// todo: maybe advance the round replicator as well
}

func (r *ReplicationState) StoreQuorumRound(round QuorumRound) {
	if round.Finalization != nil {
		r.sequenceReplicator.storeQuorumRound(round)
	}


	if _, ok := r.receivedQuorumRounds[round.GetRound()]; ok {
		// maybe this quorum round was behind
		if r.receivedQuorumRounds[round.GetRound()].Finalization == nil && round.Finalization != nil {
			r.receivedQuorumRounds[round.GetRound()] = round
		}
		return
	}

	if round.EmptyNotarization == nil && round.GetSequence() > r.highestSequenceObserved.seq {
		signedSeq, err := newSignedSequenceFromRound(round)
		if err != nil {
			// should never be here since we already checked the QuorumRound was valid
			r.logger.Error("Error creating signed sequence from round", zap.Error(err))
			return
		}

		r.highestSequenceObserved = signedSeq
	}

	r.logger.Debug("Stored quorum round ", zap.Stringer("qr", &round))
	r.receivedQuorumRounds[round.GetRound()] = round
}

func (r *ReplicationState) GetFinalizedBlockForSequence(seq uint64) (Block, Finalization, bool) {
	for _, round := range r.receivedQuorumRounds {
		if round.GetSequence() == seq {
			if round.Block == nil || round.Finalization == nil {
				// this could be an empty notarization
				continue
			}
			return round.Block, *round.Finalization, true
		}
	}
	return nil, Finalization{}, false
}

func (r *ReplicationState) highestKnownRound() uint64 {
	var highestRound uint64
	for round := range r.receivedQuorumRounds {
		if round > highestRound {
			highestRound = round
		}
	}
	return highestRound
}



// FindReplicationTask returns a TimeoutTask assigned to [node] that contains the lowest sequence in [seqs].
// A sequence is considered "contained" if it falls between a task's Start (inclusive) and End (inclusive).
func FindReplicationTask(t *TimeoutHandler, node NodeID, seqs []uint64) *TimeoutTask {
	var lowestTask *TimeoutTask

	t.forEach(string(node), func(tt *TimeoutTask) {
		for _, seq := range seqs {
			if seq >= tt.Start && seq <= tt.End {
				if lowestTask == nil {
					lowestTask = tt
				} else if seq < lowestTask.Start {
					lowestTask = tt
				}
			}
		}
	})

	return lowestTask
}

// findMissingNumbersInRange finds numbers in an array constructed by [start...end] that are not in [nums]
// ex. (3, 10, [1,2,3,4,5,6]) -> [7,8,9,10]
func findMissingNumbersInRange(start, end uint64, nums []uint64) []uint64 {
	numMap := make(map[uint64]struct{})
	for _, num := range nums {
		numMap[num] = struct{}{}
	}

	var result []uint64

	for i := start; i <= end; i++ {
		if _, exists := numMap[i]; !exists {
			result = append(result, i)
		}
	}

	return result
}

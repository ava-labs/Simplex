package simplex

import (
	"sync"
	"time"
)

type roundReplicator struct {
	requestor      *requestor
	rounds         map[uint64]*QuorumRound
	digestTimeouts *TimeoutHandler[Digest]
	roundTimeouts  *TimeoutHandler[uint64]
}

func newRoundReplicator(log Logger, id NodeID, start time.Time, maxRoundWindow uint64, sender sender, lock *sync.Mutex) *roundReplicator {
	r := &roundReplicator{
		rounds:    make(map[uint64]*QuorumRound),
		requestor: newRequestor(log, start, lock, maxRoundWindow, sender, id, false),
	}
	r.digestTimeouts = NewTimeoutHandler(log, start, DefaultReplicationRequestTimeout, r.requestDigests, alwaysFalseRemover[Digest])
	r.roundTimeouts = NewTimeoutHandler(log, start, DefaultReplicationRequestTimeout, r.requestEmptyRounds, alwaysFalseRemover[uint64])

	return r
}

func (r *roundReplicator) storeQuorumRound(qr *QuorumRound) {
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

	// check if a round exists already. if exists we may need to merge the notarizations and empty notarizations
	r.rounds[qr.GetRound()] = qr

	r.requestor.receivedSignedQuorum(newSignedQuorum(qr, r.requestor.myNodeID))
}

func (r *roundReplicator) advanceTime(now time.Time) {
	r.requestor.advanceTime(now)
	r.digestTimeouts.Tick(now)
	r.roundTimeouts.Tick(now)
}

func (r *roundReplicator) getLowestRound() *QuorumRound {
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

func (r *roundReplicator) createDependencyTimeoutTask(blockDependency *Digest, missingRounds []uint64) {
	if blockDependency != nil {
		// same problem with orphaned blocks. need to make sure we have a way of deleting this task
		r.digestTimeouts.AddTask(*blockDependency)
	}

	for _, missingRound := range missingRounds {
		r.roundTimeouts.AddTask(missingRound)
	}

	// we need to ensure these tasks are cancelled when we receive a finalization or the digest
	// we need to make sure the dependency is executed once the digest comes.
	// aka: wait for digest, digest comes and the scheduler is notified.
	// TODO: on persist finalization/notarization & persist emptyNotarization we can delete these dependencies
}

func (r *roundReplicator) updateState(currentRoundOrNextSeq uint64) {
	r.requestor.updateState(currentRoundOrNextSeq)
}

func (r *roundReplicator) deleteRound(round uint64) {
	delete(r.rounds, round)
	r.roundTimeouts.RemoveTask(round)
}

func (r *roundReplicator) deleteOldRounds(finalizedRound uint64) {
	for round := range r.rounds {
		if round <= finalizedRound {
			delete(r.rounds, round)
		}
	}

	// we don't want to call RemoveOldTasks since we still may need empty rounds < finalized round
	r.roundTimeouts.RemoveTask(finalizedRound)
	r.requestor.removeOldTasks(finalizedRound)
}

func (r *roundReplicator) getBlockWithSeq(seq uint64) Block {
	if seq == 0 {
		return nil
	}

	for _, qr := range r.rounds {
		if qr.GetSequence() == seq {
			return qr.Block
		}
	}

	return nil
}

func (r *roundReplicator) requestDigests(digests []Digest) {}

func (r *roundReplicator) requestEmptyRounds(rounds []uint64) {}
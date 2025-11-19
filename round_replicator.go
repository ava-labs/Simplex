package simplex

type roundReplicator struct {
	rounds         map[uint64]*QuorumRound
	digestTimeouts *TimeoutHandler[Digest]
	roundTimeouts  *TimeoutHandler[uint64]
}

func (r *roundReplicator) storeQuorumRound(qr *QuorumRound) {
	r.rounds[qr.GetRound()] = qr
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
		r.digestTimeouts.AddTask(*blockDependency)
	}

	for _, missingRound := range missingRounds {
		r.roundTimeouts.AddTask(missingRound)
	}

	// we need to ensure these tasks are cancelled when we receive a finalization or the digest
	// we need to make sure the dependency is executed once the digest comes.
	// aka: wait for digest, digest comes and the scheudler is notified.
}

func (r *roundReplicator) deleteRound(round uint64) {
	delete(r.rounds, round)
}

func (r *roundReplicator) deleteOldRounds(finalizedRound uint64) {
	for round := range r.rounds {
		if round <= finalizedRound {
			delete(r.rounds, round)
		}
	}
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

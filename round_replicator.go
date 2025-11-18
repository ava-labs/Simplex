package simplex


type notarizedRound struct {
	block Block
	notarization *Notarization
	emptyNotarization *EmptyNotarization
}

type roundReplicator struct {
	rounds map[uint64]*QuorumRound
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
	// if we are here then we need to re-request quorum rounds for all the [missingRounds] and also send a timeout for [blockdependency]

	// we need to ensure these tasks are cancelled when we receive a finalization or the digest
	// we need to make sure the dependency is executed once the digest comes.
	// aka: wait for digest, digest comes and the scheudler is notified.
}

func (r *roundReplicator) deleteRound(round uint64) {
	delete(r.rounds, round)
}
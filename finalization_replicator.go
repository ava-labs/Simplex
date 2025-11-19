// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"
)

type finalizedQuorumRound struct {
	block        Block
	finalization *Finalization
}

// finalizationReplicator manages the state for replicating finalized sequences until highestObservedSeq.
type finalizationReplicator struct {
	requestor *requestor

	// receivedFinalizations maps either sequences or rounds to quorum rounds
	receivedFinalizations map[uint64]*finalizedQuorumRound
}

func newSeqReplicator(logger Logger, sender sender, ourNodeID NodeID, maxRoundWindow uint64, start time.Time, lock *sync.Mutex) *finalizationReplicator {
	return &finalizationReplicator{
		receivedFinalizations: make(map[uint64]*finalizedQuorumRound),
		requestor:             newRequestor(logger, start, lock, maxRoundWindow, sender, ourNodeID, true),
	}
}

func (r *finalizationReplicator) storeQuorumRound(round *QuorumRound) {
	if _, exists := r.receivedFinalizations[round.Finalization.Finalization.Seq]; exists {
		// we've already stored this round
		return
	}

	r.receivedFinalizations[round.Finalization.Finalization.Seq] = &finalizedQuorumRound{
		block:        round.Block,
		finalization: round.Finalization,
	}

	r.requestor.receivedSignedQuorum(newSignedQuorum(round, r.requestor.myNodeID))
}

func (r *finalizationReplicator) advanceTime(now time.Time) {
	r.requestor.advanceTime(now)
}

func (r *finalizationReplicator) getHighestRound() uint64 {
	return r.requestor.getHighestRound()
}

func (r *finalizationReplicator) updateState(currentRoundOrNextSeq uint64) {
	r.removeOldValues(currentRoundOrNextSeq)
	r.requestor.updateState(currentRoundOrNextSeq)
}

func (r *finalizationReplicator) removeOldValues(newValue uint64) {
	r.requestor.timeoutHandler.RemoveOldTasks(newValue)

	for storedRound := range r.receivedFinalizations {
		if storedRound < newValue {
			delete(r.receivedFinalizations, storedRound)
		}
	}
}

func (r *finalizationReplicator) retrieveBlockAndFinalization(seq uint64) (Block, *Finalization, bool) {
	qr, ok := r.receivedFinalizations[seq]
	if ok {
		return qr.block, qr.finalization, true
	}
	return nil, nil, false
}

func (r *finalizationReplicator) resendFinalizationRequest(seq uint64, signers []NodeID) error {
	numSigners := int64(len(signers))
	index, err := rand.Int(rand.Reader, big.NewInt(numSigners))
	if err != nil {
		return err
	}

	// because we are resending because the block failed to verify, we should remove the stored quorum round
	// so that we can try to get a new block & finalization
	delete(r.receivedFinalizations, seq)
	r.requestor.sendRequestToNode(seq, seq, signers[index.Int64()])
	return nil
}

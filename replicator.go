// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"
)

// signedRoundOrSeq is a round or sequence that has been signed by a quorum certificate.
type signedRoundOrSeq struct {
	round   uint64
	seq     uint64
	signers NodeIDs
	isRound bool
}

func newSignedRoundOrSeq(round QuorumRound, myNodeID NodeID) (*signedRoundOrSeq, error) {
	ss := &signedRoundOrSeq{}
	switch {
	case round.Finalization != nil:
		ss.signers = round.Finalization.QC.Signers()
		ss.round = round.GetRound()
		ss.seq = round.GetSequence()
		ss.isRound = false
	case round.Notarization != nil:
		ss.signers = round.Notarization.QC.Signers()
		ss.round = round.GetRound()
		ss.seq = round.GetSequence()
		ss.isRound = true
	case round.EmptyNotarization != nil:
		ss.signers = round.EmptyNotarization.QC.Signers()
		ss.round = round.GetRound()
		ss.seq = 0
		ss.isRound = true
	default:
		return nil, fmt.Errorf("round does not contain a finalization, empty notarization, or notarization")
	}

	// it's possible our node has signed this ss.
	// For example this may happen if our node has sent a finalized vote
	// for this round and has not received the
	// finalization from the network.
	ss.signers = ss.signers.Remove(myNodeID)
	return ss, nil
}

func newSignedRoundOrSeqFromFinalization(finalization *Finalization, myNodeID NodeID) *signedRoundOrSeq {
	return &signedRoundOrSeq{
		round:   finalization.Finalization.Round,
		seq:     finalization.Finalization.Seq,
		signers: NodeIDs(finalization.QC.Signers()).Remove(myNodeID),
		isRound: false,
	}
}

func newSignedRoundOrSeqFromRound(round uint64, signers NodeIDs, myNodeID NodeID) *signedRoundOrSeq {
	ss := &signedRoundOrSeq{
		round:   round,
		seq:     0, // seq not needed for round replicator
		signers: signers.Remove(myNodeID),
		isRound: true,
	}
	return ss
}

// roundOrSeq returns either the round or sequence depending on whether the replicator is
// replicating rounds or sequences.
func (s *signedRoundOrSeq) roundOrSeq() uint64 {
	if s.isRound {
		return s.round
	}
	return s.seq
}

type sender interface {
	// Send sends a message to the given destination node
	Send(msg *Message, destination NodeID)
}

// replicator manages the state for replicating sequences or rounds until highestObserved.
type replicator struct {
	sender         sender
	myNodeID       NodeID
	logger         Logger
	maxRoundWindow uint64
	epochLock      *sync.Mutex

	// highest sequence or round we have requested. Ensures we don't request the
	// same sequence multiple times, also allows us to limit the number of
	// outstanding requests to be at most [maxRoundWindow] ahead of highestRequested
	highestRequested uint64

	// highest we have received
	highestObserved *signedRoundOrSeq

	// receivedQuorumRounds maps either sequences or rounds to quorum rounds
	receivedQuorumRounds map[uint64]QuorumRound

	// request iterator
	requestIterator int

	timeoutHandler *TimeoutHandler
}

func newReplicator(logger Logger, sender sender, ourNodeID NodeID, maxRoundWindow uint64, start time.Time, lock *sync.Mutex) *replicator {
	r := &replicator{
		receivedQuorumRounds: make(map[uint64]QuorumRound),
		sender:               sender,
		myNodeID:             ourNodeID,
		logger:               logger,
		maxRoundWindow:       maxRoundWindow,
		epochLock:            lock,
	}

	r.timeoutHandler = NewTimeoutHandler(logger, start, DefaultReplicationRequestTimeout, r.resendReplicationRequests)
	return r
}

func (r *replicator) advanceTime(now time.Time) {
	r.timeoutHandler.Tick(now)
}

func (r *replicator) resendReplicationRequests(missingIds []uint64) {
	// we call this function in the timeout handler goroutine, so we need to
	// ensure we don't have concurrent access to highestObserved
	r.epochLock.Lock()
	defer r.epochLock.Unlock()

	nodes := r.highestObserved.signers
	numNodes := len(nodes)
	slices.Sort(missingIds)
	segments := CompressSequences(missingIds)
	for i, seqsOrRounds := range segments {
		index := (i + r.requestIterator) % numNodes
		r.sendRequestToNode(seqsOrRounds.Start, seqsOrRounds.End, nodes[index])
	}

	r.requestIterator++
}

// isReplicationComplete returns true if we have finished the replication process.
// The process is considered finished once highestObserved has caught up to the target
// (either nextSeqToCommit or currentRound).
func (r *replicator) isReplicationComplete(target uint64) bool {
	if r.highestObserved != nil && r.highestObserved.roundOrSeq() >= target {
		return false
	}
	return true
}

func (r *replicator) getHighestRound() uint64 {
	if r.highestObserved != nil {
		return r.highestObserved.round
	}
	return 0
}

// maybeSendMoreReplicationRequests checks if we need to send more replication requests given an observed round or sequence.
// it limits the amount of outstanding requests to be at most [maxRoundWindow] ahead of [currentRoundOrNextSequence] which is
// either nextSeqToCommit or currentRound depending on if we are replicating sequences or rounds.
func (r *replicator) maybeSendMoreReplicationRequests(observed *signedRoundOrSeq, currentRoundOrNextSequence uint64) {
	observedRoundOrSeq := observed.roundOrSeq()

	// we've observed something we've already requested
	if r.highestRequested >= observedRoundOrSeq && r.highestObserved != nil {
		r.logger.Debug("Already requested observed value, skipping", zap.Uint64("value", observedRoundOrSeq), zap.Bool("isRound", observed.isRound))
		return
	}

	// if this is the highest observed sequence or round, update our state
	if r.highestObserved == nil || observedRoundOrSeq > r.highestObserved.roundOrSeq() {
		r.highestObserved = observed
	}

	start := math.Max(float64(currentRoundOrNextSequence), float64(r.highestRequested))
	// we limit the number of outstanding requests to be at most maxRoundWindow ahead of nextSeqToCommit
	end := math.Min(float64(observedRoundOrSeq), float64(r.maxRoundWindow+currentRoundOrNextSequence))

	r.logger.Debug("Node is behind, attempting to request missing values", zap.Uint64("value", observedRoundOrSeq), zap.Uint64("start", uint64(start)), zap.Uint64("end", uint64(end)), zap.Bool("isRound", observed.isRound))
	r.sendReplicationRequests(uint64(start), uint64(end))
}

func (r *replicator) updateState(currentRoundOrNextSeq uint64) {
	r.removeOldValues(currentRoundOrNextSeq)

	// we send out more requests once our seq has caught up to 1/2 of the maxRoundWindow
	if currentRoundOrNextSeq+r.maxRoundWindow/2 > r.highestRequested && r.highestObserved != nil {
		r.maybeSendMoreReplicationRequests(r.highestObserved, currentRoundOrNextSeq)
	}
}

func (r *replicator) removeOldValues(newValue uint64) {
	r.timeoutHandler.RemoveOldTasks(newValue)

	for storedRound := range r.receivedQuorumRounds {
		if storedRound < newValue {
			delete(r.receivedQuorumRounds, storedRound)
		}
	}
}

// sendReplicationRequests sends requests for missing sequences for the
// range of sequences [start, end] <- inclusive. It does so by splitting the
// range of sequences equally amount the nodes that have signed [highestSequenceObserved].
func (r *replicator) sendReplicationRequests(start uint64, end uint64) {
	// it's possible our node has signed [highestSequenceObserved].
	// For example this may happen if our node has sent a finalization
	// for [highestSequenceObserved] and has not received the
	// finalization from the network.
	nodes := r.highestObserved.signers
	numNodes := len(nodes)

	seqRequests := DistributeSequenceRequests(start, end, numNodes)

	r.logger.Debug("Distributing replication requests", zap.Uint64("start", start), zap.Uint64("end", end), zap.Stringer("nodes", NodeIDs(nodes)))
	for i, seqsOrRounds := range seqRequests {
		index := (i + r.requestIterator) % numNodes
		r.sendRequestToNode(seqsOrRounds.Start, seqsOrRounds.End, r.highestObserved.signers[index])
	}

	// next time we send requests, we start with a different permutation
	r.requestIterator++
}

// sendRequestToNode requests the sequences [start, end] from nodes[index].
// In case the nodes[index] does not respond, we create a timeout that will
// re-send the request.
func (r *replicator) sendRequestToNode(start uint64, end uint64, node NodeID) {
	roundsOrSeqs := make([]uint64, (end+1)-start)
	for i := start; i <= end; i++ {
		roundsOrSeqs[i-start] = i
		// ensure we set a timeout for this sequence
		r.timeoutHandler.AddTask(i)
	}

	val := r.highestObserved.roundOrSeq()
	if r.highestRequested < end {
		r.highestRequested = end
	}

	request := &ReplicationRequest{}
	if r.highestObserved.isRound {
		request.LatestRound = val
		request.Rounds = roundsOrSeqs
	} else {
		request.LatestFinalizedSeq = val
		request.Seqs = roundsOrSeqs
	}

	msg := &Message{ReplicationRequest: request}

	r.logger.Debug("Requesting missing rounds/sequences ",
		zap.Stringer("from", node),
		zap.Uint64("start", start),
		zap.Uint64("end", end),
		zap.Bool("isRound", r.highestObserved.isRound),
		zap.Uint64("latestRound", request.LatestRound),
		zap.Uint64("latestSeq", request.LatestFinalizedSeq),
	)
	r.sender.Send(msg, node)
}

func (r *replicator) storeQuorumRound(round QuorumRound, from NodeID, roundOrSeq uint64) {
	// check if this is the highest round or seq we have seen
	if r.highestObserved == nil || roundOrSeq > r.highestObserved.roundOrSeq() {
		signedSeq, err := newSignedRoundOrSeq(round, r.myNodeID)
		if err != nil {
			// should never be here since we already checked the QuorumRound was valid
			r.logger.Error("Error creating signed sequence from round", zap.Error(err))
			return
		}

		r.highestObserved = signedSeq
	}

	if _, exists := r.receivedQuorumRounds[roundOrSeq]; exists {
		// we've already stored this round
		return
	}

	r.receivedQuorumRounds[roundOrSeq] = round

	// we received this sequence, remove the timeout task
	r.timeoutHandler.RemoveTask(roundOrSeq)
	r.logger.Debug("Stored quorum round ", zap.Stringer("qr", &round), zap.String("from", from.String()))
}

func (r *replicator) retrieveQuorumRound(key uint64) (*QuorumRound, bool) {
	qr, ok := r.receivedQuorumRounds[key]
	if ok {
		return &qr, true
	}
	return nil, false
}

func (r *replicator) retrieveQuorumRoundBySeq(seq uint64) (*QuorumRound, bool) {
	for _, qr := range r.receivedQuorumRounds {
		if qr.Block != nil && qr.Block.BlockHeader().Seq == seq {
			return &qr, true
		}
	}
	return nil, false
}

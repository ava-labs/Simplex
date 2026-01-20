// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"math"
	"sync"
	"time"

	"go.uber.org/zap"
)

// signedQuorum is a round that has been signed by a quorum certificate.
// if the round was empty notarized, seq is set to 0.
type signedQuorum struct {
	round   uint64
	seq     uint64
	signers NodeIDs
}

func newSignedQuorum(qr *QuorumRound, myNodeID NodeID) *signedQuorum {
	// it's possible our node has signed this quorum.
	// For example this may happen if our node has sent a finalized vote
	// for this round and has not received the
	// finalization from the network.
	switch {
	case qr.EmptyNotarization != nil:
		return &signedQuorum{
			signers: NodeIDs(qr.EmptyNotarization.QC.Signers()).Remove(myNodeID),
			round:   qr.EmptyNotarization.Vote.Round,
		}
	case qr.Finalization != nil:
		return &signedQuorum{
			signers: NodeIDs(qr.Finalization.QC.Signers()).Remove(myNodeID),
			round:   qr.Finalization.Finalization.Round,
			seq:     qr.Finalization.Finalization.Seq,
		}
	case qr.Notarization != nil:
		return &signedQuorum{
			signers: NodeIDs(qr.Notarization.QC.Signers()).Remove(myNodeID),
			round:   qr.Notarization.Vote.Round,
			seq:     qr.Notarization.Vote.Seq,
		}
	default:
		return nil
	}
}

func newSignedQuorumFromFinalization(finalization *Finalization, nodeID NodeID) *signedQuorum {
	return newSignedQuorum(&QuorumRound{
		Finalization: finalization,
	}, nodeID)
}

func newSignedQuorumFromRound(round, seq uint64, signers []NodeID, myNodeID NodeID) *signedQuorum {
	return &signedQuorum{
		round:   round,
		seq:     seq,
		signers: NodeIDs(signers).Remove(myNodeID),
	}
}

type sender interface {
	// Send sends a message to the given destination node
	Send(msg *Message, destination NodeID)
}

// requestor fetches quorum rounds up to [highestObserved] from the network,
// allowing up to [maxRoundWindow] concurrent requests to limit memory use.
// Ensures all rounds/sequences are eventually received.
type requestor struct {
	epochLock *sync.Mutex

	// highestSequenceRequested prevents duplicates and limits outstanding requests.
	highestRequested uint64

	// the requestor stops requesting once all sequences/rounds up to an including `highestObserved` have been received.
	highestObserved *signedQuorum

	// Handles timeouts and retries for missing sequences/rounds.
	timeoutHandler *TimeoutHandler[uint64]

	logger Logger

	// maxRoundWindow is the maximum number of requests we can request past highestRequested.
	maxRoundWindow uint64

	sender sender

	// requestIterator is an iterator over NodeIDs in order to request quorum rounds
	requestIterator int

	// replicateSeqs is set true if this requestor is for replicating sequences, and false if for rounds.
	replicateSeqs bool
}

func newRequestor(logger Logger, start time.Time, lock *sync.Mutex, maxRoundWindow uint64, sender sender, replicateSeqs bool) *requestor {
	r := &requestor{
		logger:         logger,
		epochLock:      lock,
		maxRoundWindow: maxRoundWindow,
		sender:         sender,
		replicateSeqs:  replicateSeqs,
	}
	name := "seq-timeout-handler"
	if !replicateSeqs {
		name = "round-timeout-handler"
	}
	r.timeoutHandler = NewTimeoutHandler(logger, name, start, DefaultReplicationRequestTimeout, r.resendReplicationRequests)
	return r
}

func (r *requestor) advanceTime(now time.Time) {
	r.timeoutHandler.Tick(now)
}

func (r *requestor) resendReplicationRequests(missingIds []uint64) {
	// we call this function in the timeout handler goroutine, so we need to
	// ensure we don't have concurrent access to highestObserved
	r.epochLock.Lock()
	defer r.epochLock.Unlock()

	segments := CompressSequences(missingIds)

	r.sendSegments(segments)

	r.requestIterator++
}

// observedSignedQuorum is called when we observe a signed quorum for a future round/sequence.
// we do not mix sequences and rounds because we have separate instances of requestor for each.
func (r *requestor) observedSignedQuorum(observed *signedQuorum, currentSeqOrRound uint64) {
	observedSeqOrRound := r.getSeqOrRound(observed)

	// we've observed something we've already requested
	if r.highestRequested >= observedSeqOrRound && r.highestObserved != nil {
		r.logger.Debug("Already requested observed value, skipping", zap.Uint64("value", observedSeqOrRound), zap.Bool("Seq Replication", r.replicateSeqs))
		return
	}

	// if this is the highest observed sequence, update our state
	if r.highestObserved == nil || observedSeqOrRound > r.highestObserved.seq {
		r.highestObserved = observed
	}

	r.sendMoreReplicationRequests(observedSeqOrRound, currentSeqOrRound)
}

// maybeSendMoreReplicationRequests checks if we need to send more replication requests given an observed quorum.
// it limits the amount of outstanding requests to be at most [maxRoundWindow] ahead of [currentSeqOrRound].
func (r *requestor) sendMoreReplicationRequests(observedSeqOrRound, currentSeqOrRound uint64) {
	start := math.Max(float64(currentSeqOrRound), float64(r.highestRequested))
	// we limit the number of outstanding requests to be at most maxRoundWindow ahead of nextSeqToCommit
	end := math.Min(float64(observedSeqOrRound), float64(r.maxRoundWindow+currentSeqOrRound))

	r.logger.Debug("Node is behind, attempting to request missing values", zap.Uint64("value", observedSeqOrRound), zap.Uint64("start", uint64(start)), zap.Uint64("end", uint64(end)), zap.Bool("seq requestor", r.replicateSeqs))
	r.sendReplicationRequests(uint64(start), uint64(end))
}

// sendReplicationRequests sends requests for missing sequences for the
// range of sequences [start, end] <- inclusive. It does so by splitting the
// range of sequences equally amount the nodes that have signed [highestObserved].
func (r *requestor) sendReplicationRequests(start uint64, end uint64) {
	nodes := r.highestObserved.signers
	numNodes := len(nodes)

	seqRequests := DistributeSequenceRequests(start, end, numNodes)
	r.logger.Debug("Distributing replication requests", zap.Uint64("start", start), zap.Uint64("end", end), zap.Stringer("nodes", NodeIDs(nodes)))

	r.sendSegments(seqRequests)

	// next time we send requests, we start with a different permutation
	r.requestIterator++
}

func (r *requestor) sendSegments(segments []Segment) {
	numNodes := len(r.highestObserved.signers)
	for i, seqsOrRounds := range segments {
		index := (i + r.requestIterator) % numNodes
		r.sendRequestToNode(seqsOrRounds.Start, seqsOrRounds.End, r.highestObserved.signers[index])
	}
}

// sendRequestToNode requests [start, end] from nodes[index].
// In case the nodes[index] does not respond, we create a timeout that will
// re-send the request.
func (r *requestor) sendRequestToNode(start uint64, end uint64, node NodeID) {
	seqsOrRound := make([]uint64, (end+1)-start)
	for i := start; i <= end; i++ {
		seqsOrRound[i-start] = i
		// ensure we set a timeout for this sequence
		r.timeoutHandler.AddTask(i)
	}

	if r.highestRequested < end {
		r.highestRequested = end
	}

	request := &ReplicationRequest{}
	if r.replicateSeqs {
		request.LatestFinalizedSeq = r.highestObserved.seq
		request.Seqs = seqsOrRound
	} else {
		request.LatestRound = r.highestObserved.round
		request.Rounds = seqsOrRound
	}

	msg := &Message{ReplicationRequest: request}

	r.logger.Debug("Requesting missing rounds/sequences ",
		zap.Stringer("from", node),
		zap.Uint64("start", start),
		zap.Uint64("end", end),
		zap.Int("len seqssss", len(request.Seqs)),
		zap.Int("len rounds", len(request.Rounds)),
		zap.Uint64("latestSeq", request.LatestFinalizedSeq),
		zap.Uint64("latestRound", request.LatestRound),
	)
	r.sender.Send(msg, node)
}

func (r *requestor) receivedSignedQuorum(signedQuorum *signedQuorum) {
	seqOrRound := r.getSeqOrRound(signedQuorum)

	// check if this is the highest round or seq we have seen
	if r.highestObserved == nil || seqOrRound > r.getSeqOrRound(r.highestObserved) {
		r.highestObserved = signedQuorum
	}

	// we received this sequence, remove the timeout task
	r.timeoutHandler.RemoveTask(seqOrRound)
	r.logger.Debug("Received future quorum round", zap.Uint64("seq or round", seqOrRound), zap.Bool("is finalization", r.replicateSeqs))
}

func (r *requestor) updateState(currentRoundOrNextSeq uint64) {
	// we send out more requests once our seq has caught up to 1/2 of the maxRoundWindow
	if currentRoundOrNextSeq+r.maxRoundWindow/2 > r.highestRequested && r.highestObserved != nil {
		r.observedSignedQuorum(r.highestObserved, currentRoundOrNextSeq)
	}
}

func (r *requestor) getHighestObserved() *signedQuorum {
	return r.highestObserved
}

func (r *requestor) getSeqOrRound(signedQuorum *signedQuorum) uint64 {
	if r.replicateSeqs {
		return signedQuorum.seq
	}

	return signedQuorum.round
}

// removes all tasks less or equal to the targetSeqOrRound
func (r *requestor) removeOldTasks(targetSeqOrRound uint64) {
	r.timeoutHandler.RemoveOldTasks(func(seqOrRound uint64, _ struct{}) bool {
		return seqOrRound <= targetSeqOrRound
	})
}

func (r *requestor) removeTask(seqOrRound uint64) {
	r.timeoutHandler.RemoveTask(seqOrRound)
}

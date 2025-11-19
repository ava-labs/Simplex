// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"crypto/rand"
	"math"
	"math/big"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"
)

type finalizedQuorumRound struct {
	block        Block
	finalization *Finalization
}

// signedSeq is a sequence that has been signed by a quorum certificate.
type signedSeq struct {
	round   uint64
	seq     uint64
	signers NodeIDs
}

func newSignedSeq(finalization *Finalization, myNodeID NodeID) *signedSeq {
	// it's possible our node has signed this ss.
	// For example this may happen if our node has sent a finalized vote
	// for this round and has not received the
	// finalization from the network.
	return &signedSeq{
		signers: NodeIDs(finalization.QC.Signers()).Remove(myNodeID),
		round:   finalization.Finalization.Round,
		seq:     finalization.Finalization.Seq,
	}
}

type sender interface {
	// Send sends a message to the given destination node
	Send(msg *Message, destination NodeID)
}

// finalizationReplicator manages the state for replicating finalized sequences until highestObservedSeq.
type finalizationReplicator struct {
	sender         sender
	myNodeID       NodeID
	logger         Logger
	maxRoundWindow uint64
	epochLock      *sync.Mutex

	// highest sequence we have requested. Ensures we don't request the
	// same sequence multiple times, also allows us to limit the number of
	// outstanding requests to be at most [maxRoundWindow] ahead of highestRequested
	highestRequested uint64

	// highest we have received
	highestObserved *signedSeq

	// receivedFinalizations maps either sequences or rounds to quorum rounds
	receivedFinalizations map[uint64]*finalizedQuorumRound

	// request iterator
	requestIterator int

	timeoutHandler *TimeoutHandler
}

func newReplicator(logger Logger, sender sender, ourNodeID NodeID, maxRoundWindow uint64, start time.Time, lock *sync.Mutex) *finalizationReplicator {
	r := &finalizationReplicator{
		receivedFinalizations: make(map[uint64]*finalizedQuorumRound),
		sender:                sender,
		myNodeID:              ourNodeID,
		logger:                logger,
		maxRoundWindow:        maxRoundWindow,
		epochLock:             lock,
	}

	r.timeoutHandler = NewTimeoutHandler(logger, start, DefaultReplicationRequestTimeout, r.resendReplicationRequests)
	return r
}

func (r *finalizationReplicator) advanceTime(now time.Time) {
	r.timeoutHandler.Tick(now)
}

func (r *finalizationReplicator) resendReplicationRequests(missingIds []uint64) {
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
// The process is considered finished once highestObserved has caught up to nextSeqToCommit
func (r *finalizationReplicator) isReplicationComplete(nextSeqToCommit uint64) bool {
	return r.highestObserved == nil || nextSeqToCommit > r.highestObserved.seq
}

func (r *finalizationReplicator) getHighestRound() uint64 {
	if r.highestObserved != nil {
		return r.highestObserved.round
	}
	return 0
}

// maybeSendMoreReplicationRequests checks if we need to send more replication requests given an observed sequence.
// it limits the amount of outstanding requests to be at most [maxRoundWindow] ahead of [currentSeq].
func (r *finalizationReplicator) maybeSendMoreReplicationRequests(observed *signedSeq, currentSeq uint64) {
	observedSeq := observed.seq

	// we've observed something we've already requested
	if r.highestRequested >= observedSeq && r.highestObserved != nil {
		r.logger.Debug("Already requested observed value, skipping", zap.Uint64("value", observedSeq))
		return
	}

	// if this is the highest observed sequence, update our state
	if r.highestObserved == nil || observedSeq > r.highestObserved.seq {
		r.highestObserved = observed
	}

	start := math.Max(float64(currentSeq), float64(r.highestRequested))
	// we limit the number of outstanding requests to be at most maxRoundWindow ahead of nextSeqToCommit
	end := math.Min(float64(observedSeq), float64(r.maxRoundWindow+currentSeq))

	r.logger.Debug("Node is behind, attempting to request missing values", zap.Uint64("value", observedSeq), zap.Uint64("start", uint64(start)), zap.Uint64("end", uint64(end)))
	r.sendReplicationRequests(uint64(start), uint64(end))
}

func (r *finalizationReplicator) updateState(currentRoundOrNextSeq uint64) {
	r.removeOldValues(currentRoundOrNextSeq)

	// we send out more requests once our seq has caught up to 1/2 of the maxRoundWindow
	if currentRoundOrNextSeq+r.maxRoundWindow/2 > r.highestRequested && r.highestObserved != nil {
		r.maybeSendMoreReplicationRequests(r.highestObserved, currentRoundOrNextSeq)
	}
}

func (r *finalizationReplicator) removeOldValues(newValue uint64) {
	r.timeoutHandler.RemoveOldTasks(newValue)

	for storedRound := range r.receivedFinalizations {
		if storedRound < newValue {
			delete(r.receivedFinalizations, storedRound)
		}
	}
}

// sendReplicationRequests sends requests for missing sequences for the
// range of sequences [start, end] <- inclusive. It does so by splitting the
// range of sequences equally amount the nodes that have signed [highestObserved].
func (r *finalizationReplicator) sendReplicationRequests(start uint64, end uint64) {
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
func (r *finalizationReplicator) sendRequestToNode(start uint64, end uint64, node NodeID) {
	seqs := make([]uint64, (end+1)-start)
	for i := start; i <= end; i++ {
		seqs[i-start] = i
		// ensure we set a timeout for this sequence
		r.timeoutHandler.AddTask(i)
	}

	if r.highestRequested < end {
		r.highestRequested = end
	}

	request := &ReplicationRequest{}
	request.LatestFinalizedSeq = r.highestObserved.seq
	request.Seqs = seqs

	msg := &Message{ReplicationRequest: request}

	r.logger.Debug("Requesting missing rounds/sequences ",
		zap.Stringer("from", node),
		zap.Uint64("start", start),
		zap.Uint64("end", end),
		zap.Uint64("latestSeq", request.LatestFinalizedSeq),
	)
	r.sender.Send(msg, node)
}

func (r *finalizationReplicator) storeFinalization(block Block, finalization *Finalization, from NodeID) {
	if _, exists := r.receivedFinalizations[finalization.Finalization.Seq]; exists {
		// we've already stored this round
		return
	}

	// check if this is the highest round or seq we have seen
	if r.highestObserved == nil || finalization.Finalization.Seq > r.highestObserved.seq {
		r.highestObserved = newSignedSeq(finalization, r.myNodeID)
	}

	r.receivedFinalizations[finalization.Finalization.Seq] = &finalizedQuorumRound{
		block:        block,
		finalization: finalization,
	}

	// we received this sequence, remove the timeout task
	r.timeoutHandler.RemoveTask(finalization.Finalization.Seq)
	r.logger.Debug("Stored future finalization ", zap.Uint64("finalization seq", finalization.Finalization.Seq), zap.String("from", from.String()))
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
	r.sendRequestToNode(seq, seq, signers[index.Int64()])
	return nil
}

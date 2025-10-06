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

// signedSequence is a sequence that has been signed by a quorum certificate.
// it essentially is a quorum round without the enforcement of needing a block with a
// finalization or notarization.
type signedSequence struct {
	seq     uint64
	signers NodeIDs
}

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
	lock           *sync.Mutex
	logger         Logger
	enabled        bool
	maxRoundWindow uint64
	comm           Communication
	id             NodeID

	// latest seq requested
	lastSequenceRequested uint64

	// highest sequence we have received
	highestSequenceObserved *signedSequence

	// receivedQuorumRounds maps rounds to quorum rounds
	receivedQuorumRounds map[uint64]QuorumRound

	// request iterator
	requestIterator int

	timeoutHandler *TimeoutHandler
}

func NewReplicationState(logger Logger, comm Communication, id NodeID, maxRoundWindow uint64, enabled bool, start time.Time, lock *sync.Mutex) *ReplicationState {
	return &ReplicationState{
		lock:                 lock,
		logger:               logger,
		enabled:              enabled,
		comm:                 comm,
		id:                   id,
		maxRoundWindow:       maxRoundWindow,
		receivedQuorumRounds: make(map[uint64]QuorumRound),
		timeoutHandler:       NewTimeoutHandler(logger, start, comm.Nodes()),
	}
}

func (r *ReplicationState) AdvanceTime(now time.Time) {
	r.timeoutHandler.Tick(now)
}

// isReplicationComplete returns true if we have finished the replication process.
// The process is considered finished once [currentRound] has caught up to the highest round received.
func (r *ReplicationState) isReplicationComplete(nextSeqToCommit uint64, currentRound uint64) bool {
	if r.highestSequenceObserved == nil {
		return true
	}

	return nextSeqToCommit > r.highestSequenceObserved.seq && currentRound > r.highestKnownRound()
}

func (r *ReplicationState) collectMissingSequences(observedSignedSeq *signedSequence, nextSeqToCommit uint64) {
	observedSeq := observedSignedSeq.seq
	// Node is behind, but we've already sent messages to collect future finalizations
	if r.lastSequenceRequested >= observedSeq && r.highestSequenceObserved != nil {
		return
	}

	if r.highestSequenceObserved == nil || observedSeq > r.highestSequenceObserved.seq {
		r.highestSequenceObserved = observedSignedSeq
	}

	startSeq := math.Max(float64(nextSeqToCommit), float64(r.lastSequenceRequested))
	// Don't exceed the max round window
	endSeq := math.Min(float64(observedSeq), float64(r.maxRoundWindow+nextSeqToCommit))

	r.logger.Debug("Node is behind, requesting missing finalizations", zap.Uint64("seq", observedSeq), zap.Uint64("startSeq", uint64(startSeq)), zap.Uint64("endSeq", uint64(endSeq)))
	r.sendReplicationRequests(uint64(startSeq), uint64(endSeq))
}

// sendReplicationRequests sends requests for missing sequences for the
// range of sequences [start, end] <- inclusive. It does so by splitting the
// range of sequences equally amount the nodes that have signed [highestSequenceObserved].
func (r *ReplicationState) sendReplicationRequests(start uint64, end uint64) {
	// it's possible our node has signed [highestSequenceObserved].
	// For example this may happen if our node has sent a finalization
	// for [highestSequenceObserved] and has not received the
	// finalization from the network.
	nodes := r.highestSequenceObserved.signers.Remove(r.id)
	numNodes := len(nodes)

	seqRequests := DistributeSequenceRequests(start, end, numNodes)

	r.logger.Debug("Distributing replication requests", zap.Uint64("start", start), zap.Uint64("end", end), zap.Stringer("nodes", NodeIDs(nodes)))
	for i, seqs := range seqRequests {
		index := (i + r.requestIterator) % numNodes
		r.sendRequestToNode(seqs.Start, seqs.End, nodes, index)
	}

	r.lastSequenceRequested = end
	// next time we send requests, we start with a different permutation
	r.requestIterator++
}

// sendRequestToNode requests the sequences [start, end] from nodes[index].
// In case the nodes[index] does not respond, we create a timeout that will
// re-send the request.
func (r *ReplicationState) sendRequestToNode(start uint64, end uint64, nodes []NodeID, index int) {
	r.logger.Debug("Requesting missing finalizations ",
		zap.Stringer("from", nodes[index]),
		zap.Uint64("start", start),
		zap.Uint64("end", end))
	seqs := make([]uint64, (end+1)-start)
	for i := start; i <= end; i++ {
		seqs[i-start] = i
	}
	request := &ReplicationRequest{
		Seqs:        seqs,
		LatestRound: r.highestSequenceObserved.seq,
	}
	msg := &Message{ReplicationRequest: request}

	task := r.createReplicationTimeoutTask(start, end, nodes, index)

	r.timeoutHandler.AddTask(task)

	r.comm.Send(msg, nodes[index])
}

func (r *ReplicationState) createReplicationTimeoutTask(start, end uint64, nodes []NodeID, index int) *TimeoutTask {
	taskFunc := func() {
		r.lock.Lock()
		defer r.lock.Unlock()
		r.sendRequestToNode(start, end, nodes, (index+1)%len(nodes))
	}
	timeoutTask := &TimeoutTask{
		Start:    start,
		End:      end,
		NodeID:   nodes[index],
		TaskID:   getTimeoutID(start, end),
		Task:     taskFunc,
		Deadline: r.timeoutHandler.GetTime().Add(DefaultReplicationRequestTimeout),
	}

	return timeoutTask
}

// receivedReplicationResponse notifies the task handler a response was received. If the response
// was incomplete(meaning our timeout expected more seqs), then we will create a new timeout
// for the missing sequences and send the request to a different node.
func (r *ReplicationState) receivedReplicationResponse(data []QuorumRound, node NodeID) {
	seqs := make([]uint64, 0, len(data))

	// remove all sequences where we expect a finalization but only received a notarization
	highestSeq := r.highestSequenceObserved.seq
	for _, qr := range data {
		if qr.GetSequence() <= highestSeq && qr.Finalization == nil && qr.Notarization != nil {
			r.logger.Debug("Received notarization without finalization, skipping", zap.Stringer("from", node), zap.Uint64("seq", qr.GetSequence()))
			continue
		}

		seqs = append(seqs, qr.GetSequence())
	}

	slices.Sort(seqs)

	task := FindReplicationTask(r.timeoutHandler, node, seqs)
	if task == nil {
		r.logger.Debug("Could not find a timeout task associated with the replication response", zap.Stringer("from", node), zap.Any("seqs", seqs))
		return
	}
	r.timeoutHandler.RemoveTask(node, task.TaskID)

	// we found the timeout, now make sure all seqs were returned
	missing := findMissingNumbersInRange(task.Start, task.End, seqs)
	if len(missing) == 0 {
		return
	}

	// if not all sequences were returned, create new timeouts
	r.logger.Debug("Received missing sequences in the replication response", zap.Stringer("from", node), zap.Any("missing", missing))
	nodes := r.highestSequenceObserved.signers.Remove(r.id)
	numNodes := len(nodes)
	segments := CompressSequences(missing)
	for i, seqs := range segments {
		index := i % numNodes
		newTask := r.createReplicationTimeoutTask(seqs.Start, seqs.End, nodes, index)
		r.timeoutHandler.AddTask(newTask)
	}
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

func (r *ReplicationState) replicateBlocks(finalization *Finalization, nextSeqToCommit uint64) {
	if !r.enabled {
		return
	}

	signedSequence := &signedSequence{
		seq:     finalization.Finalization.Seq,
		signers: finalization.QC.Signers(),
	}

	r.collectMissingSequences(signedSequence, nextSeqToCommit)
}

// maybeCollectFutureSequences attempts to collect future sequences if
// there are more to be collected and the round has caught up for us to send the request.
func (r *ReplicationState) maybeCollectFutureSequences(nextSequenceToCommit uint64) {
	if !r.enabled {
		return
	}

	if r.lastSequenceRequested >= r.highestSequenceObserved.seq {
		return
	}

	// we send out more requests once our seq has caught up to 1/2 of the maxRoundWindow
	if nextSequenceToCommit+r.maxRoundWindow/2 > r.lastSequenceRequested {
		r.collectMissingSequences(r.highestSequenceObserved, nextSequenceToCommit)
	}
}

func (r *ReplicationState) StoreQuorumRound(round QuorumRound) {
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

func (r *ReplicationState) GetQuorumRoundWithSeq(seq uint64) *QuorumRound {
	for _, round := range r.receivedQuorumRounds {
		if round.GetSequence() == seq {
			return &round
		}
	}
	return nil
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

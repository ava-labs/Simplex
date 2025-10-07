package simplex

import (
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

// sequenceReplicator manages the state for replicating sequences up until highestSequenceObserved.
type sequenceReplicator struct {
	sender Sender
	myNodeID NodeID
	logger Logger
	maxRoundWindow uint64
	// highest seq we have requested. must be <= highestSequenceObserved
	// this is used to limit the number of outstanding requests we have and ensure 
	// we don't request the same sequence multiple times. Ex. if the highestSequenceObserved is 100 ahead of us, we will
	// request in batches of 10 and wait for those to be fulfilled before requesting more and updating lastSequenceRequested.
	lastSequenceRequested uint64

	// highest sequence we have received
	highestSequenceObserved *signedSequence
	// we lock since we use highestSequenceObserved in multiple goroutines(via timeout tasks)
	highestSequenceObservedLock sync.Mutex

	// TODO: these should be sequences not rounds.
	// receivedQuorumRounds maps rounds to quorum rounds
	receivedQuorumRounds map[uint64]QuorumRound

	// request iterator
	requestIterator int

	timeoutHandler *TimeoutHandler
}

func newSequenceReplicator(logger Logger, comm Communication, ourNodeID NodeID, maxRoundWindow uint64, start time.Time) *sequenceReplicator {
	return &sequenceReplicator{
		receivedQuorumRounds: make(map[uint64]QuorumRound),
		sender:               comm,
		myNodeID:            ourNodeID,
		logger:               logger,
		maxRoundWindow:       maxRoundWindow,
		timeoutHandler:      NewTimeoutHandler(logger, start, comm.Nodes()),
	}
}


func (s *sequenceReplicator) advanceTime(now time.Time) {
	s.timeoutHandler.Tick(now)
}

// we have finished replicating sequences if the `nextSeqToCommit` is greater than the highestSequenceObserved
func (s *sequenceReplicator) isReplicationComplete(nextSeqToCommit uint64) bool {
	if s.highestSequenceObserved == nil {
		return false
	}

	return nextSeqToCommit > s.highestSequenceObserved.seq
}

func (s *sequenceReplicator) receivedFutureSequence(finalization *Finalization, nextSeqToCommit uint64) {
	signedSequence := &signedSequence{
		seq:     finalization.Finalization.Seq,
		signers: finalization.QC.Signers(),
	}

	s.maybeRequestMoreSequences(signedSequence, nextSeqToCommit)
}

func (s *sequenceReplicator) maybeRequestMoreSequences(observedSignedSeq *signedSequence, nextSeqToCommit uint64) {
	observedSeq := observedSignedSeq.seq

	// we've observed a sequence we've already requested
	if observedSeq < s.lastSequenceRequested && s.highestSequenceObserved != nil {
		return
	}

	// we've already requested up to the highest sequence observed
	if s.highestSequenceObserved != nil && s.lastSequenceRequested >= s.highestSequenceObserved.seq {
		return
	}

	if s.highestSequenceObserved == nil || observedSeq > s.highestSequenceObserved.seq {
		// we have observed a higher sequence than before
		s.highestSequenceObservedLock.Lock()
		s.highestSequenceObserved = observedSignedSeq
		s.highestSequenceObservedLock.Unlock()
	}

	startSeq := math.Max(float64(nextSeqToCommit), float64(s.lastSequenceRequested))
	// we limit the number of outstanding requests to be at most maxRoundWindow ahead of nextSeqToCommit
	endSeq := math.Min(float64(observedSeq), float64(s.maxRoundWindow+nextSeqToCommit))

	s.logger.Debug("Node is behind, requesting missing finalizations", zap.Uint64("seq", observedSeq), zap.Uint64("startSeq", uint64(startSeq)), zap.Uint64("endSeq", uint64(endSeq)))
	s.sendReplicationRequests(uint64(startSeq), uint64(endSeq))
}

func (s *sequenceReplicator) maybeAdvancedState(nextSeqToCommit uint64) {
	// we send out more requests once our seq has caught up to 1/2 of the maxRoundWindow
	if nextSeqToCommit+s.maxRoundWindow/2 > s.lastSequenceRequested {
		s.maybeRequestMoreSequences(s.highestSequenceObserved, nextSeqToCommit)
	}
}

// sendReplicationRequests sends requests for missing sequences for the
// range of sequences [start, end] <- inclusive. It does so by splitting the
// range of sequences equally amount the nodes that have signed [highestSequenceObserved].
func (s *sequenceReplicator) sendReplicationRequests(start uint64, end uint64) {
	// it's possible our node has signed [highestSequenceObserved].
	// For example this may happen if our node has sent a finalization
	// for [highestSequenceObserved] and has not received the
	// finalization from the network.
	nodes := s.highestSequenceObserved.signers.Remove(s.myNodeID)
	numNodes := len(nodes)

	seqRequests := DistributeSequenceRequests(start, end, numNodes)

	s.logger.Debug("Distributing replication requests", zap.Uint64("start", start), zap.Uint64("end", end), zap.Stringer("nodes", NodeIDs(nodes)))
	for i, seqs := range seqRequests {
		index := (i + s.requestIterator) % numNodes
		s.sendRequestToNode(seqs.Start, seqs.End, nodes, index)
	}

	s.lastSequenceRequested = end
	// next time we send requests, we start with a different permutation
	s.requestIterator++
}

// sendRequestToNode requests the sequences [start, end] from nodes[index].
// In case the nodes[index] does not respond, we create a timeout that will
// re-send the request.
func (s *sequenceReplicator) sendRequestToNode(start uint64, end uint64, nodes []NodeID, index int) {
	s.logger.Debug("Requesting missing finalizations ",
		zap.Stringer("from", nodes[index]),
		zap.Uint64("start", start),
		zap.Uint64("end", end))
	seqs := make([]uint64, (end+1)-start)
	for i := start; i <= end; i++ {
		seqs[i-start] = i
	}
	
	s.highestSequenceObservedLock.Lock()
	request := &ReplicationRequest{
		Seqs:        seqs,
		LatestRound: s.highestSequenceObserved.seq,
	}
	s.highestSequenceObservedLock.Unlock()

	msg := &Message{ReplicationRequest: request}

	task := s.createReplicationTimeoutTask(start, end, nodes, index)

	s.timeoutHandler.AddTask(task)

	s.sender.Send(msg, nodes[index])
}

func (s *sequenceReplicator) createReplicationTimeoutTask(start, end uint64, nodes []NodeID, index int) *TimeoutTask {
	taskFunc := func() {
		s.sendRequestToNode(start, end, nodes, (index+1)%len(nodes))
	}
	timeoutTask := &TimeoutTask{
		Start:    start,
		End:      end,
		NodeID:   nodes[index],
		TaskID:   getTimeoutID(start, end),
		Task:     taskFunc,
		Deadline: s.timeoutHandler.GetTime().Add(DefaultReplicationRequestTimeout),
	}

	return timeoutTask
}

func (s *sequenceReplicator) getQuorumRoundWithSeq(seq uint64) *QuorumRound {
	for _, round := range s.receivedQuorumRounds {
		if round.GetSequence() == seq {
			return &round
		}
	}
	return nil
}

// TODO: don't make this associated with the node that sent the response, simply make it node agnostic
func (s *sequenceReplicator) receivedFinalizationSeqs(seqs []uint64, node NodeID) {
	slices.Sort(seqs)

	// TODO: here we find them by nodes. make this node agnostics
	task := FindReplicationTask(s.timeoutHandler, node, seqs)
	if task == nil {
		s.logger.Debug("Could not find a timeout task associated with the replication response", zap.Stringer("from", node), zap.Any("seqs", seqs))
		return
	}
	s.timeoutHandler.RemoveTask(node, task.TaskID)

	// we found the timeout, now make sure all seqs were returned
	missing := findMissingNumbersInRange(task.Start, task.End, seqs)
	if len(missing) == 0 {
		return
	}

	// if not all sequences were returned, create new timeouts
	s.logger.Debug("Received missing sequences in the replication response", zap.Stringer("from", node), zap.Any("missing", missing))
	nodes := s.highestSequenceObserved.signers.Remove(s.myNodeID)
	numNodes := len(nodes)
	segments := CompressSequences(missing)
	for i, seqs := range segments {
		index := i % numNodes
		newTask := s.createReplicationTimeoutTask(seqs.Start, seqs.End, nodes, index)
		s.timeoutHandler.AddTask(newTask)
	}
}

func (s *sequenceReplicator) storeQuorumRound(round QuorumRound) {
	s.receivedQuorumRounds[round.GetSequence()] = round
}
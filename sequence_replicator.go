package simplex

// import (
// 	"fmt"
// 	"math"
// 	"sync"
// 	"time"

// 	"go.uber.org/zap"
// )

// // sequenceReplicator manages the state for replicating sequences up until highestSequenceObserved.
// type sequenceReplicator struct {
// 	sender         Sender
// 	myNodeID       NodeID
// 	logger         Logger
// 	maxRoundWindow uint64
// 	// highest seq we have requested. must be <= highestSequenceObserved
// 	// this is used to limit the number of outstanding requests we have and ensure
// 	// we don't request the same sequence multiple times. Ex. if the highestSequenceObserved is 100 ahead of us, we will
// 	// request in batches of 10 and wait for those to be fulfilled before requesting more and updating highestSequenceRequested.
// 	highestSequenceRequested uint64

// 	// highest sequence we have received
// 	highestSequenceObserved *signedSequence
// 	// we lock since we use highestSequenceObserved in multiple goroutines(via timeout tasks)
// 	highestSequenceObservedLock sync.Mutex

// 	// TODO: these should be sequences not rounds.
// 	// receivedQuorumRounds maps rounds to quorum rounds
// 	receivedQuorumRounds map[uint64]QuorumRound

// 	// request iterator
// 	requestIterator int

// 	timeoutHandler *TimeoutHandler
// }

// // func newSequenceReplicator(logger Logger, sender Sender, ourNodeID NodeID, maxRoundWindow uint64, start time.Time) *sequenceReplicator {
// // 	s := &sequenceReplicator{
// // 		receivedQuorumRounds: make(map[uint64]QuorumRound),
// // 		sender:               sender,
// // 		myNodeID:             ourNodeID,
// // 		logger:               logger,
// // 		maxRoundWindow:       maxRoundWindow,
// // 	}

// // 	s.timeoutHandler = NewTimeoutHandler(logger, start, DefaultReplicationRequestTimeout, s.resendReplicationRequests)
// // 	return s
// // }

// // func (s *sequenceReplicator) advanceTime(now time.Time) {
// // 	s.timeoutHandler.Tick(now)
// // }

// // // we have finished replicating sequences if the `nextSeqToCommit` is greater than the highestSequenceObserved
// // func (s *sequenceReplicator) isReplicationComplete(nextSeqToCommit uint64) bool {
// // 	if s.highestSequenceObserved == nil {
// // 		return true // TODO: make sure this should be true or false, seems like if its nil so vacously we must be done
// // 	}

// // 	return nextSeqToCommit > s.highestSequenceObserved.seq
// // }

// // func (s *sequenceReplicator) receivedFutureSequence(finalization *Finalization, nextSeqToCommit uint64) {
// // 	signedSequence := &signedSequence{
// // 		seq:     finalization.Finalization.Seq,
// // 		signers: finalization.QC.Signers(),
// // 		round:   finalization.Finalization.Round,
// // 	}

// // 	s.maybeRequestMoreSequences(signedSequence, nextSeqToCommit)
// // }

// // func (s *sequenceReplicator) maybeRequestMoreSequences(observedSignedSeq *signedSequence, nextSeqToCommit uint64) {
// // 	observedSeq := observedSignedSeq.seq

// // 	// we've observed a sequence we've already requested
// // 	if observedSeq < s.highestSequenceRequested && s.highestSequenceObserved != nil {
// // 		return
// // 	}

// // 	// we've already requested up to the highest sequence observed
// // 	if s.highestSequenceObserved != nil && s.highestSequenceRequested >= s.highestSequenceObserved.seq {
// // 		return
// // 	}

// // 	if s.highestSequenceObserved == nil || observedSeq > s.highestSequenceObserved.seq {
// // 		// we have observed a higher sequence than before
// // 		s.highestSequenceObservedLock.Lock()
// // 		s.highestSequenceObserved = observedSignedSeq
// // 		s.highestSequenceObservedLock.Unlock()
// // 	}

// // 	startSeq := math.Max(float64(nextSeqToCommit), float64(s.highestSequenceRequested))
// // 	// we limit the number of outstanding requests to be at most maxRoundWindow ahead of nextSeqToCommit
// // 	endSeq := math.Min(float64(observedSeq), float64(s.maxRoundWindow+nextSeqToCommit))

// // 	s.logger.Debug("Node is behind, requesting missing finalizations", zap.Uint64("seq", observedSeq), zap.Uint64("startSeq", uint64(startSeq)), zap.Uint64("endSeq", uint64(endSeq)))
// // 	s.sendReplicationRequests(uint64(startSeq), uint64(endSeq))
// // }

// // func (s *sequenceReplicator) maybeAdvancedState(nextSeqToCommit uint64) {
// // 	s.timeoutHandler.RemoveOldTasks(nextSeqToCommit)

// // 	// we send out more requests once our seq has caught up to 1/2 of the maxRoundWindow
// // 	if nextSeqToCommit+s.maxRoundWindow/2 > s.highestSequenceRequested {
// // 		s.maybeRequestMoreSequences(s.highestSequenceObserved, nextSeqToCommit)
// // 	}
// // }

// // sendReplicationRequests sends requests for missing sequences for the
// // range of sequences [start, end] <- inclusive. It does so by splitting the
// // range of sequences equally amount the nodes that have signed [highestSequenceObserved].
// func (s *sequenceReplicator) sendReplicationRequests(start uint64, end uint64) {
// 	// it's possible our node has signed [highestSequenceObserved].
// 	// For example this may happen if our node has sent a finalization
// 	// for [highestSequenceObserved] and has not received the
// 	// finalization from the network.
// 	nodes := s.highestSequenceObserved.signers.Remove(s.myNodeID)
// 	numNodes := len(nodes)

// 	seqRequests := DistributeSequenceRequests(start, end, numNodes)

// 	s.logger.Debug("Distributing replication requests", zap.Uint64("start", start), zap.Uint64("end", end), zap.Stringer("nodes", NodeIDs(nodes)))
// 	for i, seqs := range seqRequests {
// 		index := (i + s.requestIterator) % numNodes
// 		s.sendRequestToNode(seqs.Start, seqs.End, nodes, index)
// 	}

// 	// next time we send requests, we start with a different permutation
// 	s.requestIterator++
// }

// // sendRequestToNode requests the sequences [start, end] from nodes[index].
// // In case the nodes[index] does not respond, we create a timeout that will
// // re-send the request.
// func (s *sequenceReplicator) sendRequestToNode(start uint64, end uint64, nodes []NodeID, index int) {
// 	s.logger.Debug("Requesting missing finalizations ",
// 		zap.Stringer("from", nodes[index]),
// 		zap.Uint64("start", start),
// 		zap.Uint64("end", end))
// 	seqs := make([]uint64, (end+1)-start)
// 	for i := start; i <= end; i++ {
// 		seqs[i-start] = i
// 		// ensure we set a timeout for this sequence
// 		s.timeoutHandler.AddTask(i)
// 	}

// 	s.highestSequenceObservedLock.Lock()
// 	request := &ReplicationRequest{
// 		Seqs:        seqs,
// 		LatestSeq: s.highestSequenceObserved.seq,
// 	}
// 	s.highestSequenceObservedLock.Unlock()

// 	msg := &Message{ReplicationRequest: request}

// 	s.sender.Send(msg, nodes[index])

// 	if s.highestSequenceRequested < end {
// 		s.highestSequenceRequested = end
// 	}
// }

// func (s *sequenceReplicator) retrieveFinalizedBlock(seq uint64) (Block, Finalization, bool) {
// 	qr, ok := s.receivedQuorumRounds[seq]
// 	// if we have a quorum round, it should have a finalization and block but we check anyway
// 	if ok && qr.Finalization != nil && qr.Block != nil {
// 		return qr.Block, *qr.Finalization, true
// 	}

// 	return nil, Finalization{}, false
// }

// func (s *sequenceReplicator) storeQuorumRound(round QuorumRound, from NodeID) {
// 	// check if this seq is the highest we have seen
// 	seq := round.GetSequence()
// 	if seq > s.highestSequenceObserved.seq {
// 		signedSeq, err := newSignedSequenceFromRound(round)
// 		if err != nil {
// 			// should never be here since we already checked the QuorumRound was valid
// 			s.logger.Error("Error creating signed sequence from round", zap.Error(err))
// 			return
// 		}

// 		s.highestSequenceObserved = signedSeq
// 	}

// 	s.receivedQuorumRounds[seq] = round

// 	// we received this sequence, remove the timeout task
// 	s.timeoutHandler.RemoveTask(seq)
// 	s.logger.Debug("Stored quorum round ", zap.Stringer("qr", &round), zap.String("from", from.String()))
// }

// func newSignedSequenceFromRound(round QuorumRound) (*signedSequence, error) {
// 	ss := &signedSequence{}
// 	switch {
// 	case round.Finalization != nil:
// 		ss.signers = round.Finalization.QC.Signers()
// 		ss.seq = round.Finalization.Finalization.Seq
// 		ss.round = round.Finalization.Finalization.Round
// 	case round.Notarization != nil:
// 		ss.signers = round.Notarization.QC.Signers()
// 		ss.seq = round.Notarization.Vote.Seq
// 		ss.round = round.Notarization.Vote.Round
// 	case round.EmptyNotarization != nil:
// 		return nil, fmt.Errorf("should not create signed sequence from empty notarization")
// 	default:
// 		return nil, fmt.Errorf("round does not contain a finalization, empty notarization, or notarization")
// 	}

// 	return ss, nil
// }

// func (s *sequenceReplicator) resendReplicationRequests(missingSeqs []uint64) {
// 	nodes := s.highestSequenceObserved.signers.Remove(s.myNodeID)
// 	numNodes := len(nodes)
// 	segments := CompressSequences(missingSeqs)
// 	for i, seqs := range segments {
// 		index := i % numNodes
// 		s.sendRequestToNode(seqs.Start, seqs.End, nodes, (index+1)%len(nodes))
// 	}
// }

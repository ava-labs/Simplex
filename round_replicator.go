package simplex

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

type roundReplicator struct {
	sender         Sender
	myNodeID       NodeID
	logger         Logger
	maxRoundWindow uint64
	timeoutHandler *TimeoutHandler

	rounds map[uint64]QuorumRound

	highestRoundRequested uint64
	// highest Round we have received
	highestRoundObserved *signedSequence
	// we lock since we use highestRoundObserved in multiple goroutines(via timeout tasks)
	highestRoundObservedLock sync.Mutex
}

func newRoundReplicator(logger Logger, comm Communication, ourNodeID NodeID, maxRoundWindow uint64, start time.Time) *roundReplicator {
	r := &roundReplicator{
		sender:         comm,
		myNodeID:       ourNodeID,
		logger:         logger,
		maxRoundWindow: maxRoundWindow,
		rounds:         make(map[uint64]QuorumRound),
	}

	r.timeoutHandler = NewTimeoutHandler(logger, start, DefaultReplicationRequestTimeout, r.resendReplicationRequests)
	return r
}

func (r *roundReplicator) advanceTime(now time.Time) {
	r.timeoutHandler.Tick(now)
}

func (r *roundReplicator) isReplicationComplete(currentRound uint64) bool {
	if r.highestRoundObserved == nil {
		return true // TODO: make sure this should be true or false, seems like if its nil so vacously we must be done
	}

	return currentRound > r.highestRoundObserved.round
}

func (r *roundReplicator) storeQuorumRound(qr QuorumRound, from NodeID) {
	round := qr.GetRound()
	if _, exists := r.rounds[round]; exists {
		// we've already stored this round
		// TODO: add a test where we receive a notarization first from replication, but the chain actually had an empty notarization
		return
	}

	r.rounds[round] = qr
	r.logger.Debug("Stored quorum round from replication", zap.Uint64("round", round), zap.String("from", from.String()), zap.String("node", r.myNodeID.String()))
}

func (r *roundReplicator) receivedFutureRound(round uint64) {
	r.timeoutHandler.RemoveOldTasks(round)

	for storedRound := range r.rounds {
		if storedRound < round {
			delete(r.rounds, storedRound)
		}
	}
}

func (r *roundReplicator) resendReplicationRequests(missingSeqs []uint64) {
	nodes := r.highestRoundObserved.signers.Remove(r.myNodeID)
	numNodes := len(nodes)
	segments := CompressSequences(missingSeqs)
	for i, seqs := range segments {
		index := i % numNodes
		r.sendRequestToNode(seqs.Start, seqs.End, nodes, (index+1)%len(nodes))
	}
}

// sendRequestToNode requests the sequences [start, end] from nodes[index].
// In case the nodes[index] does not respond, we create a timeout that will
// re-send the request.
func (r *roundReplicator) sendRequestToNode(start uint64, end uint64, nodes []NodeID, index int) {
	r.logger.Debug("Requesting missing notarized rounds ",
		zap.Stringer("from", nodes[index]),
		zap.Uint64("start", start),
		zap.Uint64("end", end))
	seqs := make([]uint64, (end+1)-start)
	for i := start; i <= end; i++ {
		seqs[i-start] = i
		// ensure we set a timeout for this sequence
		r.timeoutHandler.AddTask(i)
	}

	r.highestRoundObservedLock.Lock()
	request := &ReplicationRequest{
		Seqs:        seqs,
		LatestSeq: r.highestRoundObserved.seq,
	}
	r.highestRoundObservedLock.Unlock()

	msg := &Message{ReplicationRequest: request}

	r.sender.Send(msg, nodes[index])

	if r.highestRoundRequested < end {
		r.highestRoundRequested = end
	}
}

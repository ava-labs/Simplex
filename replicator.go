package simplex

import (
	"fmt"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"
)

// signedSequence is a sequence that has been signed by a quorum certificate.
// it essentially is a quorum round without the enforcement of needing a block with a
// finalization or notarization.
type signerRoundOrSeq struct {
	round   uint64
	seq     uint64
	signers NodeIDs
	isRound bool
}

func newSignedRoundOrSeq(round QuorumRound) (*signerRoundOrSeq, error) {
	ss := &signerRoundOrSeq{}
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

	return ss, nil
}

// this is the key in the map of stored quorum rounds
func (s *signerRoundOrSeq) value() uint64 {
	if s.isRound {
		return s.round
	}
	return s.seq
}

// replicator manages the state for replicating sequences up until highestSequenceObserved.
type replicator struct {
	sender         Sender
	myNodeID       NodeID
	logger         Logger
	maxRoundWindow uint64

	// highest seq we have requested. must be <= highestSequenceObserved
	// this is used to limit the number of outstanding requests we have and ensure
	// we don't request the same sequence multiple times. Ex. if the highestSequenceObserved is 100 ahead of us, we will
	// request in batches of 10 and wait for those to be fulfilled before requesting more and updating highestSequenceRequested.
	highestRequested uint64

	// highest sequence we have received
	highestObserved *signerRoundOrSeq
	// we lock since we use highestObserved in multiple goroutines(via timeout tasks)
	highestObservedLock sync.Mutex

	// receivedQuorumRounds maps either sequences or rounds to quorum rounds
	receivedQuorumRounds map[uint64]QuorumRound

	// request iterator
	requestIterator int

	timeoutHandler *TimeoutHandler
}

func newReplicator(logger Logger, sender Sender, ourNodeID NodeID, maxRoundWindow uint64, start time.Time) *replicator {
	r := &replicator{
		receivedQuorumRounds: make(map[uint64]QuorumRound),
		sender:               sender,
		myNodeID:             ourNodeID,
		logger:               logger,
		maxRoundWindow:       maxRoundWindow,
	}

	r.timeoutHandler = NewTimeoutHandler(logger, start, DefaultReplicationRequestTimeout, r.resendReplicationRequests)
	return r
}

func (r *replicator) advanceTime(now time.Time) {
	r.timeoutHandler.Tick(now)
}

func (r *replicator) resendReplicationRequests(missingIds []uint64) {
	nodes := r.highestObserved.signers.Remove(r.myNodeID)
	numNodes := len(nodes)
	segments := CompressSequences(missingIds)
	for i, seqs := range segments {
		index := i % numNodes
		r.sendRequestToNode(seqs.Start, seqs.End, nodes, (index+1)%len(nodes))
	}
}

func (r *replicator) getHighestObserved() *signerRoundOrSeq {
	return r.highestObserved
}

// minValue could be nextSeqToCOmmite or currentRound
func (r *replicator) receivedFutureValue(ss *signerRoundOrSeq, minValue uint64) {
	r.maybeSendMoreReplicationRequests(ss, minValue)
}

// either nextSeqToCommit or currentRound depending on if we are replicating sequences or rounds
func (r *replicator) maybeSendMoreReplicationRequests(observed *signerRoundOrSeq, minValue uint64) {
	val := observed.value()

	// we've observed a sequence we've already requested
	if val < r.highestRequested && r.highestObserved != nil {
		return
	}

	// we've already requested up to the highest sequence observed
	if r.highestObserved != nil && r.highestRequested >= r.highestObserved.value() {
		return
	}

	if r.highestObserved == nil || val > r.highestObserved.value() {
		// we have observed a higher sequence than before
		r.highestObservedLock.Lock()
		r.highestObserved = observed
		r.highestObservedLock.Unlock()
	}

	startSeq := math.Max(float64(minValue), float64(r.highestRequested))
	// we limit the number of outstanding requests to be at most maxRoundWindow ahead of nextSeqToCommit
	endSeq := math.Min(float64(val), float64(r.maxRoundWindow+minValue))

	r.logger.Debug("Node is behind, requesting missing values", zap.Uint64("value", val), zap.Uint64("start", uint64(startSeq)), zap.Uint64("end", uint64(endSeq)), zap.Bool("isRound", observed.isRound))
	r.sendReplicationRequests(uint64(startSeq), uint64(endSeq))
}

func (r *replicator) updateState(newValue uint64) {
	r.removeOldValues(newValue)

	// we send out more requests once our seq has caught up to 1/2 of the maxRoundWindow
	if newValue+r.maxRoundWindow/2 > r.highestRequested {
		r.maybeSendMoreReplicationRequests(r.highestObserved, newValue)
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
	nodes := r.highestObserved.signers.Remove(r.myNodeID)
	numNodes := len(nodes)

	seqRequests := DistributeSequenceRequests(start, end, numNodes)

	r.logger.Debug("Distributing replication requests", zap.Uint64("start", start), zap.Uint64("end", end), zap.Stringer("nodes", NodeIDs(nodes)))
	for i, seqs := range seqRequests {
		index := (i + r.requestIterator) % numNodes
		r.sendRequestToNode(seqs.Start, seqs.End, nodes, index)
	}

	// next time we send requests, we start with a different permutation
	r.requestIterator++
}

// sendRequestToNode requests the sequences [start, end] from nodes[index].
// In case the nodes[index] does not respond, we create a timeout that will
// re-send the request.
func (r *replicator) sendRequestToNode(start uint64, end uint64, nodes []NodeID, index int) {
	r.logger.Debug("Requesting missing finalizations ",
		zap.Stringer("from", nodes[index]),
		zap.Uint64("start", start),
		zap.Uint64("end", end))
	seqs := make([]uint64, (end+1)-start)
	for i := start; i <= end; i++ {
		seqs[i-start] = i
		// ensure we set a timeout for this sequence
		r.timeoutHandler.AddTask(i)
	}

	r.highestObservedLock.Lock()
	request := &ReplicationRequest{
		Seqs: seqs,
	}
	if r.highestObserved.isRound {
		request.LatestRound = r.highestObserved.value()
	} else {
		request.LatestSeq = r.highestObserved.value()
	}
	r.highestObservedLock.Unlock()

	msg := &Message{ReplicationRequest: request}

	r.sender.Send(msg, nodes[index])

	if r.highestRequested < end {
		r.highestRequested = end
	}
}

func (r *replicator) storeQuorumRound(round QuorumRound, from NodeID, key uint64) {
	// check if this is the highest round or seq we have seen
	if key > r.highestObserved.value() {
		signedSeq, err := newSignedRoundOrSeq(round)
		if err != nil {
			// should never be here since we already checked the QuorumRound was valid
			r.logger.Error("Error creating signed sequence from round", zap.Error(err))
			return
		}

		r.highestObserved = signedSeq
	}

	if _, exists := r.receivedQuorumRounds[key]; exists {
		// we've already stored this round
		// TODO: add a test where we receive a notarization first from replication, but the chain actually had an empty notarization
		return
	}

	r.receivedQuorumRounds[key] = round

	// we received this sequence, remove the timeout task
	r.timeoutHandler.RemoveTask(key)
	r.logger.Debug("Stored quorum round ", zap.Stringer("qr", &round), zap.String("from", from.String()))
}

func (r *replicator) retrieveQuorumRound(key uint64) (*QuorumRound, bool) {
	qr, ok := r.receivedQuorumRounds[key]
	if ok {
		return &qr, true
	}
	return nil, false
}

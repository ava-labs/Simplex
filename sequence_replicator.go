package simplex

import "time"

// signedSequence is a sequence that has been signed by a quorum certificate.
// it essentially is a quorum round without the enforcement of needing a block with a
// finalization or notarization.
type signedSequence struct {
	seq     uint64
	signers NodeIDs
}

// SequenceReplicator manages the state for replicating sequences up until highestSequenceObserved.
type SequenceReplicator struct {
	sender Sender
	ourNodeID NodeID
	logger Logger
	maxRoundWindow uint64
	// highest seq we have requested. must be <= highestSequenceObserved
	// this is used to limit the number of outstanding requests we have and ensure 
	// we don't request the same sequence multiple times. Ex. if the highestSequenceObserved is 100 ahead of us, we will
	// request in batches of 10 and wait for those to be fulfilled before requesting more and updating lastSequenceRequested.
	lastSequenceRequested uint64

	// highest sequence we have received
	highestSequenceObserved *signedSequence

	// receivedQuorumRounds maps rounds to quorum rounds
	receivedQuorumRounds map[uint64]QuorumRound

	// request iterator
	requestIterator int

	timeoutHandler *TimeoutHandler
}

func NewSequenceReplicator(logger Logger, comm Communication, ourNodeID NodeID, maxRoundWindow uint64, start time.Time) *SequenceReplicator {
	return &SequenceReplicator{
		receivedQuorumRounds: make(map[uint64]QuorumRound),
		sender:               comm,
		ourNodeID:            ourNodeID,
		logger:               logger,
		maxRoundWindow:       maxRoundWindow,
		timeoutHandler:      NewTimeoutHandler(logger, start, comm.Nodes()),
	}
}


func (s *SequenceReplicator) AdvanceTime(now time.Time) {
	s.timeoutHandler.Tick(now)
}

// we have finished replicating sequences if the `nextSeqToCommit` is greater than the highestSequenceObserved
func (s *SequenceReplicator) IsReplicationComplete(nextSeqToCommit uint64) bool {
	if s.highestSequenceObserved == nil {
		return false
	}

	return nextSeqToCommit > s.highestSequenceObserved.seq
}
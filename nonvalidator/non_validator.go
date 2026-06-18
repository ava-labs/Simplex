// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ava-labs/simplex/common"
	"github.com/ava-labs/simplex/simplex"
	"go.uber.org/zap"
)

type finalizedSeq struct {
	block        common.Block
	finalization *common.Finalization
}

func (f *finalizedSeq) String() string {
	seq := uint64(0)
	digest := common.Digest{}
	if f.block != nil {
		seq = f.block.BlockHeader().Seq
		digest = f.block.BlockHeader().Digest
	}
	if f.finalization != nil {
		seq = f.finalization.Finalization.Seq
		digest = f.finalization.Finalization.Digest
	}

	return fmt.Sprintf("FinalizedSeq {BlockDigest: %s, Seq: %d, BlockExists %t, FinalizationExists %t}", digest, seq, f.block != nil, f.finalization != nil)
}

type Config struct {
	Storage                    common.Storage
	Comm                       common.Communication
	SignatureAggregatorCreator common.SignatureAggregatorCreator

	Logger common.Logger

	// How many sequences we allow to look past our next sequence to commit
	MaxSequenceWindow uint64

	// our node ID
	ID common.NodeID

	StartTime time.Time

	// RandomSource is used by the replication state to pick which nodes to
	// request sequences from. If nil, a cryptographically secure source is used.
	RandomSource *rand.Rand
}

type NonValidator struct {
	Config

	lock        *sync.Mutex
	ctx         context.Context
	cancelCtx   context.CancelFunc
	haltedError error

	// ensures we only verify the same block one time
	oneTimeVerifier *simplex.OneTimeVerifier

	// incompleteSequences stores sequences that we have not collected
	// both a block and finalization for. Once both have been received, they are verified & indexed.
	incompleteSequences map[uint64]*finalizedSeq

	// highestEpochCollector
	highestEpochCollector *epochReplicator

	// sequence replication state
	sequenceReplicator *simplex.ReplicationState

	// epochs contain a map of all epochs that have their validator set verified.
	epochs epochs

	verifier *common.BlockDependencyManager
}

// NewNonValidator creates a NonValidator with the given `config`.
func NewNonValidator(config Config) (*NonValidator, error) {
	epochs, err := newEpochs(config.Storage, config.SignatureAggregatorCreator)
	if err != nil {
		return nil, err
	}

	randomSource := config.RandomSource
	if randomSource == nil {
		randomSource, err = simplex.NewRandomSource()
		if err != nil {
			return nil, err
		}
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	scheduler := common.NewScheduler(config.Logger, simplex.DefaultProcessingBlocks)

	lock := &sync.Mutex{}

	replicator := simplex.NewReplicationState(config.Logger, config.Comm, config.ID, config.MaxSequenceWindow, true, config.StartTime, lock, randomSource)

	return &NonValidator{
		Config:                config,
		incompleteSequences:   make(map[uint64]*finalizedSeq),
		ctx:                   ctx,
		cancelCtx:             cancelFunc,
		epochs:                epochs,
		verifier:              common.NewBlockVerificationScheduler(config.Logger, simplex.DefaultProcessingBlocks, scheduler),
		lock:                  lock,
		highestEpochCollector: newEpochReplicator(config.Logger, config.Comm),
		oneTimeVerifier:       simplex.NewOneTimeVerifier(config.Logger),
		sequenceReplicator:    replicator,
	}, nil
}

func (n *NonValidator) Start() {
	n.Logger.Info("Starting non-validator", zap.Stringer("ID", n.ID))
	n.broadcastLatestEpoch()
}

func (n *NonValidator) Stop() {
	n.Logger.Info("Shutting down non-validator", zap.Stringer("ID", n.ID))
	n.cancelCtx()
	n.sequenceReplicator.Close()
	n.verifier.Close()
}

func (n *NonValidator) AdvanceTime(t time.Time) {
	n.sequenceReplicator.AdvanceTime(t)
}

// this function should be ran under a lock?
func (n *NonValidator) HandleMessage(msg *common.Message, from common.NodeID) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	// A closed context means we have shut down.
	if n.ctx.Err() != nil {
		return nil
	}

	if n.haltedError != nil {
		return n.haltedError
	}

	switch {
	case msg.BlockDigestRequest != nil:
		// TODO: it seems reasonable for our non-validator to be able to process these messages and send out responses.
		return nil
	case msg.BlockMessage != nil && msg.BlockMessage.Block != nil:
		return n.handleBlock(msg.BlockMessage.Block, from)
	case msg.Finalization != nil:
		return n.handleFinalization(msg.Finalization, from)
	case msg.ReplicationResponse != nil:
		n.handleReplicationResponse(msg.ReplicationResponse, from)
		return nil
	default:
		n.Logger.Debug("Received unexpected message", zap.Any("Message", msg), zap.Stringer("from", from))
		return nil
	}

}

// handleBlock handles a block message. BlockMessages are sent when the leader proposes a block for its round.
// We only process blocks if they are from the leader and for the current epoch.
// Otherwise, we wait to process blocks until we receive a finalization.
func (n *NonValidator) handleBlock(block common.Block, from common.NodeID) error {
	bh := block.BlockHeader()
	n.Logger.Debug("Received a block message", zap.Uint64("Sequence", bh.Seq), zap.Stringer("From", from))

	epoch, ok := n.epochs[bh.Epoch]
	if !ok {
		n.Logger.Debug("Received a block from an epoch we do not have", zap.Uint64("Epoch", bh.Epoch), zap.Stringer("From", from))
		return nil
	}

	if bh.Seq > n.MaxSequenceWindow+n.Storage.NumBlocks() {
		n.Logger.Debug("Received a block from a sequence too far ahead", zap.Uint64("Num Blocks", n.Storage.NumBlocks()), zap.Uint64("Block Sequence", bh.Seq), zap.Stringer("From", from))
		return nil
	}

	if !bytes.Equal(simplex.LeaderForRound(epoch.nodes.NodeIDs(), bh.Round), from) {
		n.Logger.Debug("Received a block not from the leader of that round", zap.Uint64("Epoch", bh.Epoch), zap.Stringer("From", from))
		return nil
	}

	// If we have already verified the block discard it
	if n.isAccepted(bh.Seq) {
		n.Logger.Debug("Already accepted a block from this round")
		return nil
	}

	incomplete, ok := n.incompleteSequences[bh.Seq]
	// we have not received any blocks or finalizations for this sequence
	if !ok {
		incompleteSeq := &finalizedSeq{
			block: block,
		}
		n.incompleteSequences[bh.Seq] = incompleteSeq
		n.Logger.Debug("Stored incomplete sequence", zap.Stringer("Sequence", incompleteSeq))
		return nil
	}

	// Duplicate block, or finalization not yet received.
	if incomplete.block != nil || incomplete.finalization == nil {
		return nil
	}

	if !bytes.Equal(incomplete.finalization.Finalization.Digest[:], bh.Digest[:]) {
		n.Logger.Info(
			"Received a block from the leader of a round whose digest mismatches the finalization",
			zap.Stringer("Finalization Digest", incomplete.finalization.Finalization.Digest),
			zap.Stringer("Block digest", bh.Digest),
			zap.Stringer("From", from),
		)
		return nil
	}

	// add test that ensure this is here. otherwise i think an adversarial node can have us schedule many tasks
	incomplete.block = block

	n.maybeValidateNextEpoch(block)
	return n.scheduleNewFinalizedBlockTask(block, incomplete.finalization)
}

func (n *NonValidator) isAccepted(seq uint64) bool {
	return n.Storage.NumBlocks() > seq
}

// newFinalizedBlockTask verifies and indexes the nextSeqToCommit.
// This task should only get executed when `block` is next to be verified and indexed.
func (n *NonValidator) newFinalizedBlockTask(block common.Block, finalization *common.Finalization) func() common.Digest {
	return func() common.Digest {
		md := block.BlockHeader()
		n.Logger.Debug("Block verification started", zap.Uint64("sequence", md.Seq))
		start := time.Now()
		defer func() {
			elapsed := time.Since(start)
			n.Logger.Debug("Block verification ended", zap.Uint64("sequence", md.Seq), zap.Duration("elapsed", elapsed))
		}()

		verifiedBlock, err := block.Verify(n.ctx)
		// We have failed verifying a finalized block
		if err != nil {
			n.lock.Lock()
			defer n.lock.Unlock()

			// defensive check in case we scheduled multiple finalized block verification tasks
			if n.Storage.NumBlocks() != md.Seq {
				n.Logger.Debug("Received finalized block that is not the next sequence to commit",
					zap.Uint64("seq", md.Seq), zap.Uint64("height", n.Storage.NumBlocks()))
				return md.Digest
			}

			n.Logger.Info("Failed verifying a block that has a finalization", zap.Uint64("Block Seq", md.Seq), zap.Stringer("Block Digest", md.Digest), zap.Error(err))

			n.sequenceReplicator.ResendFinalizationRequest(md.Seq, finalization.QC.Signers())
			return md.Digest
		}

		n.lock.Lock()
		defer n.lock.Unlock()

		// defensive check in case we scheduled multiple finalized block verification tasks
		if n.Storage.NumBlocks() != md.Seq {
			n.Logger.Debug("Received finalized block that is not the next sequence to commit",
				zap.Uint64("seq", md.Seq), zap.Uint64("height", n.Storage.NumBlocks()))
			return md.Digest
		}

		if err := n.Storage.Index(n.ctx, verifiedBlock, *finalization); err != nil {
			n.haltedError = err
			n.Logger.Info("Failed indexing a block and finalization", zap.Uint64("Block Seq", md.Seq), zap.Stringer("Block Digest", md.Digest), zap.Error(err))
			return md.Digest
		}

		n.Logger.Info("Verified and Indexed Block", zap.Uint64("Block Seq", md.Seq), zap.Stringer("Block Digest", md.Digest))

		n.removeOldSequencesAndEpochs(md.Seq, md.Epoch)

		// in case we need to queue up any more tasks
		if err := n.processReplicationState(); err != nil {
			n.haltedError = err
			n.Logger.Info("Failed calling process replication state", zap.Error(err))
			return md.Digest
		}

		return md.Digest
	}
}

func (n *NonValidator) maybeValidateNextEpoch(block common.Block) {
	sealingInfo := block.SealingBlockInfo()
	if sealingInfo == nil {
		return
	}
	_, alreadyValidated := n.epochs[sealingInfo.Epoch]
	if alreadyValidated {
		n.Logger.Info("Already validated.", zap.Uint64("Epoch", sealingInfo.Epoch))
		return
	}

	n.Logger.Info("We have a valid sealing block, messages for that epoch can be processed.", zap.Uint64("Epoch", sealingInfo.Epoch))
	n.epochs[sealingInfo.Epoch] = newEpochMetadata(sealingInfo, n.SignatureAggregatorCreator)
}

func (n *NonValidator) removeOldSequencesAndEpochs(lastCommittedSeq, minEpochToKeep uint64) {
	for seq := range n.incompleteSequences {
		if seq <= lastCommittedSeq {
			delete(n.incompleteSequences, seq)
		}
	}

	n.epochs.removeOldEpochs(minEpochToKeep)
}

// handleFinalization process a finalization message. If its for a future epoch, it will forward the finalization
// to the replication handler.
func (n *NonValidator) handleFinalization(finalization *common.Finalization, from common.NodeID) error {
	bh := finalization.Finalization.BlockHeader

	n.Logger.Debug("Received a finalization", zap.Uint64("Seq", bh.Seq), zap.Stringer("From", from))

	if n.isAccepted(bh.Seq) {
		n.Logger.Debug("Received a stale finalization", zap.Uint64("Seq", bh.Seq), zap.Stringer("From", from))
		return nil
	}

	epoch, ok := n.epochs[bh.Epoch]
	if !ok {
		// This finalization is after our lastAcceptedEpoch and is for an unknown Epoch, request that node to send us the sealing block.
		n.Logger.Debug("Received a finalization from an unknown epoch", zap.Uint64("Unknown Epoch", bh.Epoch), zap.Stringer("From", from))
		n.sendRequest(bh.Epoch, from)
		return nil
	}

	if err := simplex.VerifyQC(finalization.QC, epoch.signatureAggregator.IsQuorum, epoch.nodeLookup, finalization, epoch.nodes); err != nil {
		n.Logger.Debug("Received an invalid finalization",
			zap.Error(err),
			zap.Int("round", int(bh.Round)),
			zap.Stringer("NodeID", from))
		return nil
	}

	// Don't store finalization in memory if it's too far ahead
	if bh.Seq > n.MaxSequenceWindow+n.Storage.NumBlocks() {
		n.Logger.Debug("Received a finalization from a sequence too far ahead", zap.Uint64("Num Blocks", n.Storage.NumBlocks()), zap.Uint64("Block Sequence", bh.Seq), zap.Stringer("From", from))
		n.sequenceReplicator.ReceivedFutureFinalization(finalization, n.nextSeqToCommit())
		return nil
	}

	incomplete, ok := n.incompleteSequences[bh.Seq]
	if !ok {
		// we have not received anything for this sequence
		incompleteSeq := &finalizedSeq{
			finalization: finalization,
		}
		n.incompleteSequences[bh.Seq] = incompleteSeq
		n.Logger.Debug("Stored incomplete sequence", zap.Stringer("Sequence", incompleteSeq))
		n.sequenceReplicator.ReceivedFutureFinalization(finalization, n.nextSeqToCommit())
		return nil
	}

	// Duplicate finalization received.
	if incomplete.finalization != nil {
		// sanity check: should never happen.
		if !bytes.Equal(incomplete.finalization.Finalization.Bytes(), finalization.Finalization.Bytes()) {
			n.Logger.Warn(
				"Mismatching finalizations",
				zap.Uint64("Incoming Sequence", finalization.Finalization.Seq),
				zap.Uint64("Stored sequence", incomplete.finalization.Finalization.Seq),
			)
			n.haltedError = fmt.Errorf("Conflicting finalizations")
			return fmt.Errorf("Conflicting finalizations")
		}

		n.sequenceReplicator.ReceivedFutureFinalization(finalization, n.nextSeqToCommit())
		return nil
	}

	incomplete.finalization = finalization

	// No block received yet for this sequence.
	if incomplete.block == nil {
		n.sequenceReplicator.ReceivedFutureFinalization(finalization, n.nextSeqToCommit())
		return nil
	}

	digest := incomplete.block.BlockHeader().Digest
	if !bytes.Equal(bh.Digest[:], digest[:]) {
		n.Logger.Info(
			"Received a block from the leader of a round whose digest mismatches the finalization",
			zap.Stringer("Finalization Digest", bh.Digest),
			zap.Stringer("Block digest", digest),
			zap.Stringer("From", from),
		)

		n.sequenceReplicator.ReceivedFutureFinalization(finalization, n.nextSeqToCommit())
		return nil
	}

	n.maybeValidateNextEpoch(incomplete.block)
	return n.scheduleNewFinalizedBlockTask(incomplete.block, incomplete.finalization)
}

func (n *NonValidator) scheduleNewFinalizedBlockTask(block common.Block, finalization *common.Finalization) error {
	bh := finalization.Finalization.BlockHeader
	if n.verifier.IsSequenceScheduled(bh.Seq) {
		// Avoid scheduling more than one task.
		// If verification during the task fails, we will try and reschedule.
		return nil
	}

	finalizedBlockTask := n.newFinalizedBlockTask(n.oneTimeVerifier.Wrap(block), finalization)

	var prev *common.Digest
	if bh.Seq > 0 && !n.isAccepted(bh.Seq-1) {
		prev = &bh.Prev
	}
	return n.verifier.ScheduleTaskWithDependencies(finalizedBlockTask, bh.Seq, prev, []uint64{})
}

func (n *NonValidator) handleReplicationResponse(resp *common.ReplicationResponse, from common.NodeID) error {
	n.Logger.Debug("Received replication response", zap.Stringer("from", from), zap.Int("num seqs", len(resp.Data)), zap.Stringer("latest seq", resp.LatestSeq), zap.Stringer("From", from), zap.Stringers("Data", resp.Data))

	for _, qr := range resp.Data {
		if err := n.processQuorumRound(&qr, from); err != nil {
			n.Logger.Debug("Failed processing quorum round", zap.Stringer("QR", qr), zap.Error(err))
		}
	}

	if err := n.processQuorumRound(resp.LatestSeq, from); err != nil {
		n.Logger.Debug("Failed processing latest seq", zap.Stringer("QR", resp.LatestSeq), zap.Error(err))
	}

	return n.processReplicationState()
}

func (n *NonValidator) processReplicationState() error {
	nextSeqToCommit := n.nextSeqToCommit()
	n.sequenceReplicator.MaybeAdvanceState(nextSeqToCommit, 0, 0)

	// first we check if we can commit the next sequence, it is ok to try and commit the next sequence
	// directly, since if there are any empty notarizations, `indexFinalization` will
	// increment the round properly.
	block, finalization, exists := n.sequenceReplicator.GetFinalizedBlockForSequence(nextSeqToCommit)
	if !exists {
		return nil
	}

	// verify the finalization
	epoch, ok := n.epochs[block.BlockHeader().Epoch]
	if !ok {
		return fmt.Errorf("expected epoch to have been validated: %d", block.BlockHeader().Epoch)
	}

	err := simplex.VerifyQC(finalization.QC, epoch.signatureAggregator.IsQuorum, epoch.nodeLookup, finalization, epoch.nodes)
	if err != nil {
		n.Logger.Debug("Failed verifying QC that was next to commit", zap.Error(err))
		// We fetch from comm.Nodes instead of the nodes given in the finalization, because this node may give us an adversarial node list.
		n.sequenceReplicator.ResendFinalizationRequest(block.BlockHeader().Seq, n.Comm.Nodes().NodeIDs())
		return nil
	}

	n.sequenceReplicator.DeleteSeq(nextSeqToCommit)

	return n.scheduleNewFinalizedBlockTask(block, finalization)
}

// This should only validate and store in the state.
func (n *NonValidator) processQuorumRound(qr *common.QuorumRound, from common.NodeID) error {
	if qr == nil {
		return nil
	}

	if err := qr.VerifyQCConsistentWithBlock(); err != nil {
		return err
	}

	block := qr.Block
	finalization := qr.Finalization

	// Non validators only process quorum rounds with finalizations
	if finalization == nil {
		return nil
	}

	if n.isAccepted(block.BlockHeader().Seq) {
		return fmt.Errorf("processing quorum round for a block we already indexed")
	}

	epoch, ok := n.epochs[block.BlockHeader().Epoch]
	if !ok {
		n.Logger.Debug("Received a QR from an Epoch that we have not validated", zap.Uint64("Epoch", block.BlockHeader().Epoch), zap.Uint64("Block Seq", block.BlockHeader().Seq), zap.Stringer("Block digest", block.BlockHeader().Digest))
		n.sendRequest(qr.Block.BlockHeader().Epoch, from)

		// This block is in an epoch that we do not have. Therefore, we cannot verify its finalization.
		// However, if it is a sealing block we may be able to validate the epoch if its part of the sealing block hash-chain.
		if n.epochs.canValidate(block) {
			n.Logger.Debug("We can validate an epoch block as we have validated the one after it.", zap.Stringer("Info", block.SealingBlockInfo()))
			n.maybeValidateNextEpoch(block)

			// We are storing a quorum round with a finalization we have not yet verified.
			// We do this to tell the replicator a valid sequence exists and to begin replication if necessary.
			// We will check the validity when we process this round.
			n.sequenceReplicator.StoreQuorumRound(qr)
			return nil
		}

		if n.highestEpochCollector.collectedQuorumRound(qr, from) {
			n.Logger.Debug("We can validate an epoch because we have received a threshold of messages of it.", zap.Stringer("Info", block.SealingBlockInfo()))
			n.maybeValidateNextEpoch(block)

			// We are storing a quorum round with a finalization we have not yet verified.
			// We do this to tell the replicator a valid sequence exists and to begin replication if necessary.
			// We will check the validity when we process this round.
			n.sequenceReplicator.StoreQuorumRound(qr)
		}

		return nil
	}

	err := simplex.VerifyQC(qr.Finalization.QC, epoch.signatureAggregator.IsQuorum, epoch.nodeLookup, qr.Finalization, epoch.nodes)
	if err != nil {
		return fmt.Errorf("could not verify quorum round QC: %w", err)
	}

	// This block could be a sealing block, validate the next epoch if so.
	n.maybeValidateNextEpoch(block)
	n.sequenceReplicator.StoreQuorumRound(qr)
	return nil
}

// TODO: add a re-broadcast timeout task until we have validated an epoch.
func (n *NonValidator) broadcastLatestEpoch() {
	highestEpoch, _ := n.epochs.highestEpoch()

	// Sending a LatestFinalizedSeq of 0 gets ignored by validators.
	if highestEpoch == 0 {
		highestEpoch = 1
	}

	request := &common.ReplicationRequest{
		LatestFinalizedSeq: highestEpoch,
	}

	n.Comm.Broadcast(&common.Message{
		ReplicationRequest: request,
	})
}

// sendRequest sends a common.BlockDigestRequest for a given sequence to a node.
func (n *NonValidator) sendRequest(seq uint64, to common.NodeID) {
	digestRequest := common.BlockDigestRequest{
		Seq:    seq,
		Digest: common.Digest{}, // TODO: In the epoch code, update how we process digests to not drop the request given an empty digest.
	}

	n.Logger.Debug("Broadcasting sealing block request", zap.Uint64("Requesting Seq", seq))

	n.Config.Comm.Send(&common.Message{
		BlockDigestRequest: &digestRequest,
	}, to)
}

func (n *NonValidator) nextSeqToCommit() uint64 {
	return n.Storage.NumBlocks()
}

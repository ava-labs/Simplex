// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"bytes"
	"context"
	"errors"
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

	// TransitionToValidator is called when our non-validator indexes the highest known epoch
	// and it is in the validator set
	TransitionToValidator func(epoch uint64, validators common.Nodes)

	// Bootstrapped is set once every epoch from our tip up to the one a threshold of the latest
	// validator set reported has been validated. Until then only replication responses are handled.
	Bootstrapped bool
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
	highestEpochCollector *epochDigestCounter

	// sequence replication state
	sequenceReplicator *simplex.ReplicationState

	// epochs contain a map of all epochs that have their validator set verified.
	epochs epochs

	verifier *common.BlockDependencyManager

	// sealingBlockTimeouts re-requests the sealing blocks between the highest validated epoch and
	// our tip that have not been validated yet. It holds exactly the missing ones, so no tasks
	// means every sealing block down to our tip is validated.
	sealingBlockTimeouts *common.TimeoutHandler[uint64]
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

	nv := &NonValidator{
		Config:                config,
		incompleteSequences:   make(map[uint64]*finalizedSeq),
		ctx:                   ctx,
		cancelCtx:             cancelFunc,
		epochs:                epochs,
		verifier:              common.NewBlockVerificationScheduler(config.Logger, simplex.DefaultProcessingBlocks, scheduler),
		lock:                  lock,
		highestEpochCollector: newEpochReplicator(config.Logger, config.Comm.Validators),
		oneTimeVerifier:       simplex.NewOneTimeVerifier(config.Logger),
		sequenceReplicator:    replicator,
	}
	nv.sealingBlockTimeouts = common.NewTimeoutHandler(config.Logger, "sealing block replication", config.StartTime, simplex.DefaultReplicationRequestTimeout, nv.requestMissingSealingBlocks)
	if !config.Bootstrapped {
		nv.sealingBlockTimeouts.AddTask(startBroadcastTask)
	}

	return nv, nil
}

func (n *NonValidator) Start() {
	n.Logger.Info("Starting non-validator", zap.Stringer("ID", n.ID))
	n.broadcastLatestEpoch()
}

func (n *NonValidator) Stop() {
	n.Logger.Info("Shutting down non-validator", zap.Stringer("ID", n.ID))
	n.cancelCtx()
	n.sequenceReplicator.Close()
	n.sealingBlockTimeouts.Close()
	n.verifier.Close()
}

func (n *NonValidator) AdvanceTime(t time.Time) {
	n.sequenceReplicator.AdvanceTime(t)
	n.sealingBlockTimeouts.Tick(t)
}

// IsBootstrapped reports whether bootstrapping has finished.
// Bootstrapping finishes when every sealing block down to our tip is validated.
func (n *NonValidator) IsBootstrapped() bool {
	n.lock.Lock()
	defer n.lock.Unlock()

	return n.Bootstrapped
}

func (n *NonValidator) HandleMessage(msg *common.Message, from common.NodeID) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	// A closed context means we have shut down.
	if n.ctx.Err() != nil {
		return nil
	}

	n.Logger.Debug("Received a message", zap.Any("Message", msg), zap.Stringer("from", from))

	if n.haltedError != nil {
		return n.haltedError
	}

	if !n.Bootstrapped && msg.ReplicationResponse == nil {
		n.Logger.Debug("Dropping message received while bootstrapping, we only accept replication responses", zap.Any("Message", msg), zap.Stringer("From", from))
		return nil
	}

	switch {
	case msg.BlockMessage != nil && msg.BlockMessage.Block != nil:
		return n.handleBlock(msg.BlockMessage.Block, from)
	case msg.Finalization != nil:
		return n.handleFinalization(msg.Finalization, from)
	case msg.ReplicationResponse != nil:
		return n.handleReplicationResponse(msg.ReplicationResponse, from)
	default:
		n.Logger.Debug("Received unexpected message", zap.Any("Message", msg), zap.Stringer("from", from))
		return nil
	}
}

// processBootstrapQuorumRound handles quorum rounds until bootstrapping finishes.
// Once a threshold validates an epoch, only sealing blocks are validated
// and stored(in a backwards manner).
func (n *NonValidator) processBootstrapQuorumRound(qr *common.QuorumRound, from common.NodeID) error {
	block := qr.Block
	bh := block.BlockHeader()
	sealingInfo := block.SealingBlockInfo()

	if sealingInfo == nil {
		n.sendRequest(bh.Epoch, from)
		return nil
	}

	switch {
	case n.epochs.canValidate(block):
		// The sealing block in the backwards hash chain
		n.validateSealingBlock(qr, from)
	case n.highestEpochCollector.collectedSealingBlockInfo(sealingInfo, bh, from):
		n.Logger.Info("A threshold of validators reported a sealing block", zap.Uint64("Seq", bh.Seq), zap.Stringer("Info", sealingInfo))
		n.sealingBlockTimeouts.RemoveTask(startBroadcastTask)
		if !n.isIndexed(bh.Seq) {
			n.validateSealingBlock(qr, from)
		}
	default:
		return nil
	}

	// No sealing block is missing, so every epoch from our tip to the highest is validated.
	if !n.sealingBlockTimeouts.HasTasks() {
		n.finishBootstrap()
		// If the highest epoch is already indexed, nothing more gets indexed to trigger the transition.
		highestEpoch, validators := n.epochs.highestEpoch()
		n.maybeTransitionToValidator(highestEpoch, validators)
	}
	return nil
}

// validateSealingBlock validates the epoch a sealing block opens and stores its quorum round.
// The finalization has not been verified yet. Storing tells the replicator a valid sequence exists
// and its validity is checked when the round is processed.
func (n *NonValidator) validateSealingBlock(qr *common.QuorumRound, from common.NodeID) {
	n.maybeValidateNextEpoch(qr.Block, from)
	n.sequenceReplicator.StoreQuorumRound(qr)
}

// finishBootstrap marks bootstrapping done. Every epoch from our tip to the highest one a
// threshold of validators reported is validated, so replication and live messages can be handled.
func (n *NonValidator) finishBootstrap() {
	n.Bootstrapped = true
	highestEpoch, _ := n.epochs.highestEpoch()
	n.Logger.Info("Finished bootstrapping", zap.Uint64("Highest Epoch", highestEpoch))
}

// maybeTransitionToValidator calls TransitionToValidator when epoch is the highest validated epoch,
// its sealing block is indexed and its validator set contains us.
func (n *NonValidator) maybeTransitionToValidator(epoch uint64, validators common.Nodes) {
	highestEpoch, highestValidatorSet := n.epochs.highestEpoch()
	if highestEpoch != epoch || !n.isIndexed(epoch) || !highestValidatorSet.Contains(n.ID) || n.TransitionToValidator == nil {
		return
	}
	n.TransitionToValidator(epoch, validators)
}

// startBroadcastTask is the sealingBlockTimeouts task that repeats the start broadcast until a
// threshold of responses validates an epoch above our tip. Seq 0 is genesis, never a sealing block we request.
const startBroadcastTask uint64 = 0

// requestMissingSealingBlocks re-requests sealing blocks of the hash chain that timed out
// from every validator. Runs on the timeout handler's goroutine.
func (n *NonValidator) requestMissingSealingBlocks(seqs []uint64) {
	n.lock.Lock()
	defer n.lock.Unlock()

	if n.ctx.Err() != nil {
		return
	}

	for _, seq := range seqs {
		if seq == startBroadcastTask {
			n.broadcastLatestEpoch()
			continue
		}
		n.Logger.Debug("Re-requesting a sealing block", zap.Uint64("Seq", seq))
		n.Comm.Broadcast(&common.Message{
			ReplicationRequest: &common.ReplicationRequest{Seqs: []uint64{seq}},
		})
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

	if bh.Seq > n.MaxSequenceWindow+n.nextSeqToCommit() {
		n.Logger.Debug("Received a block from a sequence too far ahead", zap.Uint64("Num Blocks", n.Storage.NumBlocks()), zap.Uint64("Block Sequence", bh.Seq), zap.Stringer("From", from))
		return nil
	}

	if !bytes.Equal(simplex.LeaderForRound(epoch.nodes.NodeIDs(), bh.Round), from) {
		n.Logger.Debug("Received a block not from the leader of that round", zap.Uint64("Epoch", bh.Epoch), zap.Stringer("From", from))
		return nil
	}

	// If we have already verified the block discard it
	if n.isIndexed(bh.Seq) {
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
		n.Logger.Debug(
			"Received a block from the leader of a round whose digest mismatches the finalization",
			zap.Stringer("Finalization Digest", incomplete.finalization.Finalization.Digest),
			zap.Stringer("Block digest", bh.Digest),
			zap.Stringer("From", from),
		)
		return nil
	}

	incomplete.block = block

	n.maybeValidateNextEpoch(block, from)
	return n.scheduleNewFinalizedBlockTask(block, incomplete.finalization)
}

func (n *NonValidator) isIndexed(seq uint64) bool {
	return n.nextSeqToCommit() > seq
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

		verifiedBlock, err := block.Verify(n.ctx, common.OnlyVMVerifyOpt)
		n.lock.Lock()
		defer n.lock.Unlock()

		nextSeqToCommit := n.nextSeqToCommit()
		// defensive check in case we scheduled multiple finalized block verification tasks
		if nextSeqToCommit != md.Seq {
			n.Logger.Debug("Received finalized block that is not the next sequence to commit",
				zap.Uint64("Received Seq", md.Seq), zap.Uint64("Next Seq", nextSeqToCommit))
			return md.Digest
		}

		// We have failed verifying a finalized block
		if err != nil {
			n.Logger.Info("Failed verifying a block that has a finalization", zap.Uint64("Block Seq", md.Seq), zap.Stringer("Block Digest", md.Digest), zap.Error(err))
			n.sequenceReplicator.ResendFinalizationRequest(md.Seq, finalization.QC.Signers())
			return md.Digest
		}

		if err := n.Storage.Index(n.ctx, verifiedBlock, *finalization); err != nil {
			n.haltedError = err
			n.Logger.Info("Failed indexing a block and finalization", zap.Uint64("Block Seq", md.Seq), zap.Stringer("Block Digest", md.Digest), zap.Error(err))
			return md.Digest
		}

		// Indexing a sealing block may make us a validator of the epoch it opens.
		if block.SealingBlockInfo() != nil {
			n.maybeTransitionToValidator(md.Seq, block.SealingBlockInfo().ValidatorSet)
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

// maybeValidateNextEpoch validates the epoch block opens when block is a sealing block. While
// bootstrapping it also requests the sealing block that opened block's own epoch, following the
// hash chain back until every sealing block down to an epoch we have indexed is validated.
func (n *NonValidator) maybeValidateNextEpoch(block common.Block, from common.NodeID) {
	bh := block.BlockHeader()
	nextEpoch := bh.Seq
	sealingInfo := block.SealingBlockInfo()
	if sealingInfo == nil {
		return
	}
	_, alreadyValidated := n.epochs[nextEpoch]
	if alreadyValidated {
		n.Logger.Debug("Already validated.", zap.Uint64("Epoch", nextEpoch))
		return
	}

	n.Logger.Info("We have a valid sealing block, messages for that epoch can be processed.", zap.Uint64("Epoch", nextEpoch))
	n.epochs[nextEpoch] = newEpochMetadata(nextEpoch, sealingInfo, n.SignatureAggregatorCreator)

	if n.Bootstrapped {
		return
	}

	n.sealingBlockTimeouts.RemoveTask(nextEpoch)

	// The first simplex block opens its own epoch, so there is no earlier sealing block.
	prevSealingSeq := bh.Epoch
	_, known := n.epochs[prevSealingSeq]
	if prevSealingSeq == nextEpoch || n.isIndexed(prevSealingSeq) || known {
		return
	}

	n.sendRequest(prevSealingSeq, from)
	n.sealingBlockTimeouts.AddTask(prevSealingSeq)
}

func (n *NonValidator) removeOldSequencesAndEpochs(lastCommittedSeq, minEpochToKeep uint64) {
	for seq := range n.incompleteSequences {
		if seq <= lastCommittedSeq {
			delete(n.incompleteSequences, seq)
		}
	}

	n.epochs.removeOldEpochs(minEpochToKeep)
	n.highestEpochCollector.removeOldEpochs(minEpochToKeep)
}

// handleFinalization process a finalization message. If its for a future epoch, it will forward the finalization
// to the replication handler.
func (n *NonValidator) handleFinalization(finalization *common.Finalization, from common.NodeID) error {
	bh := finalization.Finalization.BlockHeader

	n.Logger.Debug("Received a finalization", zap.Uint64("Seq", bh.Seq), zap.Stringer("From", from))

	if n.isIndexed(bh.Seq) {
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

	if err := simplex.VerifyQC(finalization.QC, epoch.signatureAggregator.IsQuorum, epoch.eligibleSigners, finalization, epoch.nodes); err != nil {
		n.Logger.Debug("Received an invalid finalization",
			zap.Error(err),
			zap.Int("round", int(bh.Round)),
			zap.Stringer("NodeID", from))
		return nil
	}

	// Don't store finalization in memory if it's too far ahead
	if bh.Seq > n.MaxSequenceWindow+n.nextSeqToCommit() {
		n.Logger.Debug("Received a finalization from a sequence too far ahead", zap.Uint64("Num Blocks", n.nextSeqToCommit()), zap.Uint64("Block Sequence", bh.Seq), zap.Stringer("From", from))
		n.sequenceReplicator.ReceivedFutureFinalization(finalization, n.nextSeqToCommit())
		return nil
	}

	incomplete, ok := n.incompleteSequences[bh.Seq]
	if !ok || (incomplete.finalization == nil || incomplete.block == nil) {
		n.sequenceReplicator.ReceivedFutureFinalization(finalization, n.nextSeqToCommit())
	}

	if !ok {
		// we have not received anything for this sequence
		incompleteSeq := &finalizedSeq{
			finalization: finalization,
		}
		n.incompleteSequences[bh.Seq] = incompleteSeq
		n.Logger.Debug("Stored incomplete sequence", zap.Stringer("Sequence", incompleteSeq))
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
			errConflictingFinalizations := fmt.Errorf("conflicting finalizations. seq: %d", bh.Seq)
			n.haltedError = errConflictingFinalizations
			return errConflictingFinalizations
		}

		return nil
	}

	incomplete.finalization = finalization

	// No block received yet for this sequence.
	if incomplete.block == nil {
		return nil
	}

	digest := incomplete.block.BlockHeader().Digest
	if !bytes.Equal(bh.Digest[:], digest[:]) {
		n.Logger.Debug(
			"Received a block from the leader of a round whose digest mismatches the finalization",
			zap.Stringer("Finalization Digest", bh.Digest),
			zap.Stringer("Block digest", digest),
			zap.Stringer("From", from),
		)

		n.sequenceReplicator.ReceivedFutureFinalization(finalization, n.nextSeqToCommit())
		return nil
	}

	n.maybeValidateNextEpoch(incomplete.block, from)
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
	if bh.Seq > 0 && !n.isIndexed(bh.Seq-1) {
		prev = &bh.Prev
	}
	return n.verifier.ScheduleTaskWithDependencies(finalizedBlockTask, bh.Seq, prev, []uint64{})
}

func (n *NonValidator) handleReplicationResponse(resp *common.ReplicationResponse, from common.NodeID) error {
	n.Logger.Debug("Received replication response", zap.Stringer("from", from), zap.Int("num seqs", len(resp.Data)), zap.Stringer("latest seq", resp.LatestSeq), zap.Stringer("From", from))

	for _, qr := range resp.Data {
		if err := n.processQuorumRound(&qr, from); err != nil {
			n.Logger.Debug("Failed processing quorum round", zap.Stringer("QR", &qr), zap.Error(err))
		}
	}

	if err := n.processQuorumRound(resp.LatestSeq, from); err != nil {
		n.Logger.Debug("Failed processing latest seq", zap.Stringer("QR", resp.LatestSeq), zap.Error(err))
	}

	return n.processReplicationState()
}

func (n *NonValidator) processReplicationState() error {
	if !n.Bootstrapped {
		return nil
	}

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

	err := simplex.VerifyQC(finalization.QC, epoch.signatureAggregator.IsQuorum, epoch.eligibleSigners, finalization, epoch.nodes)
	if err != nil {
		n.Logger.Debug("Failed verifying QC that was next to commit", zap.Error(err))
		// We fetch from comm.Nodes instead of the nodes given in the finalization, because this node may give us an adversarial node list.
		n.sequenceReplicator.ResendFinalizationRequest(block.BlockHeader().Seq, n.Comm.Validators().NodeIDs())
		return nil
	}

	n.sequenceReplicator.DeleteSeq(nextSeqToCommit)

	return n.scheduleNewFinalizedBlockTask(block, finalization)
}

// processQuorumRound validates qr can be stored in the replication state. We also try and validate
// epochs when qr has a sealing block, either by checking that we have received a threshold, or by backwards hash chain validation.
// Returns an error if the qr could not be processed.
func (n *NonValidator) processQuorumRound(qr *common.QuorumRound, from common.NodeID) error {
	if err := verifyQuorumRound(qr); err != nil {
		return err
	}

	// Runs before rejecting indexed blocks, an indexed sealing block is still a vote while bootstrapping.
	if !n.Bootstrapped {
		return n.processBootstrapQuorumRound(qr, from)
	}

	block := qr.Block

	if n.isIndexed(block.BlockHeader().Seq) {
		return fmt.Errorf("processing quorum round for a block we already indexed")
	}

	epoch, ok := n.epochs[block.BlockHeader().Epoch]
	if !ok {
		n.handleQrFromUnknownEpoch(qr, from)
		return nil
	}

	err := simplex.VerifyQC(qr.Finalization.QC, epoch.signatureAggregator.IsQuorum, epoch.eligibleSigners, qr.Finalization, epoch.nodes)
	if err != nil {
		return fmt.Errorf("could not verify quorum round QC: %w", err)
	}

	// This block could be a sealing block, validate the next epoch if so.
	n.maybeValidateNextEpoch(block, from)
	n.sequenceReplicator.StoreQuorumRound(qr)
	return nil
}

// verifyQuorumRound verifies a qr can be processed by the non-validator.
func verifyQuorumRound(qr *common.QuorumRound) error {
	if qr == nil {
		return errors.New("nil quorum round")
	}

	if err := qr.VerifyQCConsistentWithBlock(); err != nil {
		return err
	}

	if qr.Block == nil || qr.Finalization == nil {
		return errors.New("ignoring quorum round without a block and finalization")
	}

	return nil
}

func (n *NonValidator) handleQrFromUnknownEpoch(qr *common.QuorumRound, from common.NodeID) {
	block := qr.Block
	bh := block.BlockHeader()
	n.Logger.Debug("Received a QR from an Epoch that we have not validated",
		zap.Uint64("Epoch", bh.Epoch),
		zap.Uint64("Block Seq", bh.Seq),
		zap.Stringer("Block digest", bh.Digest))

	n.sendRequest(bh.Epoch, from)

	// This block is in an epoch that we do not have. Therefore, we cannot verify its finalization.
	// However, if it is a sealing block we may be able to validate the epoch if its part of the sealing block hash-chain.
	if n.epochs.canValidate(block) {
		n.Logger.Debug("We can validate an epoch block as we have validated the one after it.", zap.Stringer("Info", block.SealingBlockInfo()))
		n.validateSealingBlock(qr, from)
		return
	}

	if n.highestEpochCollector.collectedSealingBlockInfo(block.SealingBlockInfo(), bh, from) {
		n.Logger.Debug("We can validate an epoch because we have received a threshold of messages of it.", zap.Stringer("Info", block.SealingBlockInfo()))
		n.validateSealingBlock(qr, from)
	}
}

func (n *NonValidator) broadcastLatestEpoch() {
	highestEpoch, _ := n.epochs.highestEpoch()
	request := &common.ReplicationRequest{
		LatestFinalizedSeq: highestEpoch,
	}

	// Sending a LatestFinalizedSeq of 0 gets ignored by validators.
	if highestEpoch == 0 {
		request.LatestFinalizedSeq = 1
	}

	n.Comm.Broadcast(&common.Message{
		ReplicationRequest: request,
	})
}

// sendRequest sends a common.ReplicationRequest for a given sequence to a node.
func (n *NonValidator) sendRequest(seq uint64, to common.NodeID) {
	request := common.ReplicationRequest{
		Seqs: []uint64{seq},
	}

	n.Logger.Debug("Sending sealing block request", zap.Uint64("Requesting Seq", seq))

	n.Comm.Send(&common.Message{
		ReplicationRequest: &request,
	}, to)
}

func (n *NonValidator) nextSeqToCommit() uint64 {
	return n.Storage.NumBlocks()
}

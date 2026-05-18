// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ava-labs/simplex"
	"go.uber.org/zap"
)

var (
	errNoGenesis = errors.New("No Genesis Found")
)

type epochMetadata struct {
	nodes               simplex.Nodes
	nodeLookup          map[string]struct{}
	epoch               uint64
	signatureAggregator simplex.SignatureAggregator
	sealingMetadata     *simplex.SealingBlockInfo
}

func newEpochMetadata(epoch uint64, nodes []simplex.Node, sigCreator simplex.SignatureAggregatorCreator, sealingMetadata *simplex.SealingBlockInfo) *epochMetadata {
	lookup := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		lookup[string(node.Node)] = struct{}{}
	}

	return &epochMetadata{
		nodes:               nodes,
		nodeLookup:          lookup,
		epoch:               epoch,
		signatureAggregator: sigCreator(nodes),
		sealingMetadata:     sealingMetadata,
	}
}

// ValidatorSetRetriever returns the validator set for the given epoch.
// We use epochs not pChainHeight, because NonValidators receive simplex.Blocks
// which are interfaces that do not expose the pChainHeight.
type ValidatorSetRetriever func(pChainReference uint64) ([]simplex.Node, error)
type Config struct {
	Logger                     simplex.Logger
	Storage                    simplex.Storage
	ValidatorSetRetriever      ValidatorSetRetriever
	SignatureAggregatorCreator simplex.SignatureAggregatorCreator
	// how many rounds we allow to look past our current
	MaxRoundWindow uint64
	// amount of dependencies we are willing to load into the block verifier
	MaxDependencies uint64
}

type NonValidator struct {
	Config

	ctx         context.Context
	cancelCtx   context.CancelFunc
	haltedError error

	// incompleteSequences stores sequences that we have not collected
	// both a block and finalization for. Once both have been received, they are verified & indexed.
	// TODO: garbage collect old sequences
	incompleteSequences map[uint64]*finalizedSeq

	// lastAcceptedEpoch contains the metadata of the highest epoch that our NonValidating node has accepted.
	// An epoch that is accepted means we have verified and indexed the sealing block which created this epoch.
	// i.e. Seq 100 seals Epoch 50. Epoch 100 is the last accepted epoch.
	// All blocks before this epoch have been accepted and indexed.
	// lastAcceptedEpoch *epochMetadata

	// epochs contain a map of all epochs that have their validator set verified.
	epochs map[uint64]epochMetadata

	verifier *simplex.BlockDependencyManager
}

// NewNonValidator creates a NonValidator with the given `config`.
func NewNonValidator(config Config) (*NonValidator, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())

	// Retrieve the last block so we know where to start bootstrapping from
	lastBlockHeight := config.Storage.NumBlocks()

	if lastBlockHeight == 0 {
		return nil, errNoGenesis
	}

	// TODO: test when the last block retrieved is a sealing block. Currently we would set the lastAcceptedEpoch to sealingblock.epoch, but technically we could set it to sealingblock.seq. Would save a step.
	lastBlock, _, err := config.Storage.Retrieve(lastBlockHeight - 1)
	if err != nil {
		return nil, err
	}
	lastBlockEpoch := lastBlock.BlockHeader().Epoch
	pChainReference, _, err := config.Storage.Retrieve(lastBlockEpoch)
	if err != nil {
		return nil, err
	}

	nodes, err := config.ValidatorSetRetriever(pChainReference.SealingBlockInfo().PChainHeight)
	if err != nil {
		return nil, err
	}

	lastAccepted := newEpochMetadata(lastBlockEpoch, nodes, config.SignatureAggregatorCreator, lastBlock.SealingBlockInfo())
	epochs := make(map[uint64]epochMetadata)
	epochs[lastBlockEpoch] = *lastAccepted

	scheduler := simplex.NewScheduler(config.Logger, simplex.DefaultProcessingBlocks)

	return &NonValidator{
		Config:              config,
		incompleteSequences: make(map[uint64]*finalizedSeq),
		ctx:                 ctx,
		cancelCtx:           cancelFunc,
		epochs:              epochs,
		verifier:            simplex.NewBlockVerificationScheduler(config.Logger, config.MaxDependencies, scheduler),
	}, nil
}

func (n *NonValidator) Start() {
	n.broadcastLatestEpoch()
}

func (n *NonValidator) Stop() {
	n.cancelCtx()
}

// TODO: Broadcast the last known epoch to bootstrap the node. Collect responses marking the latest sealing block.
// Keep rebroadcasting requests for that sealing block until we have enough responses.
func (n *NonValidator) broadcastLatestEpoch() {

}

// this function should be ran under a lock?
func (n *NonValidator) HandleMessage(msg *simplex.Message, from simplex.NodeID) error {
	// A closed context means we have shut down.
	select {
	case <-n.ctx.Done():
		return nil
	default:
	}

	if n.haltedError != nil {
		return n.haltedError
	}

	switch {
	case msg.BlockDigestRequest != nil:
		// TODO: it seems reasonable for our non-validator to be able to process these messages and send out responses.
		return nil
	// TODO: create a test for sending a block message but a nil block
	case msg.BlockMessage != nil && msg.BlockMessage.Block != nil:
		return n.handleBlock(msg.BlockMessage.Block, from)
	case msg.Finalization != nil && msg.Finalization != nil:
		return n.handleFinalization(msg.Finalization, from)
	default:
		n.Logger.Debug("Received unexpected message", zap.Any("Message", msg), zap.Stringer("from", from))
		return nil
	}
}

// handleBlock handles a block message. BlockMessages are sent when the leader proposes a block for its round.
// We only process blocks if they are from the leader and for the current epoch.
// Otherwise, we wait to process blocks until we receive a finalization.
func (n *NonValidator) handleBlock(block simplex.Block, from simplex.NodeID) error {
	bh := block.BlockHeader()

	if bh.Seq > n.MaxRoundWindow+n.Storage.NumBlocks() {
		n.Logger.Debug("Received a block from a sequence too far ahead", zap.Uint64("Num Blocks", n.Storage.NumBlocks()), zap.Uint64("Block Sequence", bh.Seq), zap.Stringer("From", from))
		return nil
	}

	epoch, ok := n.epochs[bh.Epoch]
	if !ok {
		n.Logger.Debug("Received a block from an epoch we do not have", zap.Uint64("Epoch", bh.Epoch), zap.Stringer("From", from))
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

	finalizedBlockTask := n.createFinalizedBlockVerificationTask(block, incomplete.finalization)

	if bh.Seq == 0 || n.isAccepted(bh.Seq-1) {
		return n.verifier.ScheduleTaskWithDependencies(finalizedBlockTask, bh.Seq, nil, []uint64{})
	}

	return n.verifier.ScheduleTaskWithDependencies(finalizedBlockTask, bh.Seq, &(bh.Prev), []uint64{})
}

func (n *NonValidator) isAccepted(seq uint64) bool {
	return n.Storage.NumBlocks() > seq
}

func (n *NonValidator) createFinalizedBlockVerificationTask(block simplex.Block, finalization *simplex.Finalization) func() simplex.Digest {
	return func() simplex.Digest {
		md := block.BlockHeader()
		n.Logger.Debug("Block verification started", zap.Uint64("round", md.Round))
		start := time.Now()
		defer func() {
			elapsed := time.Since(start)
			n.Logger.Debug("Block verification ended", zap.Uint64("round", md.Round), zap.Duration("elapsed", elapsed))
		}()

		verifiedBlock, err := block.Verify(n.ctx)

		// is this block a sealing block? set epochs
		if sealingInfo := verifiedBlock.SealingBlockInfo(); sealingInfo != nil {
			nodes, err := n.ValidatorSetRetriever(sealingInfo.PChainHeight)
			if err != nil {
				n.haltedError = err
				return md.Digest
			}

			n.epochs[sealingInfo.Epoch] = *newEpochMetadata(sealingInfo.Epoch, nodes, n.SignatureAggregatorCreator, sealingInfo)
		}

		// We have failed verifying a finalized block
		if err != nil {
			n.Logger.Info("Failed verifying a block that has a finalization", zap.Uint64("Block Seq", md.Seq), zap.Stringer("Block Digest", md.Digest), zap.Error(err))
			return md.Digest
		}

		if err := n.Storage.Index(n.ctx, verifiedBlock, *finalization); err != nil {
			n.haltedError = err
			n.Logger.Info("Failed indexing a block and finalization", zap.Uint64("Block Seq", md.Seq), zap.Stringer("Block Digest", md.Digest), zap.Error(err))
			return md.Digest
		}

		n.removeIncompleteSeqs(md.Seq)

		return md.Digest
	}
}

func (n *NonValidator) removeIncompleteSeqs(startSeq uint64) {
	for seq, _ := range n.incompleteSequences {
		if seq <= startSeq {
			delete(n.incompleteSequences, seq)
		}
	}
}

// handleFinalization process a finalization message. If its for a future epoch, it will forward the finalization
// to the replication handler.
func (n *NonValidator) handleFinalization(finalization *simplex.Finalization, from simplex.NodeID) error {
	bh := finalization.Finalization.BlockHeader

	if n.isAccepted(bh.Seq) {
		n.Logger.Debug("Received a stale finalization", zap.Uint64("Seq", bh.Seq), zap.Stringer("From", from))
		return nil
	}

	epoch, ok := n.epochs[bh.Epoch]
	if !ok {
		// This finalization is after our lastAcceptedEpoch and is for an unknown Epoch, begin replication
		n.Logger.Debug("Received a finalization from an unknown epoch", zap.Uint64("Unknown Epoch", bh.Epoch), zap.Stringer("From", from))
		return nil
	}

	if err := simplex.VerifyQC(finalization.QC, n.Logger, "Finalization", epoch.signatureAggregator.IsQuorum, epoch.nodeLookup, finalization, from); err != nil {
		n.Logger.Debug("Received an invalid finalization",
			zap.Int("round", int(bh.Round)),
			zap.Stringer("NodeID", from))
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
		return nil
	}

	incomplete.finalization = finalization

	// No block received yet for this sequence.
	if incomplete.block == nil {
		// TODO: notify replication
		return nil
	}

	digest := incomplete.block.BlockHeader().Digest
	if !bytes.Equal(bh.Digest[:], digest[:]) {
		// TODO: this means the leader has equivocated and sent us a wrong block while another has been finalized.
		// We should probably handle replication for this block?
		n.Logger.Info(
			"Received a block from the leader of a round whose digest mismatches the finalization",
			zap.Stringer("Finalization Digest", bh.Digest),
			zap.Stringer("Block digest", digest),
			zap.Stringer("From", from),
		)

		// TODO: replication here as well
		return nil
	}

	finalizedBlockTask := n.createFinalizedBlockVerificationTask(incomplete.block, incomplete.finalization)

	if bh.Seq == 0 || n.isAccepted(bh.Seq-1) {
		return n.verifier.ScheduleTaskWithDependencies(finalizedBlockTask, bh.Seq, nil, []uint64{})
	}

	return n.verifier.ScheduleTaskWithDependencies(finalizedBlockTask, bh.Seq, &(bh.Prev), []uint64{})
}

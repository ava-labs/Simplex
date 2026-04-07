package nonvalidator

import (
	"bytes"
	"context"

	"github.com/ava-labs/simplex"
	"go.uber.org/zap"
)

type epochMetadata struct {
	validators []simplex.NodeID // the validators of this epoch
	epoch      uint64
}

type Config struct {
	Logger            simplex.Logger
	Storage           simplex.FullStorage
	GenesisValidators []simplex.NodeID
}

type NonValidator struct {
	Config

	ctx       context.Context
	cancelCtx context.CancelFunc
	verifier  *Verifier

	// incompleteSequences stores sequences that we have not collected
	// both a block and finalization for. Once both have been received, they are indexed & verified.
	// TODO: garbage collect old sequences
	incompleteSequences map[uint64]*finalizedSeq

	// the most recent epoch we have verified
	latestVerifiedEpoch *epochMetadata
}

// NewNonValidator creates a NonValidator with the given `config` and `lastVerifiedBlock`.
func NewNonValidator(config Config, lastVerifiedBlock simplex.Block) *NonValidator {
	ctx, cancelFunc := context.WithCancel(context.Background())
	verifier := NewVerifier(ctx, config.Logger, lastVerifiedBlock, config.Storage)

	latestEpoch := &epochMetadata{
		epoch:      0,
		validators: config.GenesisValidators,
	}

	return &NonValidator{
		Config:              config,
		incompleteSequences: make(map[uint64]*finalizedSeq),
		ctx:                 ctx,
		cancelCtx:           cancelFunc,
		verifier:            verifier,
		latestVerifiedEpoch: latestEpoch,
	}
}

func (n *NonValidator) Start() {
	n.broadcastLatestEpoch()
}

func (n *NonValidator) Stop() {
	n.cancelCtx()
}

// TODO: Broadcast the last known epoch to bootstrap the node. Collect responses marking the latest sealing block.
// Keep rebroadcasting requests for that sealing block until we have enough responses.
func (n *NonValidator) broadcastLatestEpoch() {}

// skipMessage returns whether `msg` should be processed by a non-validator.
func skipMessage(msg *simplex.Message) bool {
	switch {
	case msg == nil:
		return true
	case msg.EmptyNotarization != nil:
		return true
	case msg.VoteMessage != nil:
		return true
	case msg.EmptyVoteMessage != nil:
		return true
	case msg.Notarization != nil:
		return true
	// TODO: handle replication messages.
	// We can reuse these message types from Simplex and only care about QuorumRounds for finalized sequences.
	case msg.ReplicationResponse != nil:
		return true
	case msg.ReplicationRequest != nil:
		return true
	case msg.FinalizeVote != nil:
		return true
	default:
		return false
	}
}

func (n *NonValidator) HandleMessage(msg *simplex.Message, from simplex.NodeID) error {
	select {
	case <-n.ctx.Done():
		return nil
	default:
	}

	switch {
	case skipMessage(msg):
		return nil
	case msg.BlockDigestRequest != nil:
		// TODO: it seems reasonable for our non-validator to be able to process these messages and send out responses.
		return nil
	case msg.BlockMessage != nil:
		// convert to full block
		block, ok := msg.BlockMessage.Block.(simplex.FullBlock)
		if !ok {
			n.Logger.Debug("Unable to convert block message to full block", zap.Stringer("Digest", msg.BlockMessage.Block.BlockHeader().Digest), zap.Stringer("from", from))
			return nil
		}
		return n.handleBlock(block, from)
	case msg.Finalization != nil:
		return n.handleFinalization(msg.Finalization, from)
	default:
		n.Logger.Debug("Received unexpected message", zap.Any("Message", msg), zap.Stringer("from", from))
		return nil
	}
}

// handleBlock handles a block message.
func (n *NonValidator) handleBlock(block simplex.FullBlock, from simplex.NodeID) error {
	bh := block.BlockHeader()

	if n.latestVerifiedEpoch.epoch != bh.Epoch {
		n.Logger.Debug("Received a block from a different epoch", zap.Uint64("Current epoch", n.latestVerifiedEpoch.epoch), zap.Uint64("Block Epoch", bh.Epoch), zap.Stringer("From", from))
		return nil
	}

	if !bytes.Equal(simplex.LeaderForRound(n.latestVerifiedEpoch.validators, bh.Round), from) {
		n.Logger.Debug("Received a block not from the leader", zap.Uint64("Epoch", bh.Epoch), zap.Stringer("From", from))
		return nil
	}

	// If we have already verified the block discard it
	if n.verifier.alreadyVerifiedSeq(bh.Seq) {
		n.Logger.Debug("Received a block from an older round")
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

	// index and verify block
	return n.indexBlock(block, incomplete.finalization)
}

// indexBlock indexes the block into storage and attempts to trigger verification after.
func (n *NonValidator) indexBlock(block simplex.FullBlock, finalization *simplex.Finalization) error {
	if block == nil || finalization == nil {
		return nil
	}

	if err := n.Storage.Index(n.ctx, block, *finalization); err != nil {
		return err
	}

	return n.verifier.triggerVerify(block)
}

// handleFinalization process a finalization message. If its for a future epoch, it will forward the finalization
// to the replication handler.
func (n *NonValidator) handleFinalization(finalization *simplex.Finalization, from simplex.NodeID) error {
	bh := finalization.Finalization.BlockHeader

	if bh.Epoch < n.latestVerifiedEpoch.epoch {
		n.Logger.Debug("Received a finalization from an earlier epoch", zap.Uint64("Current epoch", n.latestVerifiedEpoch.epoch), zap.Uint64("Epoch", bh.Epoch), zap.Stringer("From", from))
		return nil
	}

	// verify the finalization
	if err := finalization.Verify(); err != nil {
		n.Logger.Debug("Received a finalization that failed verification", zap.Stringer("From", from), zap.Error(err))
		return nil
	}

	// TODO: forward to replication.
	if bh.Epoch > n.latestVerifiedEpoch.epoch {
		// we may need to begin replication on a future epoch, or notify the replicator a future finalization has arrived.
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
		}
		return nil
	}

	// No block received yet for this sequence.
	if incomplete.block == nil {
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
		return nil
	}

	return n.indexBlock(incomplete.block, finalization)
}

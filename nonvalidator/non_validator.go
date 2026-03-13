package nonvalidator

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ava-labs/simplex"
	"go.uber.org/zap"
)

type finalizedSeq struct {
	block        simplex.FullBlock
	finalization *simplex.Finalization
}

func (f *finalizedSeq) String() string {
	seq := uint64(0)
	digest := simplex.Digest{}
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

type epochInfo struct {
	validators []simplex.NodeID
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
	verifier  Verifiier

	incompleteSeqs map[simplex.Digest]*finalizedSeq

	// the most recent epoch that we have
	latestEpoch *epochInfo
}

func NewNonValidator(config Config, lastVerifiedBlock simplex.Block) *NonValidator {
	ctx, cancelFunc := context.WithCancel(context.Background())
	verifier := NewVerifier(config.Logger, lastVerifiedBlock, config.Storage)

	latestEpoch := &epochInfo{
		epoch:      0,
		validators: config.GenesisValidators,
	}

	return &NonValidator{
		Config:         config,
		incompleteSeqs: make(map[simplex.Digest]*finalizedSeq, 0),
		ctx:            ctx,
		cancelCtx:      cancelFunc,
		verifier:       *verifier,
		latestEpoch:    latestEpoch,
	}
}

func (n *NonValidator) Start() {
	n.broadcastLatestEpoch()
}

// TODO: Broadcast the last known epoch to bootstrap the node. Collect responses marking the latest seal block
// Keep rebroadcasting requests for that sealing block until we have enough responses to verify.
func (n *NonValidator) broadcastLatestEpoch() {

}

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
	// TODO: I think we want to handle replication messages.
	// We can just reuse the message types from simplex and only care about sequences not rounds.
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
	switch {
	case skipMessage(msg):
		return nil
	case msg.BlockDigestRequest != nil:
		// TODO: send out request to help the network
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
	}

	return nil
}

// handleBlock handles a block message.
func (n *NonValidator) handleBlock(block simplex.FullBlock, from simplex.NodeID) error {
	bh := block.BlockHeader()

	if n.latestEpoch.epoch != bh.Epoch {
		n.Logger.Debug("Received a block not from the current epoch", zap.Uint64("Current epoch", n.latestEpoch.epoch), zap.Uint64("Epoch", bh.Epoch), zap.Stringer("From", from))
	}

	// TODO: do we need this check for non-validators? I added because otherwise we would need to store all blocks received until we receive a finalization
	// but this could be a dos since a malicious node can spam us with bad blocks.
	if !bytes.Equal(simplex.LeaderForRound(n.latestEpoch.validators, bh.Round), from) {
		n.Logger.Debug("Received a block not from the leader", zap.Uint64("Epoch", bh.Epoch), zap.Stringer("From", from))
		return nil
	}

	// If we have already verified the block discard it
	if n.verifier.alreadyVerifiedSeq(bh.Seq) {
		n.Logger.Debug("Received a block from an older round")
		return nil
	}

	// If we have already received this block
	incomplete, ok := n.incompleteSeqs[bh.Digest]
	if !ok {
		// we have not received anything for this digest
		incompleteSeq := &finalizedSeq{
			block: block,
		}
		n.incompleteSeqs[bh.Digest] = incompleteSeq
		n.Logger.Debug("Stored incomplete sequence", zap.Stringer("Stored", incompleteSeq))
		return nil
	}

	// Already received this block
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

func (n *NonValidator) indexBlock(block simplex.FullBlock, finalization *simplex.Finalization) error {
	if block == nil || finalization == nil {
		return nil
	}

	if err := n.Storage.Index(n.ctx, block, *finalization); err != nil {
		return err
	}

	return n.verifier.triggerVerify(block)
}

func (n *NonValidator) handleFinalization(finalization *simplex.Finalization, from simplex.NodeID) error {
	bh := finalization.Finalization.BlockHeader

	if bh.Epoch < n.latestEpoch.epoch {
		n.Logger.Debug("Received a finalization from an earlier epoch", zap.Uint64("Current epoch", n.latestEpoch.epoch), zap.Uint64("Epoch", bh.Epoch), zap.Stringer("From", from))
		// TODO: we can still probably process it?
	}

	if bh.Epoch > n.latestEpoch.epoch {
		// TODO: begin replication
		// if message is from n.checkpointEpoch or less, call index block and mark the message as received to the replicator
		// otherwise notify replicator we may need to replicate a future epoch.
	}

	// If we have already received this block
	incomplete, ok := n.incompleteSeqs[bh.Digest]
	if !ok {
		// we have not received anything for this digest
		incompleteSeq := &finalizedSeq{
			finalization: finalization,
		}
		n.incompleteSeqs[bh.Digest] = incompleteSeq
		n.Logger.Debug("Stored incomplete sequence", zap.Stringer("Stored", incompleteSeq))
		return nil
	}

	// Already received this block
	if incomplete.block == nil || incomplete.finalization != nil {
		return nil
	}

	digest := incomplete.block.BlockHeader().Digest
	if !bytes.Equal(bh.Digest[:], digest[:]) {
		// TODO We probably need to start replication, for this block?
		n.Logger.Info(
			"Received a block from the leader of a round whose digest mismatches the finalization",
			zap.Stringer("Finalization Digest", incomplete.finalization.Finalization.Digest),
			zap.Stringer("Block digest", bh.Digest),
			zap.Stringer("From", from),
		)
		return nil
	}

	return n.indexBlock(incomplete.block, finalization)
}

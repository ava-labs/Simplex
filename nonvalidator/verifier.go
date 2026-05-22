package nonvalidator

import (
	"context"
	"errors"

	"github.com/ava-labs/simplex"
)

var errVerificationFailed = errors.New("Error Verification")

// n.Logger.Debug("Block verification started", zap.Uint64("round", md.Round))
// 	start := time.Now()
// 	defer func() {
// 		elapsed := time.Since(start)
// 		n.Logger.Debug("Block verification ended", zap.Uint64("round", md.Round), zap.Duration("elapsed", elapsed))
// 	}()
// n.Logger.Info("Failed verifying a block that has a finalization", zap.Uint64("Block Seq", md.Seq), zap.Stringer("Block Digest", md.Digest), zap.Error(err))
// n.Logger.Info("Failed indexing a block and finalization", zap.Uint64("Block Seq", md.Seq), zap.Stringer("Block Digest", md.Digest), zap.Error(err))

// a finalizedBlockVerificationTask should only get executed when `block` is next to be indexed.

// is this block a sealing block? set epochs
// if sealingInfo := verifiedBlock.SealingBlockInfo(); sealingInfo != nil {
// 	n.epochs[sealingInfo.Epoch] = newEpochMetadata(sealingInfo, n.SignatureAggregatorCreator)

// 	// we can delete the old epoch from the map
// 	delete(n.epochs, verifiedBlock.BlockHeader().Epoch)
// }

// n.removeOldIncompleteSeqs(md.Seq)

func verifyAndIndex(ctx context.Context, storage simplex.Storage, block simplex.Block, finalization *simplex.Finalization) error {
	// TODO: defensive check if block is not next seq to commit
	verifiedBlock, err := block.Verify(ctx)
	// We have failed verifying a finalized block
	if err != nil {
		// TODO: notify replication
		return errVerificationFailed
	}

	return storage.Index(ctx, verifiedBlock, *finalization)
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"github.com/ava-labs/simplex/common"
	"github.com/ava-labs/simplex/simplex"
	"go.uber.org/zap"
)

// FutureEpochListener detects that our epoch is stale, meaning a sealing block we have
// not indexed created an epoch after ours. Finalizations and blocks from higher epochs
// only name the epoch, so the listener requests the block sealing it. Sealing blocks
// arriving in replication responses are fed to a threshold collector, and once enough
// validators report the same one, it is handed to onSealingBlock.
type FutureEpochListener struct {
	logger common.Logger
	sender simplex.Sender

	// collector counts sealing blocks per epoch until a threshold of validators report the same one.
	collector *epochDigestCounter

	// onSealingBlock receives the quorum round of a sealing block creating an epoch after ours.
	onSealingBlock func(qr *common.QuorumRound)
}

// NewFutureEpochListener creates a listener that requests sealing blocks through sender.
// The threshold of responses is derived from latestValidators, which returns the latest
// validator set rather than the validator set of our epoch.
func NewFutureEpochListener(logger common.Logger, sender simplex.Sender, latestValidators LatestValidatorSetRetriever, onSealingBlock func(qr *common.QuorumRound)) *FutureEpochListener {
	return &FutureEpochListener{
		logger:         logger,
		sender:         sender,
		collector:      newEpochReplicator(logger, latestValidators),
		onSealingBlock: onSealingBlock,
	}
}

// HandleMessage inspects msg for evidence of an epoch after currentEpoch.
// It will keep track of sealing block responses of future epochs by the most recent validator set, to ensure a node is not behind.
func (f *FutureEpochListener) HandleMessage(msg *common.Message, from common.NodeID, currentEpoch uint64) {
	// The highest epoch this message tells us exists but we have no sealing block for.
	var highestEpoch uint64

	switch {
	case msg.ReplicationResponse != nil:
		resp := msg.ReplicationResponse
		qrs := make([]*common.QuorumRound, 0, len(resp.Data)+2)
		for j := range resp.Data {
			qrs = append(qrs, &resp.Data[j])
		}
		qrs = append(qrs, resp.LatestSeq, resp.LatestRound)

		for _, qr := range qrs {
			if qr == nil || qr.Block == nil {
				continue
			}

			bh := qr.Block.BlockHeader()
			sealingInfo := qr.Block.SealingBlockInfo()
			if sealingInfo == nil {
				// Not a sealing block, but it tells us an epoch after ours exists.
				if bh.Epoch > highestEpoch {
					highestEpoch = bh.Epoch
				}
				continue
			}

			// The sequence of a sealing block is the epoch it creates.
			if bh.Seq <= currentEpoch {
				continue
			}

			if !f.collector.collectedSealingBlockInfo(sealingInfo, bh, from) {
				continue
			}

			f.logger.Info("Received a threshold of sealing blocks for an epoch after ours",
				zap.Uint64("Our Epoch", currentEpoch),
				zap.Uint64("Sealed Epoch", bh.Seq),
				zap.Stringer("From", from),
			)
			f.onSealingBlock(qr)
		}
	case msg.Finalization != nil:
		highestEpoch = msg.Finalization.Finalization.BlockHeader.Epoch
	case msg.BlockMessage != nil && msg.BlockMessage.Block != nil:
		highestEpoch = msg.BlockMessage.Block.BlockHeader().Epoch
	}

	// We received a message that contains a higher epoch than ours, we should try and request that epochs sealing block.
	if highestEpoch > currentEpoch {
		f.requestSealingBlock(highestEpoch, from)
	}
}

// RemoveOldEpochs drops collected sealing blocks for epochs before minEpochToKeep.
func (f *FutureEpochListener) RemoveOldEpochs(minEpochToKeep uint64) {
	f.collector.removeOldEpochs(minEpochToKeep)
}

// requestSealingBlock sends a sealing block request for epoch.
// is the epoch it creates.
func (f *FutureEpochListener) requestSealingBlock(epoch uint64, to common.NodeID) {
	f.logger.Debug("Requesting the block sealing a future epoch", zap.Uint64("Epoch", epoch), zap.Stringer("To", to))

	f.sender.Send(&common.Message{
		ReplicationRequest: &common.ReplicationRequest{
			Seqs: []uint64{epoch},
		},
	}, to)
}

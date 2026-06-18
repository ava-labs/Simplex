// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"github.com/ava-labs/simplex/common"
	"go.uber.org/zap"
)

type latestValidatorSetRetriever interface {
	Nodes() common.Nodes
}

type epochReplicator struct {
	logger common.Logger

	// receivedSealingBlocks stores sealing blocks that have been received by our non-validator.
	// It maps the epoch they are creating, to the NodeIds that have sent them to us(and which digest they sent).
	// Once we have collected f+1 messages for a finalization, the sealing block for that epoch is validated.
	sealingBlockResponses map[uint64]map[string]common.Digest

	// latestValidatorSetRetriever is used to calculate the threshold of votes needed to validate an epoch
	latestValidatorSetRetriever latestValidatorSetRetriever
}

func newEpochReplicator(logger common.Logger, validatorSetRetriever latestValidatorSetRetriever) *epochReplicator {
	return &epochReplicator{
		sealingBlockResponses:       make(map[uint64]map[string]common.Digest),
		logger:                      logger,
		latestValidatorSetRetriever: validatorSetRetriever,
	}
}

// collectedQuorumRound notes that an quorum round for an unknown epoch was received. If that quorum round
// was a sealing block we mark it and return if a threshold of responses was reached. otherwise, return false.
func (e *epochReplicator) collectedQuorumRound(qr *common.QuorumRound, from common.NodeID) bool {
	if qr.Block.SealingBlockInfo() == nil {
		return false
	}
	e.logger.Debug("Collected a quorum round with a sealing block", zap.Stringer("QR", qr), zap.Stringer("From", from))

	sealingInfo := qr.Block.SealingBlockInfo()
	threshold := common.F(len(e.latestValidatorSetRetriever.Nodes())) + 1
	newEpoch := qr.Block.BlockHeader().Seq
	epochResponses, ok := e.sealingBlockResponses[newEpoch]
	digest := qr.Block.BlockHeader().Digest
	if !ok {
		epochResponses = make(map[string]common.Digest)
		epochResponses[string(from)] = digest
		e.sealingBlockResponses[newEpoch] = epochResponses
	}
	epochResponses[string(from)] = digest

	// check if we have a threshold of responses
	counts := make(map[common.Digest]uint64)
	for _, digest := range epochResponses {
		count := counts[digest]
		count++
		counts[digest] = count

		if counts[digest] >= uint64(threshold) {
			e.logger.Info("We received enough messages to validate a higher epoch", zap.Stringer("EpochInfo", sealingInfo), zap.Int("Threshold", threshold), zap.Uint64("Responses", counts[digest]))
			return true
		}
	}

	return false
}

func (e *epochReplicator) removeOldEpochs(minEpochToKeep uint64) {
	for epoch := range e.sealingBlockResponses {
		if epoch < minEpochToKeep {
			delete(e.sealingBlockResponses, epoch)
		}
	}
}

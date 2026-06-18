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

// TODO: garbage collect map
type epochReplicator struct {
	logger common.Logger

	// receivedSealingBlocks stores sealing blocks that have been received by our non-validator.
	// It maps the epoch they are creating, to the NodeIds that have sent them to us(and which digest they sent).
	// Once we have collected f+1 messages for a finalization, the sealing block for that epoch is validated.
	sealingBlockResponses map[uint64]map[string]common.Digest

	// digests stores the metadata associated with a sealing block.
	digests map[string]*common.QuorumRound

	// latestValidatorSetRetriever is used to calculate the threshold of votes needed to validate an epoch
	latestValidatorSetRetriever latestValidatorSetRetriever
}

func newEpochReplicator(logger common.Logger, validatorSetRetriever latestValidatorSetRetriever) *epochReplicator {
	return &epochReplicator{
		digests:                     make(map[string]*common.QuorumRound),
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
	epochResponses, ok := e.sealingBlockResponses[sealingInfo.Epoch]
	digest := qr.Block.BlockHeader().Digest
	if !ok {
		epochResponses = make(map[string]common.Digest)
		epochResponses[string(from)] = digest
		e.sealingBlockResponses[sealingInfo.Epoch] = epochResponses
	}
	epochResponses[string(from)] = digest

	e.digests[digest.String()] = qr

	// check if we have a threshold of responses
	counts := make(map[string]uint64)
	for _, digest := range epochResponses {
		sDigest := digest.String()
		_, ok := counts[sDigest]
		if !ok {
			counts[sDigest] = 1
		} else {
			counts[sDigest] += 1
		}

		if counts[sDigest] >= uint64(threshold) {
			e.logger.Info("We received enough messages to validate a higher epoch", zap.Stringer("EpochInfo", sealingInfo), zap.Int("Threshold", threshold), zap.Uint64("Responses", counts[sDigest]))
			return true
		}
	}

	return false
}

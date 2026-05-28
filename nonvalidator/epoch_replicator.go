// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"github.com/ava-labs/simplex"
	"go.uber.org/zap"
)

type epochReplicator struct {
	logger simplex.Logger

	// receivedSealingBlocks stores sealing blocks that have been received by our non-validator.
	// It maps the epoch they are creating to the NodeIds that have sent them to us(and which digest they have voted for).
	// Essentially this keeps a track of nodes that have confirmed a finalization for this sealing block.
	// Once we have collected f+1 votes, the sealing block for that epoch is confirmed
	// we need sealing blocks because they have the validator set
	sealingBlockResponses map[uint64]map[string]simplex.Digest

	// stores the metadata associated with a sealing block.
	// Sealing block digest -> quorum round allows us to store potentially multiple responses for
	// a sealing block. This is possible because an adversary can forge a fake finalization for a sealing block(for an epoch we do not have).
	// TODO: Create a test for the scenario above ^
	digests map[string]*simplex.QuorumRound

	comm simplex.Communication

	// threshold is the amount of responses that are required to validate a sealing block.
	// typically this is f + 1
	threshold uint64
}

func newEpochReplicator(logger simplex.Logger, comm simplex.Communication, threshold uint64) *epochReplicator {
	return &epochReplicator{
		comm:                  comm,
		threshold:             threshold,
		digests:               make(map[string]*simplex.QuorumRound),
		sealingBlockResponses: make(map[uint64]map[string]simplex.Digest),
		logger:                logger,
	}
}

// collectedQuorumRound notes that an quorum round for an unknown epoch was received. If that quorum round
// was a sealing block we mark it and return if a threshold of responses was reached. otherwise, return false.
func (e *epochReplicator) collectedQuorumRound(qr *simplex.QuorumRound, from simplex.NodeID) bool {
	reachedThreshold := false
	if qr.Block.SealingBlockInfo() != nil {
		reachedThreshold = e.handleSealingQuorumRound(qr, from)
	}

	// TODO: say we request a sealing block and get a response, we will then broadcast again. maybe wait a couple seconds before
	// broadcasting so we don't oversend digest requests

	// Because we have not verified a finalization for this sequence, we should not add it to the replicator.
	// We will simply broadcast a request to collect this block

	digestRequest := simplex.BlockDigestRequest{
		Seq:    qr.Block.BlockHeader().Epoch,
		Digest: simplex.Digest{}, // TODO: change how we process digests to allow empty digests to mean "send us back any"
	}
	e.comm.Broadcast(&simplex.Message{
		BlockDigestRequest: &digestRequest,
	})

	e.logger.Debug("Broadcasting sealing block requests", zap.Uint64("Seq", qr.Block.BlockHeader().Epoch))
	return reachedThreshold
}

func (e *epochReplicator) handleSealingQuorumRound(qr *simplex.QuorumRound, from simplex.NodeID) bool {
	sealingInfo := qr.Block.SealingBlockInfo()

	epochResponses, ok := e.sealingBlockResponses[sealingInfo.Epoch]
	digest := qr.Block.BlockHeader().Digest
	if !ok {
		digests := make(map[string]simplex.Digest)
		digests[from.String()] = digest
		e.sealingBlockResponses[sealingInfo.Epoch] = digests
	} else {
		epochResponses[string(from)] = digest
	}

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

		if counts[sDigest] >= e.threshold {
			e.logger.Info("We received enough messages to validate a higher epoch", zap.Stringer("EpochInfo", sealingInfo), zap.Uint64("Threshold", e.threshold), zap.Uint64("Responses", counts[sDigest]))
			return true
		}
	}

	return false
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"bytes"
	"errors"

	"github.com/ava-labs/simplex/common"
	"go.uber.org/zap"
)

var (
	errNoGenesis          = errors.New("no genesis found")
	errMissingSealingInfo = errors.New("no sealing block info for sealing block")
)

type epochMetadata struct {
	nodes                common.Nodes
	eligibleSigners      map[string]struct{}
	epoch                uint64
	signatureAggregator  common.SignatureAggregator
	prevSealingBlockHash common.Digest
}

type epochs map[uint64]*epochMetadata

func newEpochMetadata(epoch uint64, sealingMetadata *common.SealingBlockInfo, sigCreator common.SignatureAggregatorCreator) *epochMetadata {
	if sealingMetadata == nil {
		return nil
	}

	nodes := sealingMetadata.ValidatorSet
	lookup := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		lookup[string(node.Id)] = struct{}{}
	}

	return &epochMetadata{
		nodes:                nodes,
		eligibleSigners:      lookup,
		epoch:                epoch,
		signatureAggregator:  sigCreator(nodes),
		prevSealingBlockHash: sealingMetadata.PrevSealingBlockHash,
	}
}

// newEpochs creates a mapping of epoch numbers -> epoch metadata. The epoch metadata is used for verifying
// blocks and finalizations, and should only contain epochMetadata that we have validated.
func newEpochs(storage common.Storage, sigAggCreator common.SignatureAggregatorCreator) (epochs, error) {
	lastBlockHeight := storage.NumBlocks()
	if lastBlockHeight == 0 {
		return nil, errNoGenesis
	}

	lastBlock, _, err := storage.Retrieve(lastBlockHeight - 1)
	if err != nil {
		return nil, err
	}

	epochs := make(map[uint64]*epochMetadata)

	// A zero Epoch means this is before the first ever Simplex block(ex. Genesis or Last Snowman Block)
	if lastBlock.BlockHeader().Epoch == 0 {
		return epochs, nil
	}

	var sealingBlock common.VerifiedBlock
	if lastBlock.SealingBlockInfo() == nil {
		sealingBlock, _, err = storage.Retrieve(lastBlock.BlockHeader().Epoch)
		if err != nil {
			return nil, err
		}
		if sealingBlock.SealingBlockInfo() == nil {
			return nil, errMissingSealingInfo
		}
	} else {
		sealingBlock = lastBlock
	}

	lastAcceptedEpoch := newEpochMetadata(sealingBlock.BlockHeader().Seq, sealingBlock.SealingBlockInfo(), sigAggCreator)
	epochs[lastAcceptedEpoch.epoch] = lastAcceptedEpoch
	return epochs, nil
}

func (e epochs) highestEpoch() (uint64, common.Nodes) {
	highest := uint64(0)
	nodes := []common.Node{}
	for epoch, info := range e {
		if epoch > highest {
			highest = epoch
			nodes = info.nodes
		}
	}

	return highest, nodes
}

// removeOldEpochs deletes all epochs strictly less than startEpoch.
func (e epochs) removeOldEpochs(minEpochToKeep uint64) {
	for epoch := range e {
		if epoch < minEpochToKeep {
			delete(e, epoch)
		}
	}
}

func (e epochs) canValidate(block common.Block) bool {
	if block.SealingBlockInfo() == nil {
		return false
	}

	// the sequence number is the next proposed epoch
	_, ok := e[block.BlockHeader().Seq]
	if ok {
		// cannot validate twice
		return false
	}

	digest := block.BlockHeader().Digest
	for _, md := range e {
		if bytes.Equal(md.prevSealingBlockHash[:], digest[:]) {
			// We have validated the next epoch, and the next epoch has a backward pointer to this one
			return true
		}
	}
	return false
}

// latestValidatorSetRetriever is an allows the epoch replicator to get the latest validator set.
// This is used to calculate the threshold of votes needed to validate an epoch.
type latestValidatorSetRetriever interface {
	Validators() common.Nodes
}

// epochDigestCounter counts sealing block responses from validators for each epoch.
// It uses latestValidatorSetRetriever to determine when the required response threshold
// has been reached.
type epochDigestCounter struct {
	logger common.Logger

	// sealingBlockResponses stores sealing blocks that have been received by our non-validator.
	// It maps the epoch they are creating, to the NodeIds that have sent them to us(and which digest they sent).
	// Once we have collected f+1 messages for a finalization, the sealing block for that epoch is validated.
	sealingBlockResponses map[uint64]map[string]common.Digest

	// latestValidatorSetRetriever is used to calculate the threshold of votes needed to validate an epoch
	latestValidatorSetRetriever latestValidatorSetRetriever
}

func newEpochReplicator(logger common.Logger, validatorSetRetriever latestValidatorSetRetriever) *epochDigestCounter {
	return &epochDigestCounter{
		sealingBlockResponses:       make(map[uint64]map[string]common.Digest),
		logger:                      logger,
		latestValidatorSetRetriever: validatorSetRetriever,
	}
}

// collectedSealingBlockInfo records a sealing block response for an unknown epoch
// and returns true once a threshold of matching responses has been collected for
// that epoch. Nil sealingBlockInfo values are ignored and return false.
func (e *epochDigestCounter) collectedSealingBlockInfo(sealingBlockInfo *common.SealingBlockInfo, bh common.BlockHeader, from common.NodeID) bool {
	if sealingBlockInfo == nil {
		return false
	}
	e.logger.Debug("Collected a sealing block", zap.Stringer("QR", sealingBlockInfo), zap.Stringer("From", from))

	threshold := common.F(len(e.latestValidatorSetRetriever.Validators())) + 1
	newEpoch := bh.Seq
	epochResponses, ok := e.sealingBlockResponses[newEpoch]
	digest := bh.Digest
	if !ok {
		epochResponses = make(map[string]common.Digest)
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
			e.logger.Info("We received enough messages to validate a higher epoch", zap.Stringer("EpochInfo", sealingBlockInfo), zap.Int("Threshold", threshold), zap.Uint64("Responses", counts[digest]))
			return true
		}
	}

	return false
}

func (e *epochDigestCounter) removeOldEpochs(minEpochToKeep uint64) {
	for epoch := range e.sealingBlockResponses {
		if epoch < minEpochToKeep {
			delete(e.sealingBlockResponses, epoch)
		}
	}
}

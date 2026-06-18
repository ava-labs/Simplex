// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"bytes"
	"errors"

	"github.com/ava-labs/simplex/common"
)

var (
	errNoGenesis          = errors.New("No Genesis Found")
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

func newEpochMetadata(sealingMetadata *common.SealingBlockInfo, sigCreator common.SignatureAggregatorCreator) *epochMetadata {
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
		epoch:                sealingMetadata.Epoch,
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

	lastAcceptedEpoch := newEpochMetadata(sealingBlock.SealingBlockInfo(), sigAggCreator)
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

	_, ok := e[block.SealingBlockInfo().Epoch]
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

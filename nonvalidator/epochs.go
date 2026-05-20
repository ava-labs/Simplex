// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package nonvalidator

import (
	"errors"

	"github.com/ava-labs/simplex"
)

var errNoGenesis = errors.New("No Genesis Found")

type epochMetadata struct {
	nodes               simplex.Nodes
	nodeLookup          map[string]struct{}
	epoch               uint64
	signatureAggregator simplex.SignatureAggregator
}

type epochs map[uint64]*epochMetadata

func newEpochMetadata(sealingMetadata *simplex.SealingBlockInfo, sigCreator simplex.SignatureAggregatorCreator) *epochMetadata {
	if sealingMetadata == nil {
		return nil
	}

	nodes := sealingMetadata.ValidatorSet
	lookup := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		lookup[string(node.Node)] = struct{}{}
	}

	return &epochMetadata{
		nodes:               nodes,
		nodeLookup:          lookup,
		epoch:               sealingMetadata.Epoch,
		signatureAggregator: sigCreator(nodes),
	}
}

// newEpochs creates a mapping of epoch numbers -> epoch metadata. The epoch metadata is used for verifying
// blocks and finalizations, and should only contain epochMetadata that we have validated.
func newEpochs(storage simplex.Storage, sigAggCreator simplex.SignatureAggregatorCreator) (epochs, error) {
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

	sealingBlock := lastBlock
	if sealingBlock.SealingBlockInfo() == nil {
		sealingBlock, _, err = storage.Retrieve(lastBlock.BlockHeader().Epoch)
		if err != nil {
			return nil, err
		}
	}

	lastAcceptedEpoch := newEpochMetadata(sealingBlock.SealingBlockInfo(), sigAggCreator)
	epochs[lastAcceptedEpoch.epoch] = lastAcceptedEpoch
	return epochs, nil
}

func (e epochs) highestEpoch() uint64 {
	highest := uint64(0)
	for epoch, _ := range e {
		if epoch > highest {
			highest = epoch
		}
	}

	return highest
}

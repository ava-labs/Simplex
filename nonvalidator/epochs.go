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

// TODO: what if our last accepted block is either the firstSimplexBlock or we haven't accepted any blocks yet? aka we just have the genesis
func newEpochs(storage simplex.Storage, sigAggCreator simplex.SignatureAggregatorCreator) (map[uint64]*epochMetadata, error) {
	lastBlockHeight := storage.NumBlocks()
	if lastBlockHeight == 0 {
		return nil, errNoGenesis
	}

	lastBlock, _, err := storage.Retrieve(lastBlockHeight - 1)
	if err != nil {
		return nil, err
	}

	// A zero Epoch means this is before the first ever Simplex block(ex. Genesis or Last Snowman Block)
	if lastBlock.BlockHeader().Epoch == 0 {
		// return a nil map
		return make(map[uint64]*epochMetadata), nil
	}

	sealingBlock := lastBlock
	if sealingBlock.SealingBlockInfo() == nil {
		sealingBlock, _, err = storage.Retrieve(lastBlock.BlockHeader().Epoch)
		if err != nil {
			return nil, err
		}
	}

	lastAcceptedEpoch := newEpochMetadata(sealingBlock.SealingBlockInfo(), sigAggCreator)
	return map[uint64]*epochMetadata{lastAcceptedEpoch.epoch: lastAcceptedEpoch}, nil
}

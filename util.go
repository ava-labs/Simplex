// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"errors"
	"fmt"

	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
	"go.uber.org/zap"
)

var (
	errNoGenesisBlock  = errors.New("no genesis block found in storage")
	errNonSealingBlock = errors.New("expected sealing block, got a non-sealing block")
)

// LastBlock returns the last block in storage along with the total number of blocks.
func LastBlock(storage Storage) (metadata.StateMachineBlock, uint64, error) {
	numBlocks := storage.NumBlocks()
	if numBlocks == 0 {
		return metadata.StateMachineBlock{}, 0, errNoGenesisBlock
	}

	lastBlock, _, err := storage.GetBlock(numBlocks - 1)
	if err != nil {
		return metadata.StateMachineBlock{}, 0, fmt.Errorf("error retrieving last block from storage: %w", err)
	}

	return lastBlock, numBlocks, nil
}

// getLastAcceptedEpoch determines the epoch the instance should start at based on
// the last block in storage. If the ledger only contains non-Simplex blocks, the
// epoch is the first Simplex height. If the last block is a sealing block, the
// epoch it seals has ended, so the next epoch is returned. Otherwise, the epoch
// of the last block is returned.
func getLastAcceptedEpochAndValidatorSet(config *Config) (common.Nodes, uint64, error) {
	lastBlock, numBlocks, err := LastBlock(config.Storage)
	if err != nil {
		return nil, 0, fmt.Errorf("error retrieving last block: %w", err)
	}

	lastNonSimplexHeight := config.LastNonSimplexInnerBlock.Height()
	parsedLastBlock := ParsedBlock{StateMachineBlock: lastBlock}
	epochNum := parsedLastBlock.BlockHeader().Epoch
	genesisValidatorSet := config.PlatformChain.GenesisValidatorSet()

	var validatorSet metadata.NodeBLSMappings
	var nodes common.Nodes

	switch {
	// If all we have in the ledger is non-Simplex blocks, load the validator set from genesis
	case lastNonSimplexHeight+1 == numBlocks:
		validatorSet = genesisValidatorSet
		nodes = validatorSetToNodes(genesisValidatorSet)
		epochNum = lastNonSimplexHeight + 1
		config.Logger.Debug("Determined epoch and validator set from genesis (ledger holds only non-Simplex blocks)",
			zap.Uint64("epoch", epochNum))
	// If the last block persisted is a sealing block, then we are in the next epoch.
	case lastBlock.SealingBlockInfo() != nil:
		epochNum = parsedLastBlock.BlockHeader().Seq
		validatorSet = constructValidatorSetFromSealingBlock(&parsedLastBlock)
		nodes = lastBlock.SealingBlockInfo().ValidatorSet
		config.Logger.Debug("Determined epoch and validator set from sealing block at tip",
			zap.Uint64("epoch", epochNum))
	// Else, we have at least one Simplex block in the ledger, and it's not a sealing block.
	default:
		// Therefore, the sequence of the sealing block is the epoch number.
		sealingBlockSeq := parsedLastBlock.BlockHeader().Epoch
		sealingBlock, _, err := config.Storage.GetBlock(sealingBlockSeq)
		if err != nil {
			return nil, 0, fmt.Errorf("error retrieving sealing block from storage: %w", err)
		}
		if sealingBlock.Metadata.SimplexEpochInfo.BlockValidationDescriptor == nil {
			return nil, 0, fmt.Errorf("%w at seq %d", errNonSealingBlock, sealingBlockSeq)
		}
		validatorSet = constructValidatorSetFromSealingBlock(&ParsedBlock{StateMachineBlock: sealingBlock})
		nodes = validatorSetToNodes(validatorSet)
		config.Logger.Debug("Determined epoch and validator set from sealing block in storage",
			zap.Uint64("epoch", epochNum), zap.Uint64("sealingBlockSeq", sealingBlockSeq))
	}
	return nodes, epochNum, nil
}

func validatorSetToNodes(validatorSet metadata.NodeBLSMappings) common.Nodes {
	var nodes common.Nodes
	for i := range validatorSet {
		vdr := &validatorSet[i]
		nodes = append(nodes, common.Node{
			Id:     vdr.NodeID[:],
			Weight: vdr.Weight,
			PK:     vdr.BLSKey,
		})
	}
	return nodes
}

func constructValidatorSetFromSealingBlock(lastBlock *ParsedBlock) metadata.NodeBLSMappings {
	var validatorSet metadata.NodeBLSMappings
	vdrs := lastBlock.Metadata.SimplexEpochInfo.BlockValidationDescriptor.AggregatedMembership.Members
	for i := range vdrs {
		vdr := &vdrs[i]
		validatorSet = append(validatorSet, metadata.NodeBLSMapping{
			NodeID: vdr.NodeID,
			BLSKey: vdr.BLSKey,
			Weight: vdr.Weight,
		})
	}
	return validatorSet
}

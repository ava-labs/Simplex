// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"sync"
	"testing"

	metadata "github.com/ava-labs/simplex/msm"
	"github.com/stretchr/testify/require"
)

// func TestValidatorIndexes(t *testing.T) {
// 	validatorID := generateNodeIDMapping()

// 	genesisSet := []metadata.NodeBLSMapping{validatorID}

// 	pChain := newTestPChain(genesisSet)
// 	chain := newChain(t, pChain)
// 	chain.addNode(validatorID.NodeID[:])

// 	_, err := chain.index()
// 	require.NoError(t, err)
// }

// func TestNonValidatorSyncs(t *testing.T) {
// 	validatorID := generateNodeIDMapping()
// 	nonValidatorID := generateNodeIDMapping()

// 	genesisSet := []metadata.NodeBLSMapping{validatorID}

// 	pChain := newTestPChain(genesisSet)
// 	chain := newChain(t, pChain)
// 	chain.addNode(validatorID.NodeID[:])

// 	_, err := chain.index()
// 	require.NoError(t, err)
// 	_, err = chain.index()
// 	require.NoError(t, err)

// 	chain.addNode(nonValidatorID.NodeID[:])
// }

// func TestNonValidator_BecomesValidator(t *testing.T) {
// 	validatorID := generateNodeIDMapping()
// 	upcomingValidator := generateNodeIDMapping()

// 	genesisSet := []metadata.NodeBLSMapping{validatorID}

// 	pChain := newTestPChain(genesisSet)
// 	chain := newChain(t, pChain)
// 	chain.addNode(validatorID.NodeID[:])

// 	_, err := chain.index()
// 	require.NoError(t, err)
// 	_, err = chain.index()
// 	require.NoError(t, err)

// 	chain.addNode(upcomingValidator.NodeID[:])

// 	fmt.Println("advancing height")
// 	// initiate an epoch change
// 	pChain.setValidatorSetAt(10, []metadata.NodeBLSMapping{validatorID, upcomingValidator})
// 	pChain.advanceHeight(10)

// 	// now that we advanced the height the validator will keep building empty blocks until the upcoming validator sends an approval
// 	time.Sleep(5 * time.Second)
// }

// func TestValidator_ValidatorSetNotChanged(t *testing.T) {
// 	validatorID := generateNodeIDMapping()

// 	genesisSet := []metadata.NodeBLSMapping{validatorID}

// 	pChain := newTestPChain(genesisSet)
// 	chain := newChain(t, pChain)
// 	chain.addNode(validatorID.NodeID[:])

// 	_, err := chain.index()
// 	require.NoError(t, err)

// 	// initiate an epoch change
// 	pChain.setValidatorSetAt(10, []metadata.NodeBLSMapping{validatorID})
// 	pChain.advanceHeight(10)

// 	// potential time to propose blocks
// 	time.Sleep(3 * time.Second)

// 	block, err := chain.index()
// 	require.NoError(t, err)
// 	require.Equal(t, uint64(1), block.BlockHeader().Epoch)
// }

func TestValidator_ValidatorSetDecreased(t *testing.T) {
	validatorID := generateNodeIDMapping([20]byte{1})
	leavingValidatorID := generateNodeIDMapping([20]byte{2})

	genesisSet := []metadata.NodeBLSMapping{validatorID, leavingValidatorID}

	pChain := newTestPChain(genesisSet)
	chain := newChain(t, pChain)
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		chain.addNode(validatorID.NodeID[:])
		wg.Done()
	}()
	chain.addNode(leavingValidatorID.NodeID[:])

	// all nodes have synced the first every simplex block
	wg.Wait()

	// time.Sleep(1 * time.Second)
	block, err := chain.index()
	require.NoError(t, err)
	require.Equal(t, uint64(2), block.BlockHeader().Round)

	// // initiate an epoch change
	// pChain.setValidatorSetAt(10, []metadata.NodeBLSMapping{validatorID})
	// pChain.advanceHeight(10)

	// potential time to propose blocks
	// time.Sleep(3 * time.Second)

	// block, err := chain.index()
	// require.NoError(t, err)
	// require.Equal(t, uint64(1), block.BlockHeader().Epoch)
}

// Tests a non-validator converts to a validator when the epoch admitting it is committed,
// proven by the finalization of the next block carrying both nodes' signatures.
// func TestNonValidatorJoins(t *testing.T) {
// 	chain := newChain(t)

// 	// The initial validator set is just currentValidator, so futureValidator comes up as a
// 	// non-validator that tracks the chain.
// 	currentValidator := chain.newNode(nodeConfig{Name: "current-validator", Validator: true})
// 	futureValidator := chain.newNode(nodeConfig{Name: "future-validator"})

// 	chain.AddNodes(currentValidator, futureValidator)

// 	chain.IndexBlock()

// 	// The new validator set is current + future.
// 	chain.IndexSealing(currentValidator, futureValidator)

// 	block := chain.IndexBlock()

// 	// A quorum of the new epoch needs both nodes, so both signing the finalization proves the
// 	// joined node takes part in consensus rather than merely tracking the chain.
// 	chain.RequireFinalizedBy(block, currentValidator, futureValidator)
// }

// // Tests that a non-validator joining a chain that has already sealed an epoch syncs across
// // every epoch up to the tip.
// func TestNonValidatorSyncs(t *testing.T) {
// 	chain := newChain(t, chainConfig{})

// 	// The initial validator set is just currentValidator.
// 	currentValidator := chain.newNode(nodeConfig{Name: "current-validator", Validator: true})
// 	syncingNonValidator := chain.newNode(nodeConfig{Name: "syncing-non-validator"})

// 	chain.AddNodes(currentValidator)

// 	chain.IndexBlock()
// 	chain.IndexBlock()
// 	chain.IndexSealing(currentValidator)
// 	latestBlock := chain.IndexBlock()

// 	chain.AddNodes(syncingNonValidator)

// 	syncingNonValidator.WaitForCommit(latestBlock)
// 	syncingNonValidator.RequireNonValidator()
// }

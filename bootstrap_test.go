// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"fmt"
	"testing"
	"time"

	metadata "github.com/ava-labs/simplex/msm"
	"github.com/stretchr/testify/require"
)

func TestValidatorIndexes(t *testing.T) {
	validatorID := generateNodeIDMapping()

	genesisSet := []metadata.NodeBLSMapping{validatorID}

	pChain := newTestPChain(genesisSet)
	chain := newChain(t, pChain)
	chain.addNode(validatorID.NodeID[:])

	_, err := chain.index()
	require.NoError(t, err)
}

func TestNonValidatorSyncs(t *testing.T) {
	validatorID := generateNodeIDMapping()
	nonValidatorID := generateNodeIDMapping()

	genesisSet := []metadata.NodeBLSMapping{validatorID}

	pChain := newTestPChain(genesisSet)
	chain := newChain(t, pChain)
	chain.addNode(validatorID.NodeID[:])

	_, err := chain.index()
	require.NoError(t, err)
	_, err = chain.index()
	require.NoError(t, err)

	chain.addNode(nonValidatorID.NodeID[:])
}

func TestNonValidator_BecomesValidator(t *testing.T) {
	validatorID := generateNodeIDMapping()
	upcomingValidator := generateNodeIDMapping()

	genesisSet := []metadata.NodeBLSMapping{validatorID}

	pChain := newTestPChain(genesisSet)
	chain := newChain(t, pChain)
	chain.addNode(validatorID.NodeID[:])

	_, err := chain.index()
	require.NoError(t, err)
	_, err = chain.index()
	require.NoError(t, err)

	chain.addNode(upcomingValidator.NodeID[:])

	fmt.Println("advancing height")
	// initiate an epoch change
	pChain.setValidatorSetAt(10, []metadata.NodeBLSMapping{validatorID, upcomingValidator})
	pChain.advanceHeight(10)

	// now that we advanced the height the validator will keep building empty blocks until the upcoming validator sends an approval
	time.Sleep(5 * time.Second)

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

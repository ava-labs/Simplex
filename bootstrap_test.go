// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/stretchr/testify/require"
)

var (
	nodeID1 = [20]byte{1}
	nodeID2 = [20]byte{2}
)

// TestValidatorIndexes tests that a validator indexes and accepts a new block sent by the network
// It is the only validator, so it will build and finalize its own block.
func TestValidatorIndexes(t *testing.T) {
	validatorID := generateNodeIDMapping(nodeID1)

	genesisSet := []metadata.NodeBLSMapping{validatorID}

	pChain := newTestPChain(genesisSet)
	chain := newChain(t, pChain)
	chain.addNode(validatorID.NodeID[:])

	chain.acceptNewBlock()
}

// TestNonValidatorSyncs that a non-validator syncs the chain when added to the network.
func TestNonValidatorSyncs(t *testing.T) {
	validatorID := generateNodeIDMapping(nodeID1)
	genesisSet := []metadata.NodeBLSMapping{validatorID}

	pChain := newTestPChain(genesisSet)
	chain := newChain(t, pChain)
	chain.addNode(validatorID.NodeID[:])

	chain.acceptNewBlock()

	nonValidatorID := generateNodeIDMapping(nodeID2)
	chain.addNode(nonValidatorID.NodeID[:])
}

// TestNonValidator_BecomesValidator tests that an upcoming validator becomes a validator
// when an epoch change they are following is sealed. It does so by checking they participated in signing
// NOTE: This test will not pass until approval dissemination happens from non-validators.
// Equivalent test as the previous TestInstanceMixedNodeType.
func TestNonValidator_BecomesValidator(t *testing.T) {
	validatorID := generateNodeIDMapping(nodeID1)

	genesisSet := []metadata.NodeBLSMapping{validatorID}

	pChain := newTestPChain(genesisSet)
	chain := newChain(t, pChain)
	chain.addNode(validatorID.NodeID[:])

	chain.acceptNewBlock()

	// The non-validator node syncs the accepted blocks and then contributes to the next blocks
	upcomingValidator := generateNodeIDMapping(nodeID2)
	chain.addNode(upcomingValidator.NodeID[:])

	// initiate an epoch change
	pChain.setValidatorSetAt(10, []metadata.NodeBLSMapping{validatorID, upcomingValidator})
	pChain.advanceHeight(10)

	chain.waitUntilSealingBlock()
}

// TestValidator_ValidatorSetNotChanged tests that a pchain height increase
// that does not have a unique validator set, does not create a new epoch
func TestValidator_ValidatorSetNotChanged(t *testing.T) {
	validatorID := generateNodeIDMapping(nodeID1)

	genesisSet := []metadata.NodeBLSMapping{validatorID}

	pChain := newTestPChain(genesisSet)
	chain := newChain(t, pChain)
	chain.addNode(validatorID.NodeID[:])

	chain.acceptNewBlock()

	// initiate an epoch change
	pChain.setValidatorSetAt(10, []metadata.NodeBLSMapping{validatorID})
	pChain.advanceHeight(10)

	// potential time to propose blocks (if any)
	time.Sleep(3 * time.Second)

	block := chain.acceptNewBlock()
	require.Equal(t, uint64(1), block.BlockHeader().Epoch)
}

// TestValidator_ValidatorSetDecreased tests that an epoch with two validators
// is reduced to one, when the pchain height notes a validator is leaving.
func TestValidator_ValidatorSetDecreased(t *testing.T) {
	validatorID := generateNodeIDMapping(nodeID1)
	leavingValidatorID := generateNodeIDMapping(nodeID2)

	genesisSet := []metadata.NodeBLSMapping{validatorID, leavingValidatorID}

	pChain := newTestPChain(genesisSet)
	chain := newChain(t, pChain)
	wg := sync.WaitGroup{}

	wg.Go(func() {
		// add node is a synchronous call.
		chain.addNode(validatorID.NodeID[:])
	})

	chain.addNode(leavingValidatorID.NodeID[:])

	// all nodes have synced the first every simplex block
	// TODO: we should initialize our node so that it already stores the first ever simplex block
	// otherwise, we rely on all nodes to be connected in order to finalize it.
	wg.Wait()

	block := chain.acceptNewBlock()
	require.Equal(t, uint64(2), block.BlockHeader().Round)

	// initiate an epoch change
	pChain.setValidatorSetAt(10, []metadata.NodeBLSMapping{validatorID})
	pChain.advanceHeight(10)

	sealing := chain.waitUntilSealingBlock()
	require.Contains(t, sealing.SealingBlockInfo().ValidatorSet.NodeIDs(), common.NodeID(validatorID.NodeID[:]))
	require.NotContains(t, sealing.SealingBlockInfo().ValidatorSet.NodeIDs(), common.NodeID(leavingValidatorID.NodeID[:]))
}

// TestNonValidator_StaysNonValidator ensures that a non-validator does not restart when it is processing
// previous epoch changes.
// Equivalent to TestInstanceNonValidatorBootstraps
func TestNonValidator_StaysNonValidator(t *testing.T) {
	// case 1: epoch change is not highest and we are NOT in the validator set
	// case 1: epoch change is not highest, and we are in the validator set
	// case 1: epoch change is highest, and we are in the validator set
	// case 1: epoch change is highest, and we are NOT in the validator set
}

// TestInstanceValidatorSkipsAnEpoch tests that a validator stops and starts being a validator
// It boots up as a non-validator then syncs to the highest epoch where it is a validator,
// then it is no longer a validator, and finally it is
// Equivalent to: TestInstanceValidatorSkipsAnEpoch
func TestInstanceValidatorSkipsAnEpoch(t *testing.T) {

}

// TestInstanceTransitionFromSnowman tests that when passed in a config that indicates we are transitioning from snowman,
// the instance still properly starts and indexes future blocks.
// This test, alongside the util tests cover the previous TestInstanceRestartAcrossEpochs
func TestInstanceTransitionFromSnowman(t *testing.T) {
	validatorID := generateNodeIDMapping(nodeID1)
	validatorID2 := generateNodeIDMapping(nodeID2)

	genesisSet := []metadata.NodeBLSMapping{validatorID, validatorID2}

	pChain := newTestPChain(genesisSet)
	chain := newChain(t, pChain)
	chain.addNode(validatorID.NodeID[:])
	// chain.
}

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

// TestValidatorIndexes tests that a validator indexes a new block sent by the network
func TestValidatorIndexes(t *testing.T) {
	validatorID := generateNodeIDMapping(nodeID1)

	genesisSet := []metadata.NodeBLSMapping{validatorID}

	pChain := newTestPChain(genesisSet)
	chain := newChain(t, pChain)
	chain.addNode(validatorID.NodeID[:])

	_, err := chain.index()
	require.NoError(t, err)
}

// TestNonValidatorSyncs that a non-validator syncs the chain when added to the network.
func TestNonValidatorSyncs(t *testing.T) {
	validatorID := generateNodeIDMapping(nodeID1)
	nonValidatorID := generateNodeIDMapping(nodeID2)

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

// TestNonValidator_BecomesValidator tests that an upcoming validator becomes a validator
// when an epoch change they are following is sealed. It does so by checking they participated in signing
// NOTE: This test will not pass until approval dissemination happens from non-validators.
func TestNonValidator_BecomesValidator(t *testing.T) {
	validatorID := generateNodeIDMapping(nodeID1)
	upcomingValidator := generateNodeIDMapping(nodeID2)

	genesisSet := []metadata.NodeBLSMapping{validatorID}

	pChain := newTestPChain(genesisSet)
	chain := newChain(t, pChain)
	chain.addNode(validatorID.NodeID[:])

	_, err := chain.index()
	require.NoError(t, err)
	_, err = chain.index()
	require.NoError(t, err)

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

	_, err := chain.index()
	require.NoError(t, err)

	// initiate an epoch change
	pChain.setValidatorSetAt(10, []metadata.NodeBLSMapping{validatorID})
	pChain.advanceHeight(10)

	// potential time to propose blocks (if any)
	time.Sleep(3 * time.Second)

	block, err := chain.index()
	require.NoError(t, err)
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

	wg.Add(1)
	go func() {
		// add node is a synchronous call.
		chain.addNode(validatorID.NodeID[:])
		wg.Done()
	}()
	chain.addNode(leavingValidatorID.NodeID[:])

	// all nodes have synced the first every simplex block
	// TODO: we should initialize our node so that it already stores the first ever simplex block
	// otherwise, we rely on all nodes to be connected in order to finalize it.
	wg.Wait()

	block, err := chain.index()
	require.NoError(t, err)
	require.Equal(t, uint64(2), block.BlockHeader().Round)

	// initiate an epoch change
	pChain.setValidatorSetAt(10, []metadata.NodeBLSMapping{validatorID})
	pChain.advanceHeight(10)

	sealing := chain.waitUntilSealingBlock()
	require.Contains(t, sealing.SealingBlockInfo().ValidatorSet.NodeIDs(), common.NodeID(validatorID.NodeID[:]))
	require.NotContains(t, sealing.SealingBlockInfo().ValidatorSet.NodeIDs(), common.NodeID(leavingValidatorID.NodeID[:]))
}

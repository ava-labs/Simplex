// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/ava-labs/simplex/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

// TestValidatorIndexes tests that a validator indexes and accepts a new block sent by the network
// It is the only validator, so it will build and finalize its own block.
func TestValidatorIndexes(t *testing.T) {
	validator := newBLSMapping(1)

	genesisSet := []metadata.NodeBLSMapping{validator}

	pChain := newTestPChain(genesisSet)
	chain := newNetwork(t, pChain)
	chain.addNode(validator.NodeID[:])

	chain.acceptNewBlock()
}

// emptyVoteRecorder signals the first empty vote broadcast and drops all other traffic.
type emptyVoteRecorder struct {
	got chan struct{}
}

func (r *emptyVoteRecorder) Broadcast(msg *common.Message) {
	if msg.EmptyVoteMessage != nil {
		select {
		case r.got <- struct{}{}:
		default:
		}
	}
}

func (r *emptyVoteRecorder) Send(*common.Message, common.NodeID) {}

// TestEpochInvokesMSMWaitForPendingBlock verifies the epoch drives the MSM's WaitForPendingBlock:
// a non-leader whose VM never has a pending block must still broadcast an empty vote.
// The round leader is never instantiated, so no proposal ever arrives.
func TestEpochInvokesMSMWaitForPendingBlock(t *testing.T) {
	ourValidator := newBLSMapping(1)
	leader := newBLSMapping(2)
	genesisSet := []metadata.NodeBLSMapping{ourValidator, leader}

	pChain := newTestPChain(genesisSet)
	nodes := pChain.GenesisValidatorSet().Nodes()
	common.SortNodes(nodes)
	require.NotEqual(t, common.NodeID(ourValidator.NodeID[:]), simplex.LeaderForRound(nodes.NodeIDs(), 1))

	storage := NewMockStorageWithGenesis(t, &testInnerBlockDeserializer{})
	// the VM blocks in WaitForPendingBlock until a block is indexed, which never happens here
	vm := newBlockBuilderVM(testutil.NewTestControlledBlockBuilder(t), storage, newPendingBlockSignal())
	recorder := &emptyVoteRecorder{got: make(chan struct{}, 1)}
	wc := &walCreator{t: t}

	inst := NewInstance(Config{
		LastNonSimplexInnerBlock: genesisBlock,
		ParameterConfig:          paramConfig,
		PlatformChain:            pChain,
		Broadcaster:              recorder,
		Sender:                   recorder,
		CryptoOps:                &testCryptoOps{},
		WalCreator:               wc.createWAL,
		Storage:                  storage,
		Logger:                   testutil.MakeLogger(t, 1),
		VM:                       vm,
		ICMETransition:           noopICMTransition,
		ID:                       ourValidator.NodeID[:],
	})

	require.NoError(t, inst.Start(t.Context()))
	t.Cleanup(inst.Stop)

	select {
	case <-recorder.got:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "node never broadcast an empty vote, so the Epoch did not drive the MSM's WaitForPendingBlock")
	}
}

// TestNonValidatorSyncs that a non-validator syncs the chain when added to the network.
func TestNonValidatorSyncs(t *testing.T) {
	validator := newBLSMapping(1)
	genesisSet := []metadata.NodeBLSMapping{validator}

	pChain := newTestPChain(genesisSet)
	chain := newNetwork(t, pChain)
	chain.addNode(validator.NodeID[:])

	chain.acceptNewBlock()

	nonValidator := newBLSMapping(2)
	chain.addNode(nonValidator.NodeID[:])
}

// TestNonValidator_BecomesValidator tests that an upcoming validator becomes a validator
// when an epoch change they are following is sealed. The non-validator must contribute it's approval in
// order to do so.
func TestNonValidator_BecomesValidator(t *testing.T) {
	validator := newBLSMapping(1)

	genesisSet := []metadata.NodeBLSMapping{validator}

	pChain := newTestPChain(genesisSet)
	chain := newNetwork(t, pChain)
	chain.addNode(validator.NodeID[:])

	chain.acceptNewBlock()

	// The non-validator node syncs the accepted blocks and then contributes to the next blocks
	upcomingValidator := newBLSMapping(2)
	chain.addNode(upcomingValidator.NodeID[:])

	// initiate an epoch change
	newValidatorSet := metadata.NodeBLSMappings{validator, upcomingValidator}
	pChain.setValidatorSetAt(10, newValidatorSet)
	pChain.advanceHeight(10)

	sealingBlock := chain.waitUntilSealingBlock()
	assertExpectedNodeIds(t, sealingBlock.SealingBlockInfo().ValidatorSet.NodeIDs(), newValidatorSet.NodeIDs())

	_, finalization := chain.acceptNewBlock()
	assertExpectedNodeIds(t, finalization.QC.Signers(), newValidatorSet.NodeIDs())
}

// TestValidator_ValidatorSetNotChanged tests that a P-chain height increase
// that does not have a unique validator set, does not create a new epoch
func TestValidator_ValidatorSetNotChanged(t *testing.T) {
	validator := newBLSMapping(1)

	genesisSet := []metadata.NodeBLSMapping{validator}

	pChain := newTestPChain(genesisSet)
	chain := newNetwork(t, pChain)
	chain.addNode(validator.NodeID[:])

	firstBlock, _ := chain.acceptNewBlock()

	// initiate an epoch change
	pChain.setValidatorSetAt(10, []metadata.NodeBLSMapping{validator})
	pChain.advanceHeight(10)

	// potential time to propose blocks (if any)
	time.Sleep(3 * time.Second)

	secondBlock, _ := chain.acceptNewBlock()
	require.Equal(t, uint64(1), secondBlock.BlockHeader().Epoch)
	require.Equal(t, firstBlock.BlockHeader().Seq+1, secondBlock.BlockHeader().Seq)
}

// TestValidator_ValidatorSetDecreased tests that an epoch with two validators
// is reduced to one, when the pchain height notes a validator is leaving.
func TestValidator_ValidatorSetDecreased(t *testing.T) {
	validator := newBLSMapping(1)
	leavingValidator := newBLSMapping(2)

	genesisSet := []metadata.NodeBLSMapping{validator, leavingValidator}

	pChain := newTestPChain(genesisSet)
	chain := newNetwork(t, pChain)
	wg := sync.WaitGroup{}

	wg.Go(func() {
		// add node is a synchronous call.
		chain.addNode(validator.NodeID[:])
	})

	chain.addNode(leavingValidator.NodeID[:])

	// all nodes have synced the first every simplex block
	wg.Wait()

	block, _ := chain.acceptNewBlock()
	require.Equal(t, uint64(2), block.BlockHeader().Round)

	// initiate an epoch change
	newValidatorSet := metadata.NodeBLSMappings{validator}
	pChain.setValidatorSetAt(10, newValidatorSet)
	pChain.advanceHeight(10)

	sealing := chain.waitUntilSealingBlock()
	assertExpectedNodeIds(t, sealing.SealingBlockInfo().ValidatorSet.NodeIDs(), newValidatorSet.NodeIDs())
}

// TestNonValidator_StaysNonValidator ensures that a non-validator does not restart when it is processing
// previous epoch changes.
func TestNonValidator_StaysNonValidator(t *testing.T) {
	targetNode := newBLSMapping(42)

	// case 1: epoch change is not highest and we are NOT in the validator1 set
	// case 1: epoch change is not highest, and we are in the validator1 set
	// case 1: epoch change is highest, and we are in the validator1 set
	// case 1: epoch change is highest, and we are NOT in the validator1 set
	validator1 := newBLSMapping(1)

	genesisSet := []metadata.NodeBLSMapping{validator1}
	pChain := newTestPChain(genesisSet)
	chain := newNetwork(t, pChain)
	chain.addNode(validator1.NodeID[:])

	validator2 := newBLSMapping(2)
	validator3 := newBLSMapping(3)
	validator4 := newBLSMapping(4)
	chain.addNode(validator2.NodeID[:])
	chain.addNode(validator3.NodeID[:])
	chain.addNode(validator4.NodeID[:])

	// we should have a quorum without the target node to create this epoch change
	targetNodeNotInMiddleEpoch := metadata.NodeBLSMappings{validator1, validator2, validator3}
	targetNodeInMiddleEpoch := metadata.NodeBLSMappings{validator1, validator2, validator3, targetNode}
	targetNodeInHighestEpoch := metadata.NodeBLSMappings{validator1, validator2, validator3, validator4, targetNode}

	// set the pchain heights
	pChain.setValidatorSetAt(10, targetNodeNotInMiddleEpoch)
	pChain.setValidatorSetAt(20, targetNodeInMiddleEpoch)
	pChain.setValidatorSetAt(30, targetNodeInHighestEpoch)

	pChain.advanceHeight(10)
	sealingBlock := chain.waitUntilSealingBlock()
	assertExpectedNodeIds(t, sealingBlock.SealingBlockInfo().ValidatorSet.NodeIDs(), targetNodeNotInMiddleEpoch.NodeIDs())

	pChain.advanceHeight(20)
	sealingBlock = chain.waitUntilSealingBlock()
	assertExpectedNodeIds(t, sealingBlock.SealingBlockInfo().ValidatorSet.NodeIDs(), targetNodeInMiddleEpoch.NodeIDs())

	// TODO: since the target validator is part of the validator set, yet is offline(not added to network)
	// it can be the leader. By adding the call to acceptNewBlock, it makes the target validator the leader for the
	// round needed to batch approvals. It is offline, and we have no mechanism for triggering this block to be built yet.
	// chain.acceptNewBlock()

	pChain.advanceHeight(30)
	sealingBlock = chain.waitUntilSealingBlock()
	assertExpectedNodeIds(t, sealingBlock.SealingBlockInfo().ValidatorSet.NodeIDs(), targetNodeInHighestEpoch.NodeIDs())

	// the target node should join now
	chain.addNode(targetNode.NodeID[:])
}

// TestInstanceValidatorSkipsAnEpoch tests that a validator stops and starts being a validator
// It boots up as a non-validator then syncs to the highest epoch where it is a validator,
// then it is no longer a validator, and finally it is
func TestInstanceValidatorSkipsAnEpoch(t *testing.T) {
	validator := newBLSMapping(1)

	genesisSet := []metadata.NodeBLSMapping{validator}

	pChain := newTestPChain(genesisSet)
	chain := newNetwork(t, pChain)
	chain.addNode(validator.NodeID[:])

	// The non-validator node syncs the accepted blocks and then contributes to the next blocks
	onOffValidator := newBLSMapping(2)
	chain.addNode(onOffValidator.NodeID[:])

	// initiate an epoch change
	newValidatorSet := metadata.NodeBLSMappings{validator, onOffValidator}
	pChain.setValidatorSetAt(10, newValidatorSet)
	pChain.advanceHeight(10)

	sealingBlock := chain.waitUntilSealingBlock()
	assertExpectedNodeIds(t, sealingBlock.SealingBlockInfo().ValidatorSet.NodeIDs(), newValidatorSet.NodeIDs())

	newValidatorSet = metadata.NodeBLSMappings{validator}
	pChain.setValidatorSetAt(20, newValidatorSet)
	pChain.advanceHeight(20)
	sealingBlock = chain.waitUntilSealingBlock()
	assertExpectedNodeIds(t, sealingBlock.SealingBlockInfo().ValidatorSet.NodeIDs(), newValidatorSet.NodeIDs())

	// accept a new block to ensure both nodes are still syncing the chain
	chain.acceptNewBlock()

	// initiate the final epoch change
	newValidatorSet = metadata.NodeBLSMappings{validator, onOffValidator}
	pChain.setValidatorSetAt(30, newValidatorSet)
	pChain.advanceHeight(30)

	sealingBlock = chain.waitUntilSealingBlock()
	assertExpectedNodeIds(t, sealingBlock.SealingBlockInfo().ValidatorSet.NodeIDs(), newValidatorSet.NodeIDs())
}

func TestInstanceDoubleStartFails(t *testing.T) {
	validator := newBLSMapping(1)
	genesisSet := []metadata.NodeBLSMapping{validator}

	pChain := newTestPChain(genesisSet)
	chain := newNetwork(t, pChain)
	node := chain.addNode(validator.NodeID[:])
	require.ErrorIs(t, node.inst.Start(t.Context()), errAlreadyStarted)
}

// TestNonValidatorSkipsMSMVerification proves that a non-validator does not use the MSM to
// verify blocks: it commits a finalized block whose state machine transition is invalid.
func TestNonValidatorSkipsMSMVerification(t *testing.T) {
	validator := newBLSMapping(1)
	genesisValidatorSet := []metadata.NodeBLSMapping{validator}
	pChain := newTestPChain(genesisValidatorSet)

	// The non-validator holds genesis plus epoch 1's defining block, and lives on a network
	// of its own, so the replication response below is the only way it can learn a block.
	storage, parent := newChainStorage(t, genesisValidatorSet)
	nonValidator := newBLSMapping(2)
	nonValidatorNode := newNetwork(t, pChain).addNodeWithStorage(nonValidator.NodeID[:], storage)

	// A block whose only defect is its state machine transition: its timestamp precedes its
	// parent's.
	invalid := metadata.StateMachineBlock{
		InnerBlock: &testInnerBlock{Height_: 2, TS: time.Now(), Payload: []byte("invalid")},
		Metadata: metadata.StateMachineMetadata{
			Timestamp:               parent.Metadata.Timestamp - 1,
			SimplexProtocolMetadata: common.ProtocolMetadata{Epoch: 1, Round: 2, Seq: 2, Prev: common.Digest(parent.Digest())},
		},
	}

	// Hand the non-validator that block, finalized over its digest.
	block := &ParsedBlock{StateMachineBlock: invalid.Clone()}
	finalization, _ := testutil.NewFinalizationRecord(t, &testutil.TestSignatureAggregator{N: 1}, block, []common.NodeID{validator.NodeID[:]})
	require.NoError(t, nonValidatorNode.inst.HandleMessage(&common.Message{
		ReplicationResponse: &common.ReplicationResponse{
			Data: []common.QuorumRound{{Block: block, Finalization: &finalization}},
		},
	}, validator.NodeID[:]))

	// It commits the block its state machine would have rejected.
	storage.WaitForBlockCommit(2)
	committed, ok := storage.blockAt(2)
	require.True(t, ok)
	require.Equal(t, invalid.Digest(), committed.Digest())
}

// TestValidatorSkipsMSMVerificationWhenReplicating proves that a lagging validator does not
// use the MSM to verify blocks it replicates, as they carry a QC. It checks a notarized and
// a finalized block.
func TestValidatorSkipsMSMVerificationWhenReplicating(t *testing.T) {
	for _, tt := range []struct {
		name string
		// quorumRound wraps the replicated block with a QC.
		quorumRound func(t *testing.T, block *ParsedBlock, signers []common.NodeID) common.QuorumRound
		// requireReplicated asserts the lagging node replicated the block.
		requireReplicated func(t *testing.T, laggingNode *node, block metadata.StateMachineBlock)
	}{
		{
			name: "notarization",
			quorumRound: func(t *testing.T, block *ParsedBlock, signers []common.NodeID) common.QuorumRound {
				notarization, err := testutil.NewNotarization(testutil.MakeLogger(t, 0), &testutil.TestSignatureAggregator{N: len(signers)}, block, signers)
				require.NoError(t, err)
				return common.QuorumRound{Block: block, Notarization: &notarization}
			},
			// A notarized block is not committed but notarized: the node persists the
			// notarization to its WAL, which it only reaches after the block verified.
			requireReplicated: func(t *testing.T, laggingNode *node, block metadata.StateMachineBlock) {
				round := block.Metadata.SimplexProtocolMetadata.Round
				require.Eventually(t, func() bool {
					return laggingNode.wals.containsNotarization(round)
				}, 10*time.Second, 10*time.Millisecond, "no notarization for round %d was persisted to the WAL", round)
				require.Equal(t, block.Metadata.SimplexProtocolMetadata.Seq, laggingNode.storage.NumBlocks(), "a notarized block should not have been committed")
			},
		},
		{
			name: "finalization",
			quorumRound: func(t *testing.T, block *ParsedBlock, signers []common.NodeID) common.QuorumRound {
				finalization, _ := testutil.NewFinalizationRecord(t, &testutil.TestSignatureAggregator{N: len(signers)}, block, signers)
				return common.QuorumRound{Block: block, Finalization: &finalization}
			},
			// A finalized block is committed.
			requireReplicated: func(t *testing.T, laggingNode *node, block metadata.StateMachineBlock) {
				seq := block.Metadata.SimplexProtocolMetadata.Seq
				laggingNode.storage.WaitForBlockCommit(seq)
				committed, ok := laggingNode.storage.blockAt(seq)
				require.True(t, ok)
				require.Equal(t, block.Digest(), committed.Digest())
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Two validators, so a quorum is both of them: with its peer absent, the lagging
			// validator can neither build a block nor empty notarize a round on its own.
			lagging := newBLSMapping(1)
			peer := newBLSMapping(2)
			validators := metadata.NodeBLSMappings{lagging, peer}
			pChain := newTestPChain(validators)

			// The lagging validator holds genesis plus epoch 1's defining block, and lives on
			// a network of its own, so the replication response below is the only way it can
			// learn the block at the round it sits on.
			storage, parent := newChainStorage(t, validators)
			laggingNode := newNetwork(t, pChain).addNodeWithStorage(lagging.NodeID[:], storage)

			// A block whose only defect is its state machine transition: its timestamp
			// precedes its parent's.
			invalid := metadata.StateMachineBlock{
				InnerBlock: &testInnerBlock{Height_: 2, TS: time.Now(), Payload: []byte("invalid")},
				Metadata: metadata.StateMachineMetadata{
					Timestamp:               parent.Metadata.Timestamp - 1,
					SimplexProtocolMetadata: common.ProtocolMetadata{Epoch: 1, Round: 2, Seq: 2, Prev: common.Digest(parent.Digest())},
				},
			}

			// Hand the lagging validator that block wrapped in a QC.
			block := &ParsedBlock{StateMachineBlock: invalid.Clone()}
			quorumRound := tt.quorumRound(t, block, validators.NodeIDs())
			require.NoError(t, laggingNode.inst.HandleMessage(&common.Message{
				ReplicationResponse: &common.ReplicationResponse{Data: []common.QuorumRound{quorumRound}},
			}, peer.NodeID[:]))

			// It replicates the block its state machine would have rejected.
			tt.requireReplicated(t, laggingNode, invalid)
		})
	}
}

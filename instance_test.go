// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
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
	validator := newNodeMapping(1)

	genesisSet := []metadata.NodeBLSMapping{validator}

	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)
	network.addNode(validator.NodeID[:]).start().sync()

	network.acceptNewBlock()
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

func TestEpochInvokesMSMWaitForPendingBlock(t *testing.T) {
	// Two validators, but only one is instantiated. Our node has the smaller ID so it sorts to
	// index 0 and is a non-leader for round 1 (LeaderForRound picks index 1%2). The other
	// validator is the round leader but is never created, so no block is ever proposed.
	ourValidator := newNodeMapping(1)
	leader := newNodeMapping(2)
	genesisSet := []metadata.NodeBLSMapping{ourValidator, leader}

	pChain := newTestPChain(genesisSet)
	nodes := pChain.GenesisValidatorSet().Nodes()
	common.SortNodes(nodes)
	require.NotEqual(t, common.NodeID(ourValidator.NodeID[:]), simplex.LeaderForRound(nodes.NodeIDs(), 1))

	storage := newTestStorageWithGenesis(t)

	vm := newBlockBuilderVM(storage, newPendingBlockSignal())
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
	validator := newNodeMapping(1)
	genesisSet := []metadata.NodeBLSMapping{validator}

	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)
	network.addNode(validator.NodeID[:]).start()

	network.acceptNewBlock()

	nonValidator := newNodeMapping(2)
	network.addNode(nonValidator.NodeID[:]).start()
	network.acceptNewBlock()
}

// TestNonValidatorBecomesValidator tests that an upcoming validator becomes a validator
// when an epoch change they are following is sealed. The non-validator must contribute it's approval in
// order to do so.
func TestNonValidatorBecomesValidator(t *testing.T) {
	validator := newNodeMapping(1)

	genesisSet := []metadata.NodeBLSMapping{validator}

	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)
	network.addNode(validator.NodeID[:])

	network.acceptNewBlock()

	// The non-validator node syncs the accepted blocks and then contributes to the next blocks
	upcomingValidator := newNodeMapping(2)
	network.addNode(upcomingValidator.NodeID[:])

	// initiate an epoch change
	newValidatorSet := metadata.NodeBLSMappings{validator, upcomingValidator}
	pChain.setValidatorSetAt(10, newValidatorSet)
	pChain.advanceHeight(10)

	network.waitUntilSealingBlock(newValidatorSet.Nodes())

	// a normal block should be signed by both the validators now
	_, finalization := network.acceptNewBlock()
	assertExpectedNodeIds(t, finalization.QC.Signers(), newValidatorSet.NodeIDs())
}

// TestValidator_ValidatorSetNotChanged tests that a P-chain height increase
// that does not have a unique validator set, does not create a new epoch
func TestValidator_ValidatorSetNotChanged(t *testing.T) {
	validator := newNodeMapping(1)

	genesisSet := []metadata.NodeBLSMapping{validator}

	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)
	node := network.addNode(validator.NodeID[:])

	firstBlock, _ := network.acceptNewBlock()

	// initiate an epoch change
	pChain.setValidatorSetAt(10, []metadata.NodeBLSMapping{validator})
	pChain.advanceHeight(10)

	blockCount := node.storage.NumBlocks()
	// potential time to propose blocks (if any)
	require.Never(t, func() bool {
		return node.storage.NumBlocks() != blockCount
	}, time.Second, 100*time.Millisecond, "no new block should have been proposed")

	secondBlock, _ := network.acceptNewBlock()
	require.Equal(t, uint64(1), secondBlock.BlockHeader().Epoch)
	require.Equal(t, firstBlock.BlockHeader().Seq+1, secondBlock.BlockHeader().Seq)
}

// TestValidatorValidatorSetDecreased tests that an epoch with two validators
// is reduced to one, when the pchain height notes a validator is leaving.
func TestValidatorValidatorSetDecreased(t *testing.T) {
	validatorMapping := newNodeMapping(1)
	leavingValidatorMapping := newNodeMapping(2)

	genesisSet := []metadata.NodeBLSMapping{validatorMapping, leavingValidatorMapping}

	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)

	// starting from storage holding the first simplex block avoids
	// blocking on a sync that needs a quorum online
	network.addNode(validatorMapping.NodeID[:]).start()
	network.addNode(leavingValidatorMapping.NodeID[:]).start().sync()

	block, _ := network.acceptNewBlock()
	require.Equal(t, uint64(2), block.BlockHeader().Seq)

	// initiate an epoch change
	newValidatorSet := metadata.NodeBLSMappings{validatorMapping}
	pChain.setValidatorSetAt(10, newValidatorSet)
	pChain.advanceHeight(10)

	sealing := network.waitUntilSealingBlock(newValidatorSet.Nodes())

	block, _ = network.acceptNewBlock()
	require.Equal(t, sealing.BlockHeader().Seq+1, block.BlockHeader().Seq)
}

// TestInstanceOfflineDuringTransition asserts an epoch transition completes while a
// current validator is offline. The online quorum batches approvals and finalizes the
// sealing block, and the new epoch finalizes blocks without the offline node.
func TestInstanceOfflineDuringTransition(t *testing.T) {
	v1 := newNodeMapping(1)
	v2 := newNodeMapping(2)
	v3 := newNodeMapping(3)
	offline := newNodeMapping(4)

	genesisSet := []metadata.NodeBLSMapping{v1, v2, v3, offline}
	nodeIDs := metadata.NodeBLSMappings(genesisSet).NodeIDs()
	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)

	node1 := network.addNode(v1.NodeID[:]).start()
	network.addNode(v2.NodeID[:]).start()
	network.addNode(v3.NodeID[:]).start()
	network.sync()

	// using seq should be fine since we have no empty blocks
	leader := simplex.LeaderForRound(pChain.GenesisValidatorSet().NodeIDs(), network.seq)
	for !leader.Equals(offline.NodeID[:]) {
		network.acceptNewBlock()
		leader = simplex.LeaderForRound(nodeIDs, node1.inst.e.Metadata().Round)
	}

	// initiate an epoch change when the offline node is the leader
	newValidatorSet := []metadata.NodeBLSMapping{v1, v2, v3}
	pChain.setValidatorSetAt(10, newValidatorSet)
	pChain.advanceHeight(10)

	network.waitUntilSealingBlock(metadata.NodeBLSMappings(newValidatorSet).Nodes())
}

// TestNonValidatorStaysNonValidator ensures that a non-validator advances through different epoch changes.
func TestNonValidatorStaysNonValidator(t *testing.T) {
	targetNode := newNodeMapping(42)

	// case 1: epoch change is not highest and we are NOT in the validator1 set
	// case 2: epoch change is not highest, and we are in the validator1 set
	// case 3: epoch change is highest, and we are in the validator1 set
	// case 4: epoch change is highest, and we are NOT in the validator1 set
	validator1 := newNodeMapping(1)

	genesisSet := []metadata.NodeBLSMapping{validator1}
	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)
	network.addNode(validator1.NodeID[:]).start()

	validator2 := newNodeMapping(2)
	validator3 := newNodeMapping(3)
	validator4 := newNodeMapping(4)
	network.addNode(validator2.NodeID[:]).start().sync()
	network.addNode(validator3.NodeID[:]).start().sync()
	network.addNode(validator4.NodeID[:]).start().sync()

	// we should have a quorum without the target node to create this epoch change
	targetNodeNotInMiddleEpoch := metadata.NodeBLSMappings{validator1, validator2, validator3}
	targetNodeInMiddleEpoch := metadata.NodeBLSMappings{validator1, validator2, validator3, targetNode}
	targetNodeInHighestEpoch := metadata.NodeBLSMappings{validator1, validator2, validator3, validator4, targetNode}

	// set the pchain heights
	pChain.setValidatorSetAt(10, targetNodeNotInMiddleEpoch)
	pChain.setValidatorSetAt(20, targetNodeInMiddleEpoch)
	pChain.setValidatorSetAt(30, targetNodeInHighestEpoch)

	pChain.advanceHeight(10)
	network.waitUntilSealingBlock(targetNodeNotInMiddleEpoch.Nodes())

	pChain.advanceHeight(20)
	network.waitUntilSealingBlock(targetNodeInMiddleEpoch.Nodes())

	// Occasionally, this will offset the proposer of the transition block to be the offline target node
	// Therefore, if we don't have a mechanism to skip offline leaders during the transition phase, this test will hang
	network.acceptNewBlock()

	pChain.advanceHeight(30)
	network.waitUntilSealingBlock(targetNodeInHighestEpoch.Nodes())

	// the target node should join now
	network.addNode(targetNode.NodeID[:]).start().sync()

	// the target node must sign within a bounded number of blocks, otherwise it never rejoined
	const maxBlocksUntilTargetSigns = 10
	var signed bool
	for i := 0; i < maxBlocksUntilTargetSigns && !signed; i++ {
		_, finalization := network.acceptNewBlock()
		signed = common.NodeIDs(finalization.QC.Signers()).IndexOf(targetNode.NodeID[:]) != -1
	}
	require.True(t, signed, "target node never signed a finalization within %d blocks", maxBlocksUntilTargetSigns)
}

// TestInstanceValidatorSkipsAnEpoch tests that a validator stops and starts being a validator
// It boots up as a non-validator then syncs to the highest epoch where it is a validator,
// then it is no longer a validator, and finally it is
func TestInstanceValidatorSkipsAnEpoch(t *testing.T) {
	validator := newNodeMapping(1)

	genesisSet := []metadata.NodeBLSMapping{validator}

	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)
	network.addNode(validator.NodeID[:]).start().sync()

	// The non-validator node syncs the accepted blocks and then contributes to the next blocks
	onOffValidator := newNodeMapping(2)
	network.addNode(onOffValidator.NodeID[:]).start().sync()

	// initiate an epoch change
	newValidatorSet := metadata.NodeBLSMappings{validator, onOffValidator}
	pChain.setValidatorSetAt(10, newValidatorSet)
	pChain.advanceHeight(10)

	network.waitUntilSealingBlock(newValidatorSet.Nodes())

	newValidatorSet = metadata.NodeBLSMappings{validator}
	pChain.setValidatorSetAt(20, newValidatorSet)
	pChain.advanceHeight(20)
	network.waitUntilSealingBlock(newValidatorSet.Nodes())

	// accept a new block to ensure both nodes are still syncing the chain
	network.acceptNewBlock()

	// initiate the final epoch change
	newValidatorSet = metadata.NodeBLSMappings{validator, onOffValidator}
	pChain.setValidatorSetAt(30, newValidatorSet)
	pChain.advanceHeight(30)

	network.waitUntilSealingBlock(newValidatorSet.Nodes())
}

func TestInstanceDoubleStartFails(t *testing.T) {
	validator := newNodeMapping(1)
	genesisSet := []metadata.NodeBLSMapping{validator}

	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)
	node := network.addNode(validator.NodeID[:]).start()
	require.ErrorIs(t, node.inst.Start(t.Context()), errAlreadyStarted)
}

// TestNonValidatorSkipsMSMVerification proves that a non-validator does not use the MSM to
// verify blocks: it commits a finalized block whose state machine transition is invalid.
func TestNonValidatorSkipsMSMVerification(t *testing.T) {
	validator := newNodeMapping(1)
	genesisValidatorSet := []metadata.NodeBLSMapping{validator}
	pChain := newTestPChain(genesisValidatorSet)

	nonValidator := newNodeMapping(2)
	network := newNetwork(t, pChain)
	network.addNode(validator.NodeID[:]).start().sync()
	nonValidatorNode := network.addNode(nonValidator.NodeID[:]).start().sync()

	parent, _, err := nonValidatorNode.storage.GetBlock(1)
	require.NoError(t, err)

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
	nonValidatorNode.storage.WaitForBlockCommit(2)
	committed, _, err := nonValidatorNode.storage.GetBlock(2)
	require.NoError(t, err)
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
				committed, _, err := laggingNode.storage.GetBlock(seq)
				require.NoError(t, err)
				require.Equal(t, block.Digest(), committed.Digest())
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Two validators, so a quorum is both of them: with its peer absent, the lagging
			// validator can neither build a block nor empty notarize a round on its own.
			lagging := newNodeMapping(1)
			peer := newNodeMapping(2)
			validators := metadata.NodeBLSMappings{lagging, peer}
			pChain := newTestPChain(validators)

			// The lagging validator holds genesis plus epoch 1's defining block, and lives on
			// a network of its own, so the replication response below is the only way it can
			// learn the block at the round it sits on.

			network := newNetwork(t, pChain)
			network.addNode(peer.NodeID[:]).start()
			laggingNode := network.addNode(lagging.NodeID[:]).start()
			network.sync()

			parent, _, err := laggingNode.storage.GetBlock(1)
			require.NoError(t, err)
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

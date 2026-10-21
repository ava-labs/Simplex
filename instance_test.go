// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"testing"
	"time"

	"github.com/ava-labs/simplex/avalanchego"
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
	node := network.addNode(validator.NodeID[:]).sync()

	isValidator, epochsReplicated := node.role()
	require.True(t, epochsReplicated, "a node already at the latest validator set has no epochs to replicate")
	require.True(t, isValidator)

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

// TestNonValidatorSyncs asserts a node outside the validator set syncs the chain when added
// to the network, and stays a non-validator once it has epochsReplicated.
func TestNonValidatorSyncs(t *testing.T) {
	validator := newNodeMapping(1)
	genesisSet := []metadata.NodeBLSMapping{validator}

	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)
	network.addNode(validator.NodeID[:])

	network.acceptNewBlock()

	nonValidator := newNodeMapping(2)
	node := network.addNode(nonValidator.NodeID[:])
	network.acceptNewBlock()
	node.sync()

	// ensure we replicated epochs and are not a validator
	isValidator, epochsReplicated := node.role()
	require.True(t, epochsReplicated)
	require.False(t, isValidator)
}

// TestNonValidatorBecomesValidator tests that an upcoming validator becomes a validator
// when an epoch change they are following is sealed. The non-validator must contribute it's approval in
// order to do so.
func TestNonValidatorBecomesValidator(t *testing.T) {
	validator := newNodeMapping(1)

	genesisSet := []metadata.NodeBLSMapping{validator}

	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)
	network.addNode(validator.NodeID[:]).sync()

	network.acceptNewBlock()

	// The non-validator node syncs the accepted blocks and then contributes to the next blocks
	upcomingValidator := newNodeMapping(2)
	network.addNode(upcomingValidator.NodeID[:]).sync()

	// initiate an epoch change
	newValidatorSet := metadata.NodeBLSMappings{validator, upcomingValidator}
	pChain.setValidatorSetAt(10, newValidatorSet)
	pChain.advanceHeight(10)

	network.waitUntilSealingBlock(newValidatorSet.Nodes())

	// a normal block should be signed by both the validators now
	_, finalization := network.acceptNewBlock()
	assertExpectedNodeIds(t, finalization.QC.Signers(), newValidatorSet.NodeIDs())
}

// TestInstanceDropsMessagesBeforeStart asserts that a message delivered before Start
// is dropped without error. A validator broadcasts the finalization only after
// indexing the block, so a node created on that commit can receive it before starting.
func TestInstanceDropsMessagesBeforeStart(t *testing.T) {
	validator := newNodeMapping(1)
	pChain := newTestPChain([]metadata.NodeBLSMapping{validator})
	instance := NewInstance(Config{
		LastNonSimplexInnerBlock: genesisBlock,
		PlatformChain:            pChain,
		Storage:                  newTestStorageWithGenesis(t),
		Logger:                   testutil.MakeLogger(t, 1),
		ID:                       validator.NodeID[:],
	})

	msg := &common.Message{Finalization: &common.Finalization{}}
	require.NoError(t, instance.HandleMessage(msg, validator.NodeID[:]))
}

// TestNonValidatorThresholdFollowsPChainTip asserts a non-validator counts sealing block votes
// against the validator set at the current P-chain tip. The tip grows while the node is offline,
// raising the threshold to two, instead of the one that was required when the non-validator joined.
func TestNonValidatorThresholdFollowsPChainTip(t *testing.T) {
	v1 := newNodeMapping(1)
	v2 := newNodeMapping(2)
	v3 := newNodeMapping(3)
	targetNode := newNodeMapping(42)

	pChain := newTestPChain(metadata.NodeBLSMappings{v1})
	network := newNetwork(t, pChain)
	network.addNode(v1.NodeID[:])
	network.addNode(v2.NodeID[:]).sync()
	network.addNode(v3.NodeID[:]).sync()

	// produce a few sealing blocks
	twoValidators := metadata.NodeBLSMappings{v1, v2}
	pChain.setValidatorSetAt(10, twoValidators)
	pChain.advanceHeight(10)
	network.waitUntilSealingBlock(twoValidators.Nodes())

	threeValidators := metadata.NodeBLSMappings{v1, v2, v3}
	pChain.setValidatorSetAt(20, threeValidators)
	pChain.advanceHeight(20)
	network.waitUntilSealingBlock(threeValidators.Nodes())

	// only v1 answers the target node from here on
	network.setOffline(v2.NodeID[:])
	network.setOffline(v3.NodeID[:])

	// our nonValidator joins when a threshold of votes for tip = 1
	network.setOffline(targetNode.NodeID[:])
	nonValidator := network.addNode(targetNode.NodeID[:])

	// Now the threshold of votes for tip is F(5)+1 = 2.
	fiveValidators := metadata.NodeBLSMappings{v1, v2, v3, newNodeMapping(4), newNodeMapping(5)}
	pChain.setValidatorSetAt(30, fiveValidators)
	pChain.advanceHeight(30)
	network.setOnline(targetNode.NodeID[:])

	// the next sealing block request goes out after simplex.DefaultReplicationRequestTimeout
	require.Never(t, func() bool {
		_, epochsReplicated := nonValidator.role()
		return epochsReplicated
	}, simplex.DefaultReplicationRequestTimeout+2*time.Second, 100*time.Millisecond)

	// Now we have 2 validators online so we should reach a threshold and finish replication.
	network.setOnline(v2.NodeID[:])
	require.Eventually(t, func() bool {
		_, epochsReplicated := nonValidator.role()
		return epochsReplicated
	}, 2*simplex.DefaultReplicationRequestTimeout, 100*time.Millisecond)
}

// TestValidator_ValidatorSetNotChanged tests that a P-chain height increase
// that does not have a unique validator set, does not create a new epoch
func TestValidator_ValidatorSetNotChanged(t *testing.T) {
	validator := newNodeMapping(1)

	genesisSet := []metadata.NodeBLSMapping{validator}

	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)
	node := network.addNode(validator.NodeID[:]).sync()

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
	network.addNode(validatorMapping.NodeID[:])
	network.addNode(leavingValidatorMapping.NodeID[:]).sync()

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

	node1 := network.addNode(v1.NodeID[:])
	network.addNode(v2.NodeID[:])
	network.addNode(v3.NodeID[:])
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
	network.addNode(validator1.NodeID[:])

	validator2 := newNodeMapping(2)
	validator3 := newNodeMapping(3)
	validator4 := newNodeMapping(4)
	network.addNode(validator2.NodeID[:]).sync()
	network.addNode(validator3.NodeID[:]).sync()
	network.addNode(validator4.NodeID[:]).sync()

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
	network.addNode(targetNode.NodeID[:]).sync()

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
	network.addNode(validator.NodeID[:]).sync()

	// The non-validator node syncs the accepted blocks and then contributes to the next blocks
	onOffValidator := newNodeMapping(2)
	network.addNode(onOffValidator.NodeID[:]).sync()

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

func TestInstanceRestartsAfterZeroBlock(t *testing.T) {
	validator := newNodeMapping(1)
	pChain := newTestPChain([]metadata.NodeBLSMapping{validator})
	network := newNetwork(t, pChain)

	// The lone validator builds and commits the zero block on its own.
	node := network.addNode(validator.NodeID[:]).sync()

	zeroBlock, _, err := node.storage.GetBlock(1)
	require.NoError(t, err)
	require.Equal(t, metadata.BlockTypeZero, zeroBlock.Type())
	require.Nil(t, zeroBlock.InnerBlock)

	node.restart()

	// The restarted node keeps the chain going.
	network.acceptNewBlock()
}

func TestInstanceDoubleStartFails(t *testing.T) {
	validator := newNodeMapping(1)
	genesisSet := []metadata.NodeBLSMapping{validator}

	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)
	node := network.addNode(validator.NodeID[:])
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
	network.addNode(validator.NodeID[:]).sync()
	nonValidatorNode := network.addNode(nonValidator.NodeID[:]).sync()

	parent, _, err := nonValidatorNode.storage.GetBlock(1)
	require.NoError(t, err)

	// The non-validator requires a  threshold of F(1)+1 responses for the first sealing block.
	// Otherwise it will block non-replication messages.
	sealing := &ParsedBlock{StateMachineBlock: parent.Clone()}
	sealingFinalization, _ := testutil.NewFinalizationRecord(t, &testutil.TestSignatureAggregator{N: 1}, sealing, []common.NodeID{validator.NodeID[:]})
	require.NoError(t, nonValidatorNode.inst.HandleMessage(&common.Message{
		ReplicationResponse: &common.ReplicationResponse{
			LatestSeq: &common.QuorumRound{Block: sealing, Finalization: &sealingFinalization},
		},
	}, validator.NodeID[:]))

	_, epochsReplicated := nonValidatorNode.role()
	require.True(t, epochsReplicated)

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
			network.addNode(peer.NodeID[:])
			laggingNode := network.addNode(lagging.NodeID[:])
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

// TestValidatorIndexes tests that a validator indexes and accepts a new block sent by the network
// It is the only validator, so it will build and finalize its own block.
func TestValidatorRequestsGenesis(t *testing.T) {
	validatorID := newNodeMapping(1)
	nonValidatorID := newNodeMapping(2)

	genesisSet := []metadata.NodeBLSMapping{validatorID}

	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)
	validator := network.addNode(validatorID.NodeID[:]).sync()

	// replication request for seq 0 (genesis)
	msg := &common.Message{
		ReplicationRequest: &common.ReplicationRequest{
			Seqs: []uint64{0},
		},
	}

	require.NoError(t, validator.inst.HandleMessage(msg, nonValidatorID.NodeID[:]))
}

func TestValidatorSetsMetadataFromSnowman(t *testing.T) {
	validatorID := newNodeMapping(1)
	numNonSimplexBlocks := uint64(10)
	genesisSet := []metadata.NodeBLSMapping{validatorID}

	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)
	network.seq = numNonSimplexBlocks
	storage := newTestStorage()

	var lastBlock avalanchego.VMBlock
	for i := range numNonSimplexBlocks {
		md := common.ProtocolMetadata{
			Epoch: 0, // non-simplex don't have an epoch
			Round: 0, // non-simplex blocks don't have rounds
			Seq:   uint64(i),
		}
		innerBlock := &testInnerBlock{Height_: uint64(i), TS: time.Now(), Payload: []byte{byte(i)}}
		pb := &ParsedBlock{StateMachineBlock: metadata.StateMachineBlock{InnerBlock: innerBlock, Metadata: metadata.StateMachineMetadata{
			SimplexProtocolMetadata: md,
		}}}
		require.NoError(t, storage.Index(t.Context(), pb, common.Finalization{}))
		lastBlock = innerBlock
	}

	config := nodeConfig{
		storage:             storage,
		lastNonSimplexBlock: lastBlock,
	}

	network.addNodeWithConfig(validatorID.NodeID[:], config).sync()

	block, _ := network.acceptNewBlock()
	require.Equal(t, numNonSimplexBlocks, block.BlockHeader().Epoch)
	require.Equal(t, uint64(1), block.BlockHeader().Round)
	require.Equal(t, numNonSimplexBlocks, block.BlockHeader().Seq)
}

// TestInstanceZeroBlockAfterPreSimplexBlocks brings up a network whose ledger already holds pre-Simplex
// blocks beyond the genesis block, and asserts that the zero block the network commits chains to the
// last non-Simplex block through that block's inner digest, at the sequence right above it.
func TestInstanceZeroBlockAfterPreSimplexBlocks(t *testing.T) {
	// The ledger holds pre-Simplex blocks from height 0 (genesis) to lastNonSimplexHeight.
	const lastNonSimplexHeight = uint64(3)
	zeroBlockSeq := lastNonSimplexHeight + 1

	// Timestamps lie in the past because the zero block carries over the last non-Simplex block's timestamp.
	preSimplexBlocks := make([]*testInnerBlock, 0, lastNonSimplexHeight+1)
	for h := uint64(0); h <= lastNonSimplexHeight; h++ {
		preSimplexBlocks = append(preSimplexBlocks, &testInnerBlock{
			Height_: h,
			TS:      time.Now().Add(-time.Duration(lastNonSimplexHeight-h+1) * time.Second),
			Payload: append([]byte("pre-simplex block "), byte('0'+h)),
		})
	}
	lastNonSimplexBlock := preSimplexBlocks[lastNonSimplexHeight]

	// Two validators, so neither can commit the zero block alone: the leader's proposal is only
	// committed once the other node has verified it against its own copy of the pre-Simplex chain.
	nodeA := newNodeMapping(1)
	nodeB := newNodeMapping(2)
	pChain := newTestPChain(metadata.NodeBLSMappings{nodeA, nodeB})

	network := newNetwork(t, pChain)
	// The zero block is built automatically at zeroBlockSeq; the first block the network asks for follows it.
	network.seq = zeroBlockSeq + 1

	for _, id := range []common.NodeID{nodeA.NodeID[:], nodeB.NodeID[:]} {
		network.addNodeWithConfig(id, nodeConfig{
			storage:             newTestStorageWithPreSimplexBlocks(t, preSimplexBlocks...),
			lastNonSimplexBlock: lastNonSimplexBlock,
		})
	}

	// Both nodes commit the zero block, then a couple of ordinary Simplex blocks on top of it.
	network.sync()
	network.acceptNewBlock()
	network.acceptNewBlock()

	for _, n := range network.nodesSnapshot() {
		zeroBlock, _, err := n.storage.GetBlock(zeroBlockSeq)
		require.NoError(t, err)
		require.Equal(t, metadata.BlockTypeZero, zeroBlock.Type())
		require.Nil(t, zeroBlock.InnerBlock)

		md := zeroBlock.Metadata.SimplexProtocolMetadata
		require.Equal(t, common.Digest(lastNonSimplexBlock.Digest()), md.Prev,
			"zero block must point to the last non-Simplex block by its inner digest")
		require.Equal(t, zeroBlockSeq, md.Seq)
	}
}

// TestValidatorReplicatesEpochsDuringTransition asserts a validator replicates epochs when
// its latest epoch is mid-transition. i.e. the latest pchain validator set disagrees
// with the validators latest index validator set.
func TestValidatorReplicatesEpochsDuringTransition(t *testing.T) {
	ourNodeMapping := newNodeMapping(1)
	v2 := newNodeMapping(2)
	v3 := newNodeMapping(3)
	v4 := newNodeMapping(4)
	futureValidator := newNodeMapping(5)
	genesisValidatorSet := metadata.NodeBLSMappings{ourNodeMapping, v2, v3, v4}
	futureValidatorSet := metadata.NodeBLSMappings{ourNodeMapping, v2, v3, v4, futureValidator}

	// The P-chain moved on to a set that contains ourNode, but no sealing block for it has
	// been indexed, so our indexed set and the latest set disagree.
	pChain := newTestPChain(genesisValidatorSet)

	network := newNetwork(t, pChain)
	network.addNode(futureValidator.NodeID[:])
	network.addNode(v2.NodeID[:])
	network.addNode(v3.NodeID[:])
	network.addNode(v4.NodeID[:])
	network.sync()

	// all validators are offline
	network.setOffline(v4.NodeID[:])
	network.setOffline(v3.NodeID[:])
	network.setOffline(v2.NodeID[:])

	// the future validator set is different than the current validator set ourNodeIsIn
	pChain.setValidatorSetAt(10, futureValidatorSet)
	pChain.advanceHeight(10)

	// the node joins, but because the pchain validator set is different than our epoch we will sync as a non-validator
	node := network.addNode(ourNodeMapping.NodeID[:])
	isValidator, epochsReplicated := node.role()
	require.False(t, epochsReplicated)
	require.False(t, isValidator, "a node whose indexed set is not the latest must replicate epochs first") // even though we are a validator

	// bring 2 node back online. The threshold for non-validators to complete epoch replication is 2 votes,
	// but to make a quorum is 3. This means the pchain transition will not occur, however the node should now sync as a validator
	network.setOnline(v2.NodeID[:])
	network.setOnline(v3.NodeID[:])
	node.sync()

	// The only way for the epoch transition to finish is if ourNode becomes a validator
	// and produces an approval & participates in the finalization.
	network.waitUntilSealingBlock(futureValidatorSet.Nodes())
	isValidator, _ = node.role()
	require.True(t, isValidator)
}

// TestInstanceBlacklistConfigurationRollsOverAcrossEpochs builds a chain across epochs that alternate between two and three
// validators, whose configured max blacklisted rounds differ, and then inspects the chain to ensure that the max blacklisted
// rounds roll over from one epoch to the next, and that each block moves them by at most one round per validator towards the
// configuration of the node that built it. Every epoch has at least two validators, so every block is verified by a
// validator other than the one that built it.
func TestInstanceBlacklistConfigurationRollsOverAcrossEpochs(t *testing.T) {
	v1 := newNodeMapping(1)
	v2 := newNodeMapping(2)
	v3 := newNodeMapping(3)

	// The first ever blacklist is taken from the configuration of its builder, so the validators of the first epoch agree on it.
	// v3 is configured differently, and is a validator only in the second epoch.
	maxBlacklistedRoundsByNode := map[string]uint16{
		string(v1.NodeID[:]): 24,
		string(v2.NodeID[:]): 24,
		string(v3.NodeID[:]): 60,
	}
	twoValidators := metadata.NodeBLSMappings{v1, v2}
	threeValidators := metadata.NodeBLSMappings{v1, v2, v3}

	pChain := newTestPChain(twoValidators)
	network := newNetwork(t, pChain)
	addNode := func(vdr metadata.NodeBLSMapping) *node {
		return network.addNodeWithConfig(vdr.NodeID[:], nodeConfig{maxBlacklistedRounds: maxBlacklistedRoundsByNode[string(vdr.NodeID[:])]})
	}

	// The first epoch is run by v1 and v2.
	node1 := addNode(v1)
	addNode(v2)
	network.sync()
	for range 4 {
		network.acceptNewBlock()
	}

	// v3 joins in the second epoch.
	addNode(v3).sync()
	pChain.setValidatorSetAt(10, threeValidators)
	pChain.advanceHeight(10)
	network.waitUntilSealingBlock(threeValidators.Nodes())
	for range 9 {
		network.acceptNewBlock()
	}

	// v3 leaves in the third epoch.
	pChain.setValidatorSetAt(20, twoValidators)
	pChain.advanceHeight(20)
	network.waitUntilSealingBlock(twoValidators.Nodes())
	for range 8 {
		network.acceptNewBlock()
	}

	// Inspect the chain: every block moves the max blacklisted rounds of its parent, possibly from the previous epoch,
	// by at most one round per validator towards the configuration of the node that built it.
	// The first ever blacklist has v1's and v2's configuration.
	validators := map[uint16]common.NodeIDs{2: twoValidators.NodeIDs(), 3: threeValidators.NodeIDs()}
	rounds := uint16(24)
	var epochSizes []uint16
	for seq := uint64(1); seq < network.seq; seq++ {
		block := node1.storage.WaitForBlockCommit(seq)
		blacklist := block.Blacklist()
		nodeCount := blacklist.NodeCount()
		if len(epochSizes) == 0 || epochSizes[len(epochSizes)-1] != nodeCount {
			epochSizes = append(epochSizes, nodeCount)
		}

		leader := simplex.LeaderForRound(validators[nodeCount], block.BlockHeader().Round)
		target := maxBlacklistedRoundsByNode[string(leader)]
		if rounds < target {
			rounds += min(target-rounds, nodeCount)
		} else {
			rounds -= min(rounds-target, nodeCount)
		}
		require.Equal(t, rounds, blacklist.MaxBlacklistedRounds(), "block %d built by %s", seq, leader)
	}

	require.Equal(t, []uint16{2, 3, 2}, epochSizes)
	// The last epoch is run by v1 and v2, so the chain ends up with their configuration.
	require.Equal(t, uint16(24), rounds)
}

// TestInstanceMaxBlacklistedRoundsRollingRestart restarts the validators one after another with a higher max blacklisted rounds,
// and then again with a lower one, while the chain keeps growing. It then inspects the chain to ensure that each block moves
// the max blacklisted rounds by at most one round per validator towards the configuration of the node that built it at the time,
// and that the chain ends up with the configuration all nodes were eventually restarted with.
func TestInstanceMaxBlacklistedRoundsRollingRestart(t *testing.T) {
	const (
		initialRounds   = 20
		increasedRounds = 32
		decreasedRounds = 12
	)

	genesisSet := metadata.NodeBLSMappings{newNodeMapping(1), newNodeMapping(2), newNodeMapping(3), newNodeMapping(4)}
	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)

	// configured holds the max blacklisted rounds each node is currently configured with.
	configured := make(map[string]uint16)
	nodes := make([]*node, len(genesisSet))
	for i, vdr := range genesisSet {
		nodes[i] = network.addNodeWithConfig(vdr.NodeID[:], nodeConfig{maxBlacklistedRounds: initialRounds})
		configured[string(vdr.NodeID[:])] = initialRounds
	}
	network.sync()

	// targets holds the max blacklisted rounds each block's builder was configured with when it built the block.
	// The first simplex block is built while the nodes sync, with the initial configuration.
	nodeCount := uint16(len(genesisSet))
	targets := map[uint64]uint16{1: initialRounds}
	acceptNewBlocks := func(count int) {
		for range count {
			block, _ := network.acceptNewBlock()
			leader := simplex.LeaderForRound(genesisSet.NodeIDs(), block.BlockHeader().Round)
			targets[block.BlockHeader().Seq] = configured[string(leader)]
		}
	}

	acceptNewBlocks(4)
	for _, maxBlacklistedRounds := range []uint16{increasedRounds, decreasedRounds} {
		for i := range nodes {
			nodes[i] = nodes[i].restartWithNodeConfig(nodeConfig{maxBlacklistedRounds: maxBlacklistedRounds})
			configured[string(nodes[i].id)] = maxBlacklistedRounds
			acceptNewBlocks(4)
		}
		acceptNewBlocks(8)
	}

	// Inspect the chain: every block moves the max blacklisted rounds of its parent by up to nodeCount rounds
	// towards its target, without overshooting it.
	// The first ever blacklist has no parent, it is taken from the initial configuration.
	prevRounds := uint16(initialRounds)
	var highest uint16
	for seq := uint64(1); seq < network.seq; seq++ {
		block := nodes[0].storage.WaitForBlockCommit(seq)
		blacklist := block.Blacklist()
		expected := prevRounds
		if target := targets[seq]; expected < target {
			expected += min(target-expected, nodeCount)
		} else {
			expected -= min(expected-target, nodeCount)
		}
		require.Equal(t, expected, blacklist.MaxBlacklistedRounds(), "block %d", seq)
		prevRounds = expected
		highest = max(highest, expected)
	}

	// The chain reached the increased configuration, and ended up with the decreased one.
	require.Equal(t, uint16(increasedRounds), highest)
	require.Equal(t, uint16(decreasedRounds), prevRounds)
}

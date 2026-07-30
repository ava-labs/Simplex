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
	"github.com/ava-labs/simplex/wal"
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

// TestInstanceGarbageCollectsWAL asserts a validator garbage collects its WAL every
// walGCInterval rounds, so the WAL never holds an entry from a round below the last
// round it garbage collected at.
func TestInstanceGarbageCollectsWAL(t *testing.T) {
	const targetBlocks = uint64(100)

	validator := newNodeMapping(1)
	// The validator set never changes, so the node commits every block on its own
	// and the chain stays in a single Simplex epoch throughout.
	pChain := newTestPChain([]metadata.NodeBLSMapping{validator})
	network := newNetwork(t, pChain)

	// A tiny WAL size makes the WAL rotate upon every entry, so garbage collection
	// discards entries round by round rather than in large batches.
	params := paramConfig
	params.WALMaxSizeBytes = 10
	node := network.addNodeWithConfig(validator.NodeID[:], nodeConfig{params: &params}).sync()

	// collectRoundsFromWAL returns the rounds of the entries the node's WAL currently
	// holds, across all the WAL files it rotated through. Garbage collected files are
	// deleted, hence contribute no entries.
	collectRoundsFromWAL := func() []uint64 {
		var reader common.WALReader
		var rounds []uint64

		node.wals.lock.Lock()
		defer node.wals.lock.Unlock()
		for _, w := range node.wals.wals {
			entries, err := w.ReadAll()
			require.NoError(t, err)
			for _, entry := range entries {
				round, err := reader.Round(entry)
				require.NoError(t, err)
				rounds = append(rounds, round)
			}
		}
		return rounds
	}

	var garbageCollectedRoundThreshold uint64
	for range targetBlocks {
		block, _ := network.acceptNewBlock()
		round := block.BlockHeader().Round

		// When we commit a sealing block we wipe out the WAL entirely, so ignore this corner case.
		// We only care about the ordinary blocks because we're testing regular garbage collection of the WAL.
		// We also ignore the first walGCInterval rounds because the WAL is not garbage collected until after that.
		if block.SealingBlockInfo() != nil || round < walGCInterval {
			continue
		}

		// A block is indexed, which is what the commit reports, before the garbage
		// collection it triggers runs. The garbage collection of the previous interval
		// has certainly completed by now though, so verify the WAL against it.
		roundsFromWAL := collectRoundsFromWAL()
		require.NotEmpty(t, roundsFromWAL) // Just a check that the below for loop doesn't iterate over an empty slice.
		for _, r := range roundsFromWAL {
			require.GreaterOrEqual(t, r, garbageCollectedRoundThreshold,
				"WAL holds an entry of round %d although it was garbage collected at round %d", r, garbageCollectedRoundThreshold)
		}

		// We garbage collect the WAL every walGCInterval rounds,
		// but we want to check the GC one round after the GC because we don't want the WAL to be empty when we check it.
		// The WAL purges all entries of rounds below the round before this round.
		if round > walGCInterval && round%walGCInterval == 1 {
			garbageCollectedRoundThreshold = round - 1
		}
	}

	// Committing targetBlocks blocks takes at least as many rounds, so the chain ran
	// long enough for the WAL to have been garbage collected several times over.
	require.GreaterOrEqual(t, garbageCollectedRoundThreshold, targetBlocks-2*walGCInterval)

	// The garbage collection kept the entries of the rounds at and above the round it garbage collected at.
	require.NotEmpty(t, collectRoundsFromWAL())
}

// TestInstanceRestartSkipsSealedEpochWALRecords emulates a crash right after a sealing block
// was persisted, but before the WAL was wiped for the new epoch. The WAL then still holds the
// sealed epoch's records: the sealing block's own, and the Telocks that extended the epoch
// until the sealing block finalized. Telock rounds collide with the new epoch's first rounds,
// so replaying them would drag the node back into the sealed epoch. The restarted node must
// ignore them, start in the new epoch, and keep committing blocks.
//
// The WAL is the node's own: the test delays the finalization of the sealing block so that the
// validators extend the sealed epoch with Telocks, and then takes the WAL as the node wiped it
// when it moved to the new epoch, which is what a crash in that window would have left behind.
func TestInstanceRestartSkipsSealedEpochWALRecords(t *testing.T) {
	validator := newNodeMapping(1)
	leavingValidator := newNodeMapping(2)
	genesisSet := metadata.NodeBLSMappings{validator, leavingValidator}
	pChain := newTestPChain(genesisSet)
	network := newNetwork(t, pChain)
	node := network.addNode(validator.NodeID[:])
	otherNode := network.addNode(leavingValidator.NodeID[:]).sync()
	network.acceptNewBlock()

	// Hold back the finalize votes for the sealing block and for whatever follows it in the
	// sealed epoch. The sealing block is then notarized but not finalized, so the leaders
	// must extend the epoch with Telocks. The sender has written the block to its WAL by the
	// time it votes to finalize it, so the block's type is read from there.
	walsOf := map[string]*walCreator{
		string(node.id):      node.wals,
		string(otherNode.id): otherNode.wals,
	}
	extendsSealedEpoch := func(from common.NodeID, round uint64) bool {
		for _, block := range walsOf[string(from)].blocksInWAL() {
			if block.Metadata.SimplexProtocolMetadata.Round != round {
				continue
			}
			blockType := block.Type()
			return blockType == metadata.BlockTypeSealing || blockType == metadata.BlockTypeTelock
		}
		return false
	}
	network.holdMessages(func(from common.NodeID, msg *common.Message) bool {
		switch {
		case msg.FinalizeVote != nil:
			return extendsSealedEpoch(from, msg.FinalizeVote.Finalization.Round)
		case msg.Finalization != nil:
			return extendsSealedEpoch(from, msg.Finalization.Finalization.Round)
		default:
			return false
		}
	})

	// Seal the first epoch by shrinking the validator set to our node alone, which then
	// carries the new epoch on its own.
	newValidatorSet := metadata.NodeBLSMappings{validator}
	pChain.setValidatorSetAt(10, newValidatorSet)
	pChain.advanceHeight(10)

	// Wait until our node holds a sealing block and a Telock built on top of it.
	telocksInWAL := func(blocks []metadata.StateMachineBlock) (sealingRound uint64, telocks int) {
		for _, block := range blocks {
			switch block.Type() {
			case metadata.BlockTypeSealing:
				sealingRound = block.Metadata.SimplexProtocolMetadata.Round
			case metadata.BlockTypeTelock:
				telocks++
			}
		}
		return sealingRound, telocks
	}
	require.Eventually(t, func() bool {
		_, telocks := telocksInWAL(node.wals.blocksInWAL())
		return telocks > 0
	}, 30*time.Second, 10*time.Millisecond, "the sealed epoch was never extended by a Telock")

	// Let the finalize votes through. The sealing block finalizes, our node commits it, moves to
	// the new epoch and wipes its WAL, which the WAL creator records for us.
	network.releaseHeldMessages()
	sealingBlock := network.waitUntilSealingBlock(newValidatorSet.Nodes())
	sealingHeader := sealingBlock.BlockHeader()
	sealedEpoch, newEpoch := sealingHeader.Epoch, sealingHeader.Seq
	require.NotEqual(t, sealedEpoch, newEpoch)

	require.Eventually(t, func() bool {
		node.inst.lock.Lock()
		defer node.inst.lock.Unlock()
		return node.inst.e != nil && node.inst.e.Metadata().Epoch == newEpoch
	}, 20*time.Second, 10*time.Millisecond, "the node never moved to the new epoch")

	// From here on only our node matters; the one that left must not interfere with its restart.
	network.setOffline(leavingValidator.NodeID[:])
	node.stop()
	// A crash right after the sealing block was persisted leaves it at the tip.
	require.Equal(t, sealingHeader.Seq, node.storage.NumBlocks()-1)

	// The WAL the node wiped is what it would have found on restart after such a crash:
	// the sealing block and the Telocks that extended its epoch, all in the sealed epoch.
	staleRecords := node.wals.deletedRecords()
	staleSealingRound, staleTelocks := telocksInWAL(parseBlockRecords(t, staleRecords))
	require.Equal(t, sealingHeader.Round, staleSealingRound)
	require.Positive(t, staleTelocks)
	var reader common.WALReader
	for _, record := range staleRecords {
		epoch, err := reader.Epoch(record)
		require.NoError(t, err)
		require.Equal(t, sealedEpoch, epoch)
	}

	staleWAL := testutil.NewTestWAL(t)
	for _, record := range staleRecords {
		require.NoError(t, staleWAL.Append(record))
	}

	// Come back up over the same storage with the stale WAL. Starting is where the WAL is replayed.
	restarted := network.addNodeWithConfig(validator.NodeID[:], nodeConfig{
		wals: []wal.DeletableWAL{staleWAL}, storage: node.storage, existingNode: true,
	})

	restarted.inst.lock.Lock()
	epoch := restarted.inst.e
	restarted.inst.lock.Unlock()
	require.NotNil(t, epoch, "the node should have restarted as a validator")

	// The node starts in the new epoch, past the sealing round, not in the epoch the stale records belong to.
	md := epoch.Metadata()
	require.Equal(t, newEpoch, md.Epoch)
	require.Greater(t, md.Round, sealingHeader.Round)

	// It keeps committing blocks in the new epoch, at the sequence right after the sealing block,
	// which the first Telock would have occupied had it been replayed.
	network.pending.addPendingBlock()
	nextSeq := sealingHeader.Seq + 1
	require.Eventually(t, func() bool {
		return restarted.storage.NumBlocks() > nextSeq
	}, 20*time.Second, 10*time.Millisecond, "the restarted node never committed a block in the new epoch")

	block, _, err := restarted.storage.Retrieve(nextSeq)
	require.NoError(t, err)
	require.Equal(t, newEpoch, block.BlockHeader().Epoch)
	require.Equal(t, nextSeq, block.BlockHeader().Seq)
	require.Equal(t, sealingHeader.Digest, block.BlockHeader().Prev)
	require.Equal(t, metadata.BlockTypeNormal, block.(*ParsedBlock).Type())
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

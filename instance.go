// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ava-labs/simplex/avalanchego"
	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/ava-labs/simplex/nonvalidator"
	"github.com/ava-labs/simplex/simplex"
	"github.com/ava-labs/simplex/wal"
	"go.uber.org/zap"
)

const (
	// tickInterval is the interval at which the instance will call AdvanceTime on the current epoch or non-validator.
	tickInterval = time.Millisecond * 100
)

type Config struct {
	// LastNonSimplexInnerBlock is the last non-simplex inner block that was persisted to storage.
	// This is used to determine the current epoch and validator set.
	LastNonSimplexInnerBlock avalanchego.VMBlock
	// ParameterConfig is the configuration for the simplex instance.
	ParameterConfig ParameterConfig
	// PlatformChain is the interface to the P-chain.
	PlatformChain PlatformChain
	// Broadcaster is the interface to broadcast messages to other nodes in the network.
	Broadcaster Broadcaster
	// CryptoOps is the interface to the cryptographic operations needed by the simplex instance.
	CryptoOps CryptoOps
	// WalCreator is the interface to create new write-ahead logs for the simplex instance.
	WalCreator wal.Creator
	// Storage is the interface to the block storage layer for the simplex instance.
	Storage Storage
	Logger  common.Logger
	Sender  Sender
	WALs    []wal.DeletableWAL
	VM      VM
	ID      common.NodeID
}
type epochChange struct {
	epoch      uint64
	validators common.Nodes
}

type timeAdvancer interface {
	AdvanceTime(t time.Time)
}

type Instance struct {
	Config Config

	lock         sync.Mutex
	started      bool
	cs           *CachedStorage
	wal          *wal.GarbageCollectedWAL
	msm          *metadata.StateMachine
	e            *simplex.Epoch
	nv           *nonvalidator.NonValidator
	epochOrNV    timeAdvancer
	epochChanges chan epochChange
	stopCh       chan struct{}
}

func NewInstance(config Config) *Instance {
	return &Instance{
		Config:       config,
		stopCh:       make(chan struct{}),
		cs:           NewCachedStorage(config.Storage),
		epochChanges: make(chan epochChange, 1),
	}
}

func (i *Instance) Start(ctx context.Context) error {
	// Hold the lock throughout startup to block HandleMessage from being called in between.
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.started {
		return fmt.Errorf("instance already started")
	}

	i.started = true

	context.AfterFunc(ctx, i.Stop)

	nodes, epochNum, err := i.getLastAcceptedEpochAndValidatorSet()
	if err != nil {
		return fmt.Errorf("error determining latest epoch and validator set: %w", err)
	}

	if err := i.startAtEpoch(nodes, epochNum); err != nil {
		return fmt.Errorf("error starting instance at epoch %d: %w", epochNum, err)
	}

	go i.tick()
	go i.listenForEpochChanges()

	return nil
}

func (i *Instance) startValidator(epoch uint64, validators common.Nodes) error {
	epochConfig, err := i.createEpochConfig(epoch, validators)
	if err != nil {
		return err
	}
	return i.startEpoch(epochConfig)
}

func (i *Instance) startNonValidator(epoch uint64) error {
	config, err := i.createNonValidatorConfig(epoch)
	if err != nil {
		return err
	}

	nonValidator, err := nonvalidator.NewNonValidator(config)
	if err != nil {
		return fmt.Errorf("error creating non-validator: %w", err)
	}
	i.nv = nonValidator
	i.epochOrNV = nonValidator
	nonValidator.Start()
	return nil
}

func (i *Instance) createNonValidatorConfig(epochNum uint64) (nonvalidator.Config, error) {
	source, err := simplex.NewRandomSource()
	if err != nil {
		return nonvalidator.Config{}, err
	}

	nodes, err := GetHighestValidatorSet(i.Config.PlatformChain)
	if err != nil {
		return nonvalidator.Config{}, err
	}
	comm := newCommunication(i.Config.Sender, i.Config.Broadcaster, nodes)

	epochAwareStorage := &EpochAwareStorage{
		epoch:   epochNum,
		Storage: i.Config.Storage,
		onEpochChange: func(epoch uint64, validators common.Nodes) error {
			// set the communication to the highest validator set, since this node is a non-validator and may be behind.
			nodes, err := GetHighestValidatorSet(i.Config.PlatformChain)
			if err != nil {
				return err
			}
			comm.SetValidators(nodes)
			i.notifyEpochChange(epoch, validators)

			return nil
		},
	}

	// Plant an artificial MSM that just skips verification.
	i.msm = &metadata.StateMachine{
		Config: &metadata.Config{
			SkipMSMVerification: true,
		},
	}
	i.cs.msm = i.msm

	config := nonvalidator.Config{
		ID:                         i.Config.ID,
		RandomSource:               source,
		Storage:                    epochAwareStorage,
		Comm:                       comm,
		Logger:                     i.Config.Logger,
		StartTime:                  time.Now(),
		SignatureAggregatorCreator: i.Config.CryptoOps.CreateSignatureAggregator,
		MaxSequenceWindow:          simplex.DefaultMaxRoundWindow,
	}
	return config, nil
}

func (i *Instance) notifyEpochChange(epoch uint64, validators common.Nodes) {
	select {
	case i.epochChanges <- epochChange{
		epoch:      epoch,
		validators: validators,
	}:
	case <-i.stopCh:
		// If the instance is stopped, we don't need to notify about epoch changes.
		return
	}
}

func (i *Instance) tick() {
	ticker := time.NewTicker(tickInterval)
	for {
		select {
		case now := <-ticker.C:
			i.lock.Lock()
			timeAdvancer := i.epochOrNV
			i.lock.Unlock()
			if timeAdvancer != nil {
				timeAdvancer.AdvanceTime(now)
			}
		case <-i.stopCh:
			return
		}
	}
}

func (i *Instance) isStopped() bool {
	select {
	case <-i.stopCh:
		return true
	default:
		return false
	}
}

func (i *Instance) Stop() {
	i.lock.Lock()
	defer i.lock.Unlock()

	select {
	case <-i.stopCh:
		// Already stopped, do nothing
		return
	default:
		close(i.stopCh)
	}

	i.stopValidator()
	i.stopNonValidator()
}

func (i *Instance) stopNonValidator() {
	if i.nv != nil {
		i.nv.Stop()
		i.nv = nil
		i.epochOrNV = nil
	}
}

func (i *Instance) stopValidator() {
	if i.e != nil {
		i.e.Stop()
		i.e = nil
		i.epochOrNV = nil
	}
}

func (i *Instance) HandleMessage(msg *common.Message, from common.NodeID) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	// We need to artificially wire the MSM and the cache to the block,
	// in order to intercept the Verify() call.
	switch {
	case msg.BlockMessage != nil:
		err := i.wireBlockMessage(msg)
		if err != nil {
			i.Config.Logger.Debug("Error wiring block message", zap.Error(err))
			return nil
		}
	case msg.ReplicationResponse != nil:
		err := i.wireReplicationResponse(msg)
		if err != nil {
			i.Config.Logger.Debug("Error wiring replication response message", zap.Error(err))
			return nil
		}
	}

	if i.e != nil {
		return i.e.HandleMessage(msg, from)
	}

	if i.nv != nil {
		return i.nv.HandleMessage(msg, from)
	}
	return nil
}

func (i *Instance) wireReplicationResponse(msg *common.Message) error {
	resp := msg.ReplicationResponse
	if resp.LatestRound != nil && resp.LatestRound.Block != nil {
		block, err := i.wireBlock(resp.LatestRound.Block)
		if err != nil {
			return err
		}
		resp.LatestRound.Block = block
	}
	if resp.LatestSeq != nil && resp.LatestSeq.Block != nil {
		block, err := i.wireBlock(resp.LatestSeq.Block)
		if err != nil {
			return err
		}
		resp.LatestSeq.Block = block
	}
	for j, datum := range resp.Data {
		if datum.Block == nil {
			continue
		}
		block, err := i.wireBlock(datum.Block)
		if err != nil {
			return err
		}
		resp.Data[j].Block = block
	}
	return nil
}

func (i *Instance) wireBlock(block common.Block) (common.Block, error) {
	pb, isParsedBlock := block.(*ParsedBlock)
	if !isParsedBlock {
		return nil, fmt.Errorf("expected ParsedBlock, got %T", block)
	}
	block = &cachedBlock{
		cache:       i.cs,
		ParsedBlock: pb,
	}
	pb.msm = i.msm
	return block, nil
}

func (i *Instance) wireBlockMessage(msg *common.Message) error {
	block, err := i.wireBlock(msg.BlockMessage.Block)
	if err != nil {
		return err
	}
	msg.BlockMessage.Block = block
	return nil
}

func (i *Instance) listenForEpochChanges() {
	for {
		select {
		case epochChange := <-i.epochChanges:
			i.processEpochChange(epochChange)
		case <-i.stopCh:
			return
		}
	}
}

func (i *Instance) processEpochChange(epochChange epochChange) {
	if i.isStopped() {
		i.Config.Logger.Info("instance is already stopped, skipping epoch change")
		return
	}

	var err error
	switch {
	case i.e != nil && i.nv != nil:
		i.Config.Logger.Fatal("We are running both a validator and non-validator")
		return
	case i.nv != nil:
		err = i.transitionEpochNonValidator(epochChange)
	case i.e != nil:
		err = i.transitionEpochValidator(epochChange)
	default: // This should never happen, but we log it just in case.
		i.Config.Logger.Fatal("We are not running either a validator or non-validator")
		return
	}

	if err != nil {
		i.Config.Logger.Error("Error transitioning epoch", zap.Error(err))
		i.Stop()
	}
}

// startEpoch starts a new epoch with the given configuration.
// Must be called under the lock, and assumes that the previous epoch has been stopped (if any).
func (i *Instance) startEpoch(epochConfig simplex.EpochConfig) error {
	epoch, err := simplex.NewEpoch(epochConfig)
	if err != nil {
		return fmt.Errorf("error creating simplex epoch: %w", err)
	}
	epoch.Epoch = epochConfig.Epoch
	i.e = epoch
	i.epochOrNV = epoch

	return epoch.Start()
}

func (i *Instance) lastBlock() (metadata.StateMachineBlock, uint64, error) {
	numBlocks := i.Config.Storage.NumBlocks()
	if numBlocks == 0 {
		return metadata.StateMachineBlock{}, 0, fmt.Errorf("no genesis block found in storage")
	}

	lastBlock, _, err := i.Config.Storage.GetBlock(numBlocks - 1)
	if err != nil {
		return metadata.StateMachineBlock{}, 0, fmt.Errorf("error retrieving last block from storage: %w", err)
	}

	return lastBlock, numBlocks, nil
}

func (i *Instance) createEpochConfig(epoch uint64, validators common.Nodes) (simplex.EpochConfig, error) {
	wal, err := wal.NewGarbageCollectedWAL(i.Config.WALs, i.Config.WalCreator, &common.WALRetentionReader{}, i.Config.ParameterConfig.WALMaxEntryCount)
	if err != nil {
		return simplex.EpochConfig{}, fmt.Errorf("error creating garbage collected wal: %w", err)
	}
	i.wal = wal

	// We might have crashed right after a sealing block was persisted to storage,
	// but before the WAL was garbage collected.
	// In that case, we need to garbage collect the WAL to remove all entries from previous epochs.
	if err := i.maybeGarbageCollectWAL(); err != nil {
		return simplex.EpochConfig{}, err
	}

	msm, err := metadata.NewStateMachine(&metadata.Config{
		GetTime:                         time.Now,
		MyNodeID:                        i.Config.ID,
		KeyAggregator:                   i.Config.CryptoOps,
		GetValidatorSet:                 i.Config.PlatformChain.GetValidatorSet,
		SignatureVerifier:               i.Config.CryptoOps,
		PChainProgressListener:          i.Config.PlatformChain,
		LatestPersistedHeight:           i.Config.Storage.NumBlocks(),
		MaxBlockBuildingWaitTime:        i.Config.ParameterConfig.MaxNetworkDelay,
		Logger:                          i.Config.Logger,
		Signer:                          i.Config.CryptoOps,
		GenesisValidatorSet:             i.Config.PlatformChain.GenesisValidatorSet(),
		LastNonSimplexBlockPChainHeight: i.Config.LastNonSimplexInnerBlock.Height(),
		SignatureAggregatorCreator:      i.Config.CryptoOps.CreateSignatureAggregator,
		BlockBuilder:                    i.Config.VM,
		LastNonSimplexInnerBlock:        i.Config.LastNonSimplexInnerBlock,
		GetPChainHeightForProposing:     i.Config.PlatformChain.GetMinimumHeight,
		GetPChainHeightForVerifying:     i.Config.PlatformChain.GetCurrentHeight,
		AuxiliaryInfoApp:                &NoopAuxiliaryInfoApp{},
		ComputeICMEpoch:                 i.Config.VM.ComputeICMEpoch,
		GetBlock:                        i.cs.RetrieveBlock,
	})
	if err != nil {
		return simplex.EpochConfig{}, fmt.Errorf("error creating metadata state machine: %w", err)
	}

	i.msm = msm
	i.cs.msm = msm

	source, err := simplex.NewRandomSource()
	if err != nil {
		return simplex.EpochConfig{}, err
	}

	blockBuilder := &BlockBuilderWaiter{vm: i.Config.VM, msm: msm}

	comm := newCommunication(i.Config.Sender, i.Config.Broadcaster, validators)

	epochAwareStorage := &EpochAwareStorage{
		msm:     msm,
		epoch:   epoch,
		Storage: i.cs,
		onEpochChange: func(epoch uint64, validators common.Nodes) error {
			blockBuilder.stop()
			comm.SetValidators(validators)
			i.notifyEpochChange(epoch, validators)
			return nil
		},
	}

	epochConfig := simplex.EpochConfig{
		Epoch:              epoch,
		ReplicationEnabled: true,
		StartTime:          time.Now(),
		// TODO: For simplicity, we use the same value for all timeouts. If needed we can expand the config.
		MaxProposalWait:            i.Config.ParameterConfig.MaxNetworkDelay * 2, // 1 proposal + 1 vote
		MaxRebroadcastWait:         i.Config.ParameterConfig.MaxNetworkDelay * 2,
		FinalizeRebroadcastTimeout: i.Config.ParameterConfig.MaxNetworkDelay * 2,
		MaxRoundWindow:             i.Config.ParameterConfig.MaxRoundWindow,
		ID:                         i.Config.ID,
		RandomSource:               source, // Seed the random source from crypto/rand
		WAL:                        wal,
		Logger:                     i.Config.Logger,
		SignatureAggregatorCreator: i.Config.CryptoOps.CreateSignatureAggregator,
		QCDeserializer:             i.Config.CryptoOps,
		Signer:                     i.Config.CryptoOps,
		Verifier:                   i.Config.CryptoOps,
		Storage:                    epochAwareStorage,
		Comm:                       comm,
		BlockBuilder:               blockBuilder,
		BlockDeserializer:          &blockDeserializer{vm: i.Config.VM, msm: msm},
	}
	return epochConfig, nil
}

func (i *Instance) maybeGarbageCollectWAL() error {
	lastBlock, _, err := i.lastBlock()
	if err != nil {
		return fmt.Errorf("error retrieving last block: %w", err)
	}

	if lastBlock.Metadata.SimplexEpochInfo.BlockValidationDescriptor != nil {
		i.Config.Logger.Info("Last block is a sealing block, garbage collecting all WALs preceding it to start a new epoch")
		// We figure out the round number of the latest block and garbage collect all WALs preceding it.
		// TODO: We need to test a scenario where an epoch change occurred and then a few notarizations have been persisted to WAL,
		// but no block has been finalized. So the WAL contains entries from previous epochs as well as from the current epoch.
		// TODO: We need to test a scenario where an epoch change occurred but the node has crashed after notarizing some Telocks.
		md, err := common.ProtocolMetadataFromBytes(lastBlock.Metadata.SimplexProtocolMetadata)
		if err != nil {
			return fmt.Errorf("error parsing protocol metadata from last block: %w", err)
		}
		if err := i.wal.GarbageCollect(md.Round); err != nil {
			return fmt.Errorf("error garbage collecting WALs: %w", err)
		}
	}
	return nil
}

func (i *Instance) transitionEpochNonValidator(epochChange epochChange) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if !i.isValidatorForLatestEpoch(epochChange.epoch, epochChange.validators) {
		i.Config.Logger.Debug("Skipping restarting a non-validator because I am not a validator yet")
		return nil
	}

	// Stop the non-validator before doing anything else, so that we don't process any more messages while we are changing epochs.
	i.stopNonValidator()

	return i.startAtEpoch(epochChange.validators, epochChange.epoch)
}

// isValidatorForLatestEpoch returns if this instance is a validator for the highest validator set
func (i *Instance) isValidatorForLatestEpoch(epoch uint64, newValidatorSet common.Nodes) bool {
	if i.nv != nil {
		highestEpoch, highestValidatorSet := i.nv.HighestValidatedEpoch()
		return highestValidatorSet.Contains(i.Config.ID) && highestEpoch == epoch
	}

	// TODO: this assumes newValidatorSet & epoch are the highest, which may not hold.
	// Validators should collect a threshold of votes for the highest epoch, like non-validators do when starting.
	return newValidatorSet.Contains(i.Config.ID)
}

// startAtEpoch starts either a validator or non-validator at epoch.
func (i *Instance) startAtEpoch(validators common.Nodes, epoch uint64) error {
	if i.isValidatorForLatestEpoch(epoch, validators) {
		if err := i.startValidator(epoch, validators); err != nil {
			i.Config.Logger.Error("Error starting validator on epoch change", zap.Error(err))
			return err
		}
		return nil
	}

	if err := i.startNonValidator(epoch); err != nil {
		i.Config.Logger.Error("Error starting non-validator on epoch change", zap.Error(err))
		return err
	}
	return nil
}

func (i *Instance) transitionEpochValidator(epochChange epochChange) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	// Stop the epoch before doing anything else, so that we don't process any more messages while we are changing epochs.
	i.stopValidator()
	// Wipe out the WALs from the config so we won't try to load them again
	i.Config.WALs = nil
	// On epoch change, garbage collect the WAL to remove all entries from previous epochs.
	if err := i.wal.GarbageCollect(math.MaxUint64); err != nil {
		i.Config.Logger.Error("Error garbage collecting epoch config on epoch change", zap.Error(err))
	}

	return i.startAtEpoch(epochChange.validators, epochChange.epoch)
}

// getLastAcceptedEpoch determines the epoch the instance should start at based on
// the last block in storage. If the ledger only contains non-Simplex blocks, the
// epoch is the first Simplex height. If the last block is a sealing block, the
// epoch it seals has ended, so the next epoch is returned. Otherwise, the epoch
// of the last block is returned.
func (i *Instance) getLastAcceptedEpochAndValidatorSet() (common.Nodes, uint64, error) {
	lastBlock, numBlocks, err := i.lastBlock()
	if err != nil {
		return nil, 0, fmt.Errorf("error retrieving last block: %w", err)
	}

	lastNonSimplexHeight := i.Config.LastNonSimplexInnerBlock.Height()
	parsedLastBlock := ParsedBlock{StateMachineBlock: lastBlock}
	epochNum := parsedLastBlock.BlockHeader().Epoch
	genesisValidatorSet := i.Config.PlatformChain.GenesisValidatorSet()

	var validatorSet metadata.NodeBLSMappings
	var nodes common.Nodes

	switch {
	// If all we have in the ledger is non-Simplex blocks, load the validator set from genesis
	case lastNonSimplexHeight+1 == numBlocks:
		validatorSet = genesisValidatorSet
		nodes = validatorSetToNodes(genesisValidatorSet)
		epochNum = lastNonSimplexHeight + 1
		i.Config.Logger.Debug("Determined epoch and validator set from genesis (ledger holds only non-Simplex blocks)",
			zap.Uint64("epoch", epochNum))
	// If the last block persisted is a sealing block, then we are in the next epoch.
	case lastBlock.SealingBlockInfo() != nil:
		epochNum = parsedLastBlock.BlockHeader().Seq
		validatorSet = constructValidatorSetFromSealingBlock(parsedLastBlock)
		nodes = lastBlock.SealingBlockInfo().ValidatorSet
		i.Config.Logger.Debug("Determined epoch and validator set from sealing block at tip",
			zap.Uint64("epoch", epochNum))
	// Else, we have at least one Simplex block in the ledger, and it's not a sealing block.
	default:
		// Therefore, the sequence of the sealing block is the epoch number.
		sealingBlockSeq := parsedLastBlock.BlockHeader().Epoch
		sealingBlock, _, err := i.Config.Storage.GetBlock(sealingBlockSeq)
		if err != nil {
			return nil, 0, fmt.Errorf("error retrieving sealing block from storage: %w", err)
		}
		if sealingBlock.Metadata.SimplexEpochInfo.BlockValidationDescriptor == nil {
			return nil, 0, fmt.Errorf("expected sealing block at seq %d, but got a non-sealing block", sealingBlockSeq)
		}
		validatorSet = constructValidatorSetFromSealingBlock(ParsedBlock{StateMachineBlock: sealingBlock})
		nodes = validatorSetToNodes(validatorSet)
		i.Config.Logger.Debug("Determined epoch and validator set from sealing block in storage",
			zap.Uint64("epoch", epochNum), zap.Uint64("sealingBlockSeq", sealingBlockSeq))
	}
	return nodes, epochNum, nil
}

func validatorSetToNodes(validatorSet metadata.NodeBLSMappings) common.Nodes {
	var nodes common.Nodes
	for _, vdr := range validatorSet {
		nodes = append(nodes, common.Node{
			Id:     vdr.NodeID[:],
			Weight: vdr.Weight,
			PK:     vdr.BLSKey,
		})
	}
	return nodes
}

func constructValidatorSetFromSealingBlock(lastBlock ParsedBlock) metadata.NodeBLSMappings {
	var validatorSet metadata.NodeBLSMappings
	vdrs := lastBlock.Metadata.SimplexEpochInfo.BlockValidationDescriptor.AggregatedMembership.Members
	for _, vdr := range vdrs {
		validatorSet = append(validatorSet, metadata.NodeBLSMapping{
			NodeID: vdr.NodeID,
			BLSKey: vdr.BLSKey,
			Weight: vdr.Weight,
		})
	}
	return validatorSet
}

func GetHighestValidatorSet(platform PlatformChain) (common.Nodes, error) {
	height := platform.GetCurrentHeight()
	mappings, err := platform.GetValidatorSet(height)
	if err != nil {
		return nil, err
	}

	return mappings.Nodes(), nil
}

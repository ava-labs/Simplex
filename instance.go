// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

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
	LastNonSimplexInnerBlock metadata.VMBlock
	// ParameterConfig is the configuration for the simplex instance.
	ParameterConfig ParameterConfig
	// PlatformChain is the interface to the P-chain.
	PlatformChain PlatformChain
	// CryptoOps is the interface to the cryptographic operations needed by the simplex instance.
	CryptoOps CryptoOps
	// Storage is the interface to the block storage layer for the simplex instance.
	Storage     Storage
	Logger      common.Logger
	Sender      Sender
	Broadcaster Broadcaster
	WALs        []wal.DeletableWAL
	VM          VM
	ID          common.NodeID
}

type MessageHandler interface {
}

type nodeRole byte

const (
	nonValidator nodeRole = iota
	validator
)

type epochChange struct {
	epochNum   uint64
	validators common.Nodes
	nodeRole   nodeRole
}

type timeAdvancer interface {
	AdvanceTime(t time.Time)
}

type Instance struct {
	Config                Config
	lock                  sync.Mutex
	cs                    *CachedStorage
	wal                   *wal.GarbageCollectedWAL
	msm                   *metadata.StateMachine
	e                     *simplex.Epoch
	nv                    *nonvalidator.NonValidator
	epochOrNV             timeAdvancer
	epochChanges          chan epochChange
	stopCh                chan struct{}
	epochChangeSupression epochChangeSupression
}

func (i *Instance) Start(ctx context.Context) error {
	// Hold the lock throughout startup to block HandleMessage from being called in between.
	i.lock.Lock()
	defer i.lock.Unlock()

	i.stopCh = make(chan struct{})
	i.epochChanges = make(chan epochChange)
	context.AfterFunc(ctx, i.Stop)

	cachedStorage := NewCachedStorage(i.Config.Storage)
	i.cs = cachedStorage

	lastBlock, numBlocks, err := i.lastBlock()

	lastNonSimplexHeight := i.Config.LastNonSimplexInnerBlock.Height()
	genesisValidatorSet := i.Config.PlatformChain.GenesisValidatorSet()
	nodes, epochNum, err := constructEpochAndValidatorSet(lastNonSimplexHeight, genesisValidatorSet, numBlocks, ParsedBlock{StateMachineBlock: lastBlock}, i.Config.Storage)
	if err != nil {
		return fmt.Errorf("error determining latest epoch and validator set: %w", err)
	}

	iAmValidator := i.determineValidatorOrNot(nodes)

	if iAmValidator {
		if err := i.startValidator(); err != nil {
			return fmt.Errorf("error starting validator: %w", err)
		}
	} else {
		if err := i.startNonValidator(epochNum, nodes); err != nil {
			return fmt.Errorf("error starting non-validator: %w", err)
		}
	}

	go i.tick()
	go i.listenForEpochChanges()

	return nil
}

func (i *Instance) startValidator() error {
	epochConfig, err := i.createEpochConfig()
	if err != nil {
		return err
	}

	if err := i.startEpoch(epochConfig); err != nil {
		return err
	}

	return nil
}

func (i *Instance) startNonValidator(epochNum uint64, validators common.Nodes) error {
	config, err := i.createNonValidatorConfig(epochNum, validators)
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

func (i *Instance) createNonValidatorConfig(epochNum uint64, validators common.Nodes) (nonvalidator.Config, error) {
	source, err := simplex.NewRandomSource()
	if err != nil {
		return nonvalidator.Config{}, err
	}

	comm := &Communication{Sender: i.Config.Sender, Broadcaster: i.Config.Broadcaster, epochChangeSupression: &i.epochChangeSupression}
	comm.SetValidators(validators)

	epochAwareStorage := &EpochAwareStorage{
		Epoch:   epochNum,
		Storage: i.Config.Storage,
		OnEpochChange: func(epoch uint64, validators common.Nodes) error {
			i.epochChangeSupression.setSupression(epoch) // The epoch number is also the sealing block sequence.
			i.notifyEpochChange(epoch, validators, nonValidator)
			comm.SetValidators(validators)
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
		MaxSequenceWindow:          nonvalidator.DefaultMaxSequenceWindow,
	}
	return config, nil
}

func (i *Instance) notifyEpochChange(epoch uint64, validators common.Nodes, role nodeRole) {
	select {
	case i.epochChanges <- epochChange{
		epochNum:   epoch,
		validators: validators,
		nodeRole:   role,
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

	if i.e != nil {
		i.stopValidator()
	}
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

	if i.epochChangeSupression.isSupressionActive() {
		return nil
	}

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
		pb, isParsedBlock := resp.LatestRound.Block.(*ParsedBlock)
		if !isParsedBlock {
			return fmt.Errorf("expected ParsedBlock, got %T", resp.LatestRound.Block)
		}
		resp.LatestRound.Block = &cachedBlock{
			cache:       i.cs,
			ParsedBlock: pb,
		}
		pb.msm = i.msm
	}
	if resp.LatestSeq != nil && resp.LatestSeq.Block != nil {
		pb, isParsedBlock := resp.LatestSeq.Block.(*ParsedBlock)
		if !isParsedBlock {
			return fmt.Errorf("expected ParsedBlock, got %T", resp.LatestSeq.Block)
		}
		resp.LatestSeq.Block = &cachedBlock{
			cache:       i.cs,
			ParsedBlock: pb,
		}
		pb.msm = i.msm
	}
	for j, datum := range resp.Data {
		if datum.Block == nil {
			continue
		}
		pb, isParsedBlock := datum.Block.(*ParsedBlock)
		if !isParsedBlock {
			return fmt.Errorf("expected ParsedBlock, got %T", datum.Block)
		}
		resp.Data[j].Block = &cachedBlock{
			cache:       i.cs,
			ParsedBlock: pb,
		}
		pb.msm = i.msm
	}
	return nil
}

func (i *Instance) wireBlockMessage(msg *common.Message) error {
	pb, isParsedBlock := msg.BlockMessage.Block.(*ParsedBlock)
	if !isParsedBlock {
		return fmt.Errorf("expected ParsedBlock, got %T", msg.BlockMessage.Block)
	}
	msg.BlockMessage.Block = &cachedBlock{
		cache:       i.cs,
		ParsedBlock: pb,
	}
	pb.msm = i.msm
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
	switch epochChange.nodeRole {
	case nonValidator:
		i.transitionEpochNonValidator(epochChange)
	case validator:
		i.transitionEpochValidator(epochChange)
	default: // This should never happen, but we log it just in case.
		i.Config.Logger.Fatal("Unknown node role on epoch change",
			zap.String("role", fmt.Sprintf("%v", epochChange.nodeRole)))
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

func (i *Instance) determineValidatorOrNot(nodes common.Nodes) bool {
	for _, node := range nodes {
		if i.Config.ID.Equals(node.Id) {
			return true
		}
	}
	return false
}

func (i *Instance) createEpochConfig() (simplex.EpochConfig, error) {
	lastBlock, numBlocks, err := i.lastBlock()

	lastNonSimplexHeight := i.Config.LastNonSimplexInnerBlock.Height()
	genesisValidatorSet := i.Config.PlatformChain.GenesisValidatorSet()
	nodes, epochNum, err := constructEpochAndValidatorSet(lastNonSimplexHeight, genesisValidatorSet, numBlocks, ParsedBlock{StateMachineBlock: lastBlock}, i.Config.Storage)
	if err != nil {
		return simplex.EpochConfig{}, err
	}

	wal, err := wal.NewGarbageCollectedWAL(i.Config.WALs, i.Config.Storage.CreateWAL, &common.WALRetentionReader{}, i.Config.ParameterConfig.WALMaxEntryCount)
	if err != nil {
		return simplex.EpochConfig{}, fmt.Errorf("error creating garbage collected wal: %w", err)
	}
	i.wal = wal

	// We might have crashed right after a sealing block was persisted to storage,
	// but before the WAL was garbage collected.
	// In that case, we need to garbage collect the WAL to remove all entries from previous epochs.
	if err := i.maybeGarbageCollectWAL(lastBlock); err != nil {
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
		GenesisValidatorSet:             genesisValidatorSet,
		LastNonSimplexBlockPChainHeight: lastNonSimplexHeight,
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

	blockBuilder := &BlockBuilderWaiter{vm: i.Config.VM, msm: msm, epochChangeSupression: &i.epochChangeSupression}

	comm := &Communication{Sender: i.Config.Sender, Broadcaster: i.Config.Broadcaster, epochChangeSupression: &i.epochChangeSupression}
	comm.SetValidators(nodes)

	epochAwareStorage := &EpochAwareStorage{
		msm:     msm,
		Epoch:   epochNum,
		Storage: i.cs,
		OnEpochChange: func(epoch uint64, validators common.Nodes) error {
			i.epochChangeSupression.setSupression(epoch) // The epoch number is also the sealing block sequence.
			blockBuilder.stop()
			comm.SetValidators(validators)
			i.notifyEpochChange(epoch, validators, validator)
			return nil
		},
	}

	epochConfig := simplex.EpochConfig{
		Epoch:              epochNum,
		ReplicationEnabled: true,
		StartTime:          time.Now(),
		// TODO: For simpicity, we use the same value for all timeouts. If needed we can expand the config.
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

func (i *Instance) maybeGarbageCollectWAL(lastBlock metadata.StateMachineBlock) error {
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

func (i *Instance) transitionEpochNonValidator(epochChange epochChange) {
	i.lock.Lock()
	defer i.lock.Unlock()

	// Stop the non-validator before doing anything else, so that we don't process any more messages while we are changing epochs.
	i.stopNonValidator()

	i.epochChangeSupression.clearSupression()

	// First, figure out if I'm still a validator.
	if i.determineValidatorOrNot(epochChange.validators) {
		i.Config.Logger.Info("I am now a validator")
		i.startValidator()
		return
	}

	i.startNonValidator(epochChange.epochNum, epochChange.validators)
}

func (i *Instance) transitionEpochValidator(epochChange epochChange) {
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

	i.epochChangeSupression.clearSupression()

	// First, figure out if I'm still a validator.
	if !i.determineValidatorOrNot(epochChange.validators) {
		i.Config.Logger.Info("I am no longer a validator")
		i.startNonValidator(epochChange.epochNum, epochChange.validators)
		return
	}

	config, err := i.createEpochConfig()
	if err != nil {
		i.Config.Logger.Error("Error creating epoch config on epoch change", zap.Error(err))
		return
	}

	if config.Epoch != epochChange.epochNum {
		i.Config.Logger.Error("Epoch number mismatch on epoch change", zap.Uint64("expected", epochChange.epochNum), zap.Uint64("actual", config.Epoch))
		return
	}

	if err := i.startEpoch(config); err != nil {
		i.Config.Logger.Error("Error starting new epoch on epoch change", zap.Error(err))
	}
}

func constructEpochAndValidatorSet(lastNonSimplexInnerBlockHeight uint64, genesisValidatorSet metadata.NodeBLSMappings, numBlocks uint64, lastBlock ParsedBlock, storage Storage) (common.Nodes, uint64, error) {
	epochNum := lastBlock.BlockHeader().Epoch

	var validatorSet metadata.NodeBLSMappings
	var nodes common.Nodes

	switch {
	// If all we have in the ledger is non-Simplex blocks, load the validator set from genesis
	case lastNonSimplexInnerBlockHeight+1 == numBlocks:
		validatorSet = genesisValidatorSet
		nodes = validatorSetToNodes(genesisValidatorSet)
		epochNum = lastNonSimplexInnerBlockHeight + 1
	// If the last block persisted is a sealing block, then we are in the next h.
	case lastBlock.SealingBlockInfo() != nil:
		epochNum = lastBlock.BlockHeader().Seq
		validatorSet = constructValidatorSetFromSealingBlock(lastBlock)
		nodes = lastBlock.SealingBlockInfo().ValidatorSet
	// Else, we have at least one Simplex block in the ledger, and it's not a sealing block.
	default:
		// Therefore, the sequence of the sealing block is the h number.
		sealingBlockSeq := lastBlock.BlockHeader().Epoch
		sealingBlock, _, err := storage.GetBlock(sealingBlockSeq)
		if err != nil {
			return nil, 0, fmt.Errorf("error retrieving sealing block from storage: %w", err)
		}
		if sealingBlock.Metadata.SimplexEpochInfo.BlockValidationDescriptor == nil {
			return nil, 0, fmt.Errorf("expected sealing block at seq %d, but got a non-sealing block", sealingBlockSeq)
		}
		validatorSet = constructValidatorSetFromSealingBlock(ParsedBlock{StateMachineBlock: sealingBlock})
		nodes = validatorSetToNodes(validatorSet)
	}
	return nodes, epochNum, nil
}

func validatorSetToNodes(genesisValidatorSet metadata.NodeBLSMappings) common.Nodes {
	var nodes common.Nodes
	for _, vdr := range genesisValidatorSet {
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

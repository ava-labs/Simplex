// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"errors"
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

var errAlreadyStarted = errors.New("instance already started")

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
	// Sender is an interface to send messages to a specific node in the network
	Sender Sender
	// CryptoOps is the interface to the cryptographic operations needed by the simplex instance.
	CryptoOps CryptoOps
	// WalCreator is the interface to create new write-ahead logs for the simplex instance.
	WalCreator wal.Creator
	// Storage is the interface to the block storage layer for the simplex instance.
	Storage        Storage
	Logger         common.Logger
	WALs           []wal.DeletableWAL
	VM             VM
	ICMETransition metadata.ICMEpochTransition
	ID             common.NodeID
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
	Config       Config
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
		return errAlreadyStarted
	}

	i.started = true

	context.AfterFunc(ctx, i.Stop)

	nodes, epochNum, err := getLastAcceptedEpochAndValidatorSet(&i.Config)
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

func (i *Instance) startValidator() error {
	epochConfig, err := i.createEpochConfig()
	if err != nil {
		return err
	}
	return i.startEpoch(epochConfig)
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

	comm := &Communication{Sender: i.Config.Sender, Broadcaster: i.Config.Broadcaster}
	comm.SetValidators(validators)

	epochAwareStorage := &EpochAwareStorage{
		CachedStorage: i.cs,
		epoch:         epochNum,
		onEpochChange: func(epoch uint64, validators common.Nodes) error {
			height := i.Config.PlatformChain.GetCurrentHeight()
			vdrs, err := i.Config.PlatformChain.GetValidatorSet(height)
			if err != nil {
				i.Config.Logger.Error("error getting validator set", zap.Error(err))
				return fmt.Errorf("error getting validator set from platform chain: %w", err)
			}
			comm.SetValidators(validators)
			if i.iAmValidator(vdrs.Nodes()) {
				i.notifyEpochChange(epoch, validators, nonValidator)
			} else {
				i.Config.Logger.Debug("I am still a non-validator at the tip of the P-chain, skipping role change",
					zap.Uint64("height", height))
			}
			return nil
		},
	}

	// Plant an artificial MSM. A non-validator never verifies the state machine transition,
	// it only verifies the inner block (see common.OnlyVMVerifyOpt), so this MSM is only
	// used to wire blocks and is never asked to verify them.
	i.msm = &metadata.StateMachine{
		Config: &metadata.Config{},
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
	var err error
	switch epochChange.nodeRole {
	case nonValidator:
		err = i.transitionEpochNonValidator(epochChange)
	case validator:
		err = i.transitionEpochValidator(epochChange)
	default: // This should never happen, but we log it just in case.
		i.Config.Logger.Fatal("Unknown node role on epoch change",
			zap.String("role", fmt.Sprintf("%v", epochChange.nodeRole)))
		return
	}
	if err != nil {
		i.Config.Logger.Error("Error transitioning epoch", zap.Uint8("role", uint8(epochChange.nodeRole)), zap.Error(err))
		i.Stop()
	}
}

// startEpoch starts a new epoch with the given configuration.
// Must be called under the lock, and assumes that the previous epoch has been stopped (if any).
func (i *Instance) startEpoch(epochConfig *epochConfig) error {
	epoch, err := simplex.NewEpoch(epochConfig.EpochConfig)
	if err != nil {
		return fmt.Errorf("error creating simplex epoch: %w", err)
	}
	epoch.Epoch = epochConfig.Epoch
	i.e = epoch
	i.epochOrNV = epoch
	epochConfig.bbw.e = epoch
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

func (i *Instance) iAmValidator(nodes common.Nodes) bool {
	for _, node := range nodes {
		if i.Config.ID.Equals(node.Id) {
			return true
		}
	}
	return false
}

func (i *Instance) createEpochConfig() (*epochConfig, error) {
	lastBlock, _, err := i.lastBlock()
	if err != nil {
		return nil, err
	}

	genesisValidatorSet := i.Config.PlatformChain.GenesisValidatorSet()
	nodes, epochNum, err := getLastAcceptedEpochAndValidatorSet(&i.Config)
	if err != nil {
		return nil, err
	}

	wal, err := wal.NewGarbageCollectedWAL(i.Config.WALs, i.Config.WalCreator, &common.WALRetentionReader{}, i.Config.ParameterConfig.WALMaxEntryCount)
	if err != nil {
		return nil, fmt.Errorf("error creating garbage collected wal: %w", err)
	}
	i.wal = wal

	// We might have crashed right after a sealing block was persisted to storage,
	// but before the WAL was garbage collected.
	// In that case, we need to garbage collect the WAL to remove all entries from previous epochs.
	if err := i.maybeGarbageCollectWAL(lastBlock); err != nil {
		return nil, err
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
		LastNonSimplexBlockPChainHeight: i.Config.PlatformChain.LastNonSimplexBlockPChainHeight(),
		SignatureAggregatorCreator:      i.Config.CryptoOps.CreateSignatureAggregator,
		BlockBuilder:                    i.Config.VM,
		LastNonSimplexInnerBlock:        i.Config.LastNonSimplexInnerBlock,
		GetPChainHeightForProposing:     i.Config.PlatformChain.GetMinimumHeight,
		GetPChainHeightForVerifying:     i.Config.PlatformChain.GetCurrentHeight,
		AuxiliaryInfoApp:                &NoopAuxiliaryInfoApp{},
		ComputeICMEpoch:                 i.Config.ICMETransition,
		GetBlock:                        i.cs.RetrieveBlock,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating metadata state machine: %w", err)
	}

	i.msm = msm
	i.cs.msm = msm

	source, err := simplex.NewRandomSource()
	if err != nil {
		return nil, err
	}

	blockBuilder := &BlockBuilderWaiter{vm: i.Config.VM, msm: msm}

	comm := &Communication{Sender: i.Config.Sender, Broadcaster: i.Config.Broadcaster}
	comm.SetValidators(nodes)

	epochAwareStorage := &EpochAwareStorage{
		CachedStorage: i.cs,
		msm:           msm,
		epoch:         epochNum,
		onEpochChange: func(epoch uint64, validators common.Nodes) error {
			blockBuilder.stop()
			comm.SetValidators(validators)
			i.notifyEpochChange(epoch, validators, validator)
			return nil
		},
	}

	ec := simplex.EpochConfig{
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
		BlockDeserializer:          &blockDeserializer{vm: i.Config.VM, cs: i.cs},
	}
	return &epochConfig{
		EpochConfig: ec,
		bbw:         blockBuilder,
	}, nil
}

func (i *Instance) maybeGarbageCollectWAL(lastBlock metadata.StateMachineBlock) error {
	if lastBlock.Metadata.SimplexEpochInfo.BlockValidationDescriptor != nil {
		i.Config.Logger.Info("Last block is a sealing block, garbage collecting all WALs preceding it to start a new epoch")
		// We figure out the round number of the latest block and garbage collect all WALs preceding it.
		// TODO: We need to test a scenario where an epoch change occurred and then a few notarizations have been persisted to WAL,
		// but no block has been finalized. So the WAL contains entries from previous epochs as well as from the current epoch.
		// TODO: We need to test a scenario where an epoch change occurred but the node has crashed after notarizing some Telocks.
		md := lastBlock.Metadata.SimplexProtocolMetadata
		if err := i.wal.GarbageCollect(md.Round); err != nil {
			return fmt.Errorf("error garbage collecting WALs: %w", err)
		}
	}
	return nil
}

func (i *Instance) transitionEpochNonValidator(epochChange epochChange) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.isStopped() {
		i.Config.Logger.Info("instance is already stopped, skipping epoch change")
		return nil
	}

	if !i.iAmValidator(epochChange.validators) {
		i.Config.Logger.Debug("Skipping restarting a non-validator because I am not a validator yet")
		return nil
	}

	// Stop the non-validator before doing anything else, so that we don't process any more messages while we are changing epochs.
	i.stopNonValidator()

	return i.startAtEpoch(epochChange.validators, epochChange.epochNum)
}

func (i *Instance) startAtEpoch(validators common.Nodes, epoch uint64) error {
	if i.iAmValidator(validators) {
		if err := i.startValidator(); err != nil {
			i.Config.Logger.Error("Error starting validator on epoch change", zap.Error(err))
			return err
		}
		return nil
	}

	if err := i.startNonValidator(epoch, validators); err != nil {
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

	return i.startAtEpoch(epochChange.validators, epochChange.epochNum)
}

type epochConfig struct {
	simplex.EpochConfig
	bbw *BlockBuilderWaiter
}

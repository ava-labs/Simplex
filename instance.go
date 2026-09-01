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

type epochChange struct {
	epoch      uint64
	validators common.Nodes
}
type timeAdvancer interface {
	AdvanceTime(t time.Time)
}

type Instance struct {
	Config Config

	lock               sync.Mutex
	started            bool
	cs                 *CachedStorage
	transitionListener *epochTransitionListener
	wal                *wal.GarbageCollectedWAL
	msm                *metadata.StateMachine
	e                  *simplex.Epoch
	nv                 *nonvalidator.NonValidator
	epochOrNV          timeAdvancer
	epochChanges       chan epochChange
	stopCh             chan struct{}

	// bootstrapped represents whether the instance has completed bootstrapping.
	// This is false on Start, and also set to false when our notices it's validator
	// has fallen behind by many epochs.
	bootstrapped bool
}

func NewInstance(config Config) *Instance {
	cs := NewCachedStorage(config.Storage)
	// Non-validators have no block builder, so they pass a nil approval handler:
	// they broadcast approvals but do not need to record their own locally.
	transitionListener := newEpochTransitionListener(
		config.Logger,
		config.Sender,
		avalanchego.NodeID(config.ID),
		config.PlatformChain.GetValidatorSet,
		cs.RetrieveBlock,
		config.CryptoOps,
		&NoopAuxiliaryInfoApp{}, // TODO: set this in the config
		nil,
	)

	return &Instance{
		Config:             config,
		stopCh:             make(chan struct{}),
		epochChanges:       make(chan epochChange, 1),
		cs:                 cs,
		transitionListener: transitionListener,
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

	if err := i.bootstrap(); err != nil {
		return err
	}

	go i.tick()
	go i.listenForEpochChanges()

	return nil
}

func (i *Instance) bootstrap() error {
	latestValidatorSet, err := getLatestPlatformChainValidatorSet(i.Config.PlatformChain)
	if err != nil {
		return err
	}

	latestIndexedEpochValidators, epochNum, err := getLastAcceptedEpochAndValidatorSet(&i.Config)
	if err != nil {
		return err
	}

	// We have indexed the latest validator set, therefore we can skip bootstrapping and start as a validator.
	// Note: this may not be the latest epoch, but our futureEpochCollector will eventually notice we are behind and transition properly.
	if latestIndexedEpochValidators.Equal(latestValidatorSet.Nodes()) && latestValidatorSet.Nodes().Contains(i.Config.ID) {
		i.bootstrapped = true
		return i.startValidator(epochNum, latestIndexedEpochValidators)
	}

	// Start as non-validator if our last indexed validator set does not equal, the latest p-chain validator set
	// Note: the epoch may be transitioning, so the latest p-chain validator set actually points to a future epoch.
	// The non-validator should finish bootstrapping and convert our non-validator to a validator in this case.
	return i.startNonValidator()
}

func (i *Instance) onBootstrapFinish(highestKnownEpoch uint64, highestKnownValidators common.Nodes) error {
	i.bootstrapped = true
	i.Config.Logger.Debug(
		"Node finished bootstrapping",
		zap.Stringers("Highest Validators",
			highestKnownValidators.NodeIDs()),
		zap.Uint64("Highest Epoch", highestKnownEpoch),
	)

	_, lastAcceptedEpoch, err := getLastAcceptedEpochAndValidatorSet(&i.Config)
	if err != nil {
		return err
	}

	// the latest epoch contains our node, we should asynchronously notify the listener
	// which will convert our node to a validator for this epoch.
	if highestKnownValidators.Contains(i.Config.ID) && lastAcceptedEpoch == highestKnownEpoch {
		i.Config.Logger.Debug("Our node completed bootstrapping and it is a validator")
		i.notifyEpochChange(highestKnownEpoch, highestKnownValidators)
		return nil
	}

	// we have completed bootstrapping, but our node is still a non-validator
	i.Config.Logger.Debug("Our node completed bootstrapping but it is not a validator")
	return nil
}

func (i *Instance) startValidator(epochNum uint64, validators common.Nodes) error {
	epochConfig, err := i.createEpochConfig(epochNum, validators)
	if err != nil {
		return err
	}

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

func (i *Instance) startNonValidator() error {
	config, err := i.createNonValidatorConfig()
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

func (i *Instance) createNonValidatorConfig() (nonvalidator.Config, error) {
	source, err := simplex.NewRandomSource()
	if err != nil {
		return nonvalidator.Config{}, err
	}

	latestValidatorSet, err := getLatestPlatformChainValidatorSet(i.Config.PlatformChain)
	if err != nil {
		return nonvalidator.Config{}, err
	}

	comm := newCommunication(i.Config.Sender, i.Config.Broadcaster, latestValidatorSet.Nodes())

	// Plant an artificial MSM. A non-validator never verifies the state machine transition,
	// it only verifies the inner block (see common.OnlyVMVerifyOpt), so this MSM is only
	// used to wire blocks and is never asked to verify them.
	i.msm = &metadata.StateMachine{
		Config: &metadata.Config{},
	}
	i.cs.msm = i.msm
	instanceStorage := NewCallbackStorage(i.cs, i.msm, func(block *ParsedBlock) error {
		switch {
		case block.Type() == metadata.BlockTypeTransitioning:
			if err := i.transitionListener.handleTransitionBlock(block); err != nil {
				return err
			}
		}
		return nil
	})

	config := nonvalidator.Config{
		ID:                         i.Config.ID,
		RandomSource:               source,
		Storage:                    instanceStorage,
		Comm:                       comm,
		Logger:                     i.Config.Logger,
		StartTime:                  time.Now(),
		SignatureAggregatorCreator: i.Config.CryptoOps.CreateSignatureAggregator,
		MaxSequenceWindow:          simplex.DefaultMaxRoundWindow,
		TransitionToValidator:      i.notifyEpochChange,
		OnFinishBootstrapping:      i.onBootstrapFinish,
		Bootstrapped:               i.bootstrapped,
	}
	return config, nil
}

func (i *Instance) notifyEpochChange(epoch uint64, validators common.Nodes) {
	i.Config.Logger.Debug("Notifying the instance of an epoch change", zap.Uint64("Epoch", epoch), zap.Stringers("Validators", validators.NodeIDs()))
	ec := epochChange{
		epoch:      epoch,
		validators: validators,
	}

	for {
		select {
		case i.epochChanges <- ec:
			return
		// The slot holds a stale epoch change: take it, keep the newer of the two and retry.
		case pending := <-i.epochChanges:
			if pending.epoch > ec.epoch {
				ec = pending
			}
		case <-i.stopCh:
			// If the instance is stopped, we don't need to notify about epoch changes.
			return
		}
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

	i.stopValidator(false)
	i.stopNonValidator()
}

func (i *Instance) stopNonValidator() {
	if i.nv != nil {
		i.nv.Stop()
		i.nv = nil
		i.epochOrNV = nil
	}
}

func (i *Instance) stopValidator(garbageCollectWAL bool) {
	if i.e != nil {
		i.e.Stop()
		// Wipe out the WALs from the config so we won't try to load them again
		if garbageCollectWAL {
			i.Config.WALs = nil
			// On epoch change, garbage collect the WAL to remove all entries from previous epochs.
			if err := i.wal.GarbageCollect(math.MaxUint64); err != nil {
				i.Config.Logger.Error("Error garbage collecting epoch config on epoch change", zap.Error(err))
			}
		}

		i.e = nil
		i.epochOrNV = nil
	}
}

func (i *Instance) HandleMessage(msg *common.Message, from common.NodeID) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	select {
	case <-i.stopCh:
		i.Config.Logger.Debug("Instance is stopped, dropping message")
		return nil
	default:
	}

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

	if i.nv != nil {
		return i.nv.HandleMessage(msg, from)
	}

	if i.e != nil {
		i.handleMessageForEpoch(msg, from)
	}

	return nil
}

func (i *Instance) handleMessageForEpoch(msg *common.Message, from common.NodeID) error {

	switch {
	case msg.AuxiliaryInfo != nil:
		if msg.AuxiliaryInfo.Epoch != i.e.Epoch {
			i.Config.Logger.Debug(
				"Received an auxiliary info from an old epoch",
				zap.Uint64("Aux Info Epoch", msg.AuxiliaryInfo.Epoch),
				zap.Uint64("Our Epoch", i.e.Epoch),
				zap.Stringer("From", from))
			return nil
		}
		i.msm.HandleAuxiliaryInfo(*msg.AuxiliaryInfo, avalanchego.NodeID(from))
	case msg.EpochTransitionApproval != nil:
		// TODO: pass in time.Now() rather than uint64
		i.msm.HandleApproval(msg.EpochTransitionApproval, uint64(time.Now().UnixMilli()))
		return nil
	}
	return i.e.HandleMessage(msg, from)
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
	// Hold the lock so the transition cannot interleave with Stop or HandleMessage.
	i.lock.Lock()

	if i.isStopped() {
		i.lock.Unlock()
		i.Config.Logger.Info("instance is already stopped, skipping epoch change")
		return
	}

	var err error

	runningNonValidator := i.nv != nil
	runningValidator := i.e != nil

	switch {
	case runningNonValidator && runningValidator:
		i.lock.Unlock()
		i.Config.Logger.Fatal("We are running both a validator or non-validator")
		return
	case runningNonValidator:
		// Stop the non-validator before doing anything else, so that we don't process any more messages while we are changing epochs.
		i.stopNonValidator()
		err = i.startAtEpoch(epochChange.validators, epochChange.epoch)
	case runningValidator:
		i.stopValidator(true)
		err = i.startAtEpoch(epochChange.validators, epochChange.epoch)
	default: // This should never happen, but we log it just in case.
		i.lock.Unlock()
		i.Config.Logger.Fatal("We are not running either a validator or non-validator")
		return
	}
	i.lock.Unlock()

	if err != nil {
		i.Config.Logger.Error("Error transitioning epoch", zap.Error(err))
		i.Stop()
	}
}

func (i *Instance) createEpochConfig(epoch uint64, validators common.Nodes) (*epochConfig, error) {
	wal, err := wal.NewGarbageCollectedWAL(i.Config.WALs, i.Config.WalCreator, &common.WALRetentionReader{}, i.Config.ParameterConfig.WALMaxSizeBytes)
	if err != nil {
		return nil, fmt.Errorf("error creating garbage collected wal: %w", err)
	}
	i.wal = wal

	// We might have crashed right after a sealing block was persisted to storage,
	// but before the WAL was garbage collected.
	// In that case, we need to garbage collect the WAL to remove all entries from previous epochs.
	if err := i.maybeGarbageCollectWAL(); err != nil {
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
		GenesisValidatorSet:             i.Config.PlatformChain.GenesisValidatorSet(),
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

	blockBuilder := newBlockBuilderWaiter(msm, i.cs, i.Config.VM)

	comm := newCommunication(i.Config.Sender, i.Config.Broadcaster, validators)

	// set the handle approval method so that the MSM can receive self approvals
	i.transitionListener.handleApproval = msm.HandleApproval
	instanceStorage := NewCallbackStorage(i.cs, msm, func(block *ParsedBlock) error {
		switch {
		case block.Type() == metadata.BlockTypeTransitioning:
			if err := i.transitionListener.handleTransitionBlock(block); err != nil {
				return err
			}
		case block.Type() == metadata.BlockTypeSealing:
			blockBuilder.stop()

			i.transitionListener.handleApproval = nil
			i.notifyEpochChange(block.BlockHeader().Seq, block.SealingBlockInfo().ValidatorSet)
		}
		return nil
	})

	ec := simplex.EpochConfig{
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
		Storage:                    instanceStorage,
		Comm:                       comm,
		BlockBuilder:               blockBuilder,
		BlockDeserializer:          &blockDeserializer{vm: i.Config.VM, cs: i.cs},
	}
	return &epochConfig{
		EpochConfig: ec,
		bbw:         blockBuilder,
	}, nil
}

func (i *Instance) maybeGarbageCollectWAL() error {
	lastBlock, _, err := LastBlock(i.Config.Storage)
	if err != nil {
		return fmt.Errorf("error retrieving last block: %w", err)
	}

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

// startAtEpoch starts either a validator or non-validator at `epoch“.
func (i *Instance) startAtEpoch(validators common.Nodes, epoch uint64) error {
	if validators.Contains(i.Config.ID) {
		return i.startValidator(epoch, validators)
	}

	return i.startNonValidator()
}

type epochConfig struct {
	simplex.EpochConfig
	bbw *blockBuilderWaiter
}

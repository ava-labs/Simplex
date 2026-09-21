// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"errors"
	"fmt"
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
	// WALs holds the write-ahead logs of the simplex instance, one per epoch.
	WALs wal.Store
	// Storage is the interface to the block storage layer for the simplex instance.
	Storage        Storage
	Logger         common.Logger
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
	wal                common.WriteAheadLog
	msm                *metadata.StateMachine
	e                  *simplex.Epoch
	nv                 *nonvalidator.NonValidator
	epochOrNV          timeAdvancer
	epochChanges       chan epochChange
	stopCh             chan struct{}
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

func (i *Instance) startValidator(validators common.Nodes, epochNum uint64) error {
	epochConfig, err := i.createEpochConfig(validators, epochNum)
	if err != nil {
		return err
	}

	epoch, err := simplex.NewEpoch(epochConfig.EpochConfig)
	if err != nil {
		return fmt.Errorf("error creating simplex epoch: %w", err)
	}

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

	height := i.Config.PlatformChain.GetCurrentHeight()
	mappings, err := i.Config.PlatformChain.GetValidatorSet(height)
	if err != nil {
		return nonvalidator.Config{}, err
	}

	comm := newCommunication(i.Config.Sender, i.Config.Broadcaster, mappings.Nodes())

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

	if i.wal != nil {
		if err := i.wal.Close(); err != nil {
			i.Config.Logger.Error("Error closing the WAL", zap.Error(err))
		}
		i.wal = nil
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

	if !i.started {
		i.Config.Logger.Debug("Instance has not started, dropping message")
		return nil
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

	if i.e != nil {
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
			if !from.Equals(msg.EpochTransitionApproval.NodeID[:]) {
				i.Config.Logger.Debug("Dropping approval not sent by its signer",
					zap.Stringer("from", from),
					zap.Stringer("signer", common.NodeID(msg.EpochTransitionApproval.NodeID[:])))
				return nil
			}
			// TODO: pass in time.Now() rather than uint64
			i.msm.HandleApproval(msg.EpochTransitionApproval, uint64(time.Now().UnixMilli()))
			return nil
		}
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
		i.stopValidator()
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

func (i *Instance) createEpochConfig(validators common.Nodes, epoch uint64) (*epochConfig, error) {
	// The logs of earlier epochs hold nothing this epoch may replay, and their rounds would
	// leak into it. Only this epoch's log is ever opened, so a failed discard costs disk, not safety.
	if err := i.Config.WALs.DiscardBefore(epoch); err != nil {
		i.Config.Logger.Error("Error discarding the WALs of previous epochs", zap.Error(err))
	}
	epochWAL, err := i.Config.WALs.Open(epoch)
	if err != nil {
		return nil, fmt.Errorf("error opening the WAL of epoch %d: %w", epoch, err)
	}
	i.wal = epochWAL

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
		ReplicationEnabled: true,
		StartTime:          time.Now(),
		// TODO: For simplicity, we use the same value for all timeouts. If needed we can expand the config.
		MaxProposalWait:            i.Config.ParameterConfig.MaxNetworkDelay * 2, // 1 proposal + 1 vote
		MaxRebroadcastWait:         i.Config.ParameterConfig.MaxNetworkDelay * 2,
		FinalizeRebroadcastTimeout: i.Config.ParameterConfig.MaxNetworkDelay * 2,
		MaxRoundWindow:             i.Config.ParameterConfig.MaxRoundWindow,
		ID:                         i.Config.ID,
		RandomSource:               source, // Seed the random source from crypto/rand
		WAL:                        epochWAL,
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

// startAtEpoch starts a validator for the given epoch if we are in its validator set, and a non-validator otherwise.
func (i *Instance) startAtEpoch(validators common.Nodes, epoch uint64) error {
	if validators.Contains(i.Config.ID) {
		return i.startValidator(validators, epoch)
	}

	return i.startNonValidator()
}

type epochConfig struct {
	simplex.EpochConfig
	bbw *blockBuilderWaiter
}

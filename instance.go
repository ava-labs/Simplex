// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/ava-labs/simplex/simplex"
	"github.com/ava-labs/simplex/wal"
	"go.uber.org/zap"
)

const (
	tickInterval = time.Millisecond * 100
)

type Config struct {
	LastNonSimplexInnerBlock metadata.VMBlock
	ParameterConfig          ParameterConfig
	PlatformChain            PlatformChain
	CryptoOps                CryptoOps
	Storage                  Storage
	Logger                   common.Logger
	Sender                   Sender
	WALs                     []wal.DeletableWAL
	VM                       VM
	ID                       common.NodeID
}

type MsgHandler interface {
	HandleMessage(msg *common.Message, from common.NodeID) error
}

type Instance struct {
	Config            Config
	lock              sync.Mutex
	cs                *CachedStorage
	msm               *metadata.StateMachine
	e                 *simplex.Epoch
	stopCh            chan struct{}
	duringEpochChange atomic.Bool
}

func (i *Instance) Start() error {
	i.lock.Lock()
	stopCh := make(chan struct{})
	i.stopCh = stopCh
	i.lock.Unlock()

	epochConfig, err := i.createEpochConfig()
	if err != nil {
		return err
	}

	if err := i.startEpoch(epochConfig); err != nil {
		return err
	}

	// Pass the stop channel explicitly so the tick loop observes the channel it
	// was started with, rather than reading the i.stopCh field (which a
	// subsequent Start would overwrite).
	go i.tick(stopCh)

	return nil
}

func (i *Instance) tick(stopCh chan struct{}) {
	ticker := time.NewTicker(tickInterval)
	for {
		select {
		case now := <-ticker.C:
			i.lock.Lock()
			epoch := i.e
			i.lock.Unlock()
			if epoch == nil {
				// Stopped, so we exit the tick loop.
				return
			}
			epoch.AdvanceTime(now)
		case <-stopCh:
			return
		}
	}
}

func (i *Instance) Stop() {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.e != nil {
		i.e.Stop()
		close(i.stopCh)
		i.e = nil
	}
}

func (i *Instance) HandleMessage(msg *common.Message, from common.NodeID) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	// A message may arrive while we are between epochs (e.g. during an epoch
	// change, when the old epoch has been stopped and the new one has not
	// started yet). Drop it rather than dereferencing a nil epoch.
	if i.e == nil {
		return nil
	}

	return i.e.HandleMessage(msg, from)
}

func (i *Instance) HandleBlockMessage(ctx context.Context, block *RawBlock, vote *common.Vote, from common.NodeID) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.e == nil {
		return nil
	}

	if block == nil || vote == nil {
		i.Config.Logger.Debug("Received nil block or vote")
		return nil
	}

	// TODO: use a real context
	vmBlock, err := i.Config.VM.ParseBlock(ctx, block.InnerBlockBytes)
	if err != nil {
		i.Config.Logger.Debug("Failed to parse inner block", zap.Error(err))
		return nil
	}

	parsedBlock := ParsedBlock{
		StateMachineBlock: metadata.StateMachineBlock{
			InnerBlock: vmBlock,
			Metadata:   block.Metadata,
		},
		msm: i.msm,
	}

	var cb = &cachedBlock{
		cache:       i.cs,
		ParsedBlock: &parsedBlock,
	}

	msg := &common.Message{
		BlockMessage: &common.BlockMessage{
			Block: cb,
			Vote:  *vote,
		},
	}

	return i.e.HandleMessage(msg, from)
}

func (i *Instance) startEpoch(epochConfig simplex.EpochConfig) error {
	epoch, err := simplex.NewEpoch(epochConfig)
	if err != nil {
		return fmt.Errorf("error creating simplex epoch: %w", err)
	}
	epoch.Epoch = epochConfig.Epoch

	i.lock.Lock()
	i.e = epoch
	i.lock.Unlock()

	return epoch.Start()
}

func (i *Instance) createEpochConfig() (simplex.EpochConfig, error) {
	wal, err := wal.NewGarbageCollectedWAL(i.Config.WALs, i.Config.Storage.CreateWAL, &common.WALRetentionReader{}, i.Config.ParameterConfig.WALMaxEntryCount)
	if err != nil {
		return simplex.EpochConfig{}, fmt.Errorf("error creating garbage collected wal: %w", err)
	}
	storage := i.Config.Storage

	numBlocks := storage.NumBlocks()
	if numBlocks == 0 {
		return simplex.EpochConfig{}, fmt.Errorf("no genesis block found in storage")
	}

	latestSeq := numBlocks - 1

	lastBlock, _, err := storage.GetBlock(latestSeq)
	if err != nil {
		return simplex.EpochConfig{}, fmt.Errorf("error retrieving last block from storage: %w", err)
	}

	if lastBlock.Metadata.SimplexEpochInfo.BlockValidationDescriptor != nil {
		i.Config.Logger.Info("Last block is a sealing block, garbage collecting all WALs to start a new epoch")
		if err := wal.GarbageCollect(math.MaxUint64); err != nil {
			return simplex.EpochConfig{}, fmt.Errorf("error garbage collecting WALs: %w", err)
		}
	}

	var md *common.ProtocolMetadata
	if len(lastBlock.Metadata.SimplexProtocolMetadata) > 0 {
		md, err = common.ProtocolMetadataFromBytes(lastBlock.Metadata.SimplexProtocolMetadata)
		if err != nil {
			return simplex.EpochConfig{}, fmt.Errorf("error parsing protocol metadata from last block: %w", err)
		}
	} else {
		md = &common.ProtocolMetadata{}
	}

	epochNum := md.Epoch

	lastParsedBlock := &ParsedBlock{
		StateMachineBlock: lastBlock,
	}

	lastNonSimplexHeight := i.Config.LastNonSimplexInnerBlock.Height()
	genesisValidatorSet := i.Config.PlatformChain.GenesisValidatorSet()
	validatorSet, nodes, epochNum, err := constructEpochAndValidatorSet(lastNonSimplexHeight, genesisValidatorSet, numBlocks, *lastParsedBlock, storage)
	if err != nil {
		return simplex.EpochConfig{}, err
	}


	_ = validatorSet

	cachedStorage := NewCachedStorage(storage)
	i.lock.Lock()
	i.cs = cachedStorage
	i.lock.Unlock()

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
		GetBlock:                        cachedStorage.RetrieveBlock,
	})
	if err != nil {
		return simplex.EpochConfig{}, fmt.Errorf("error creating metadata state machine: %w", err)
	}

	i.lock.Lock()
	i.msm = msm
	i.lock.Unlock()

	// Wire the MSM into cached storage.
	// We do this because otherwise we have a chicken and egg problem - MSM needs the cached storage,
	// but the cached storage also needs the MSM
	cachedStorage.msm = msm

	source, err := simplex.NewRandomSource()
	if err != nil {
		return simplex.EpochConfig{}, err
	}

	blockBuilder := &BlockBuilderWaiter{vm: i.Config.VM, msm: msm, blocked: &i.duringEpochChange}

	epochAwareStorage := &EpochAwareStorage{
		Epoch:   epochNum,
		Storage: cachedStorage,
		OnEpochChange: func(epoch uint64) error {
			i.duringEpochChange.Store(true)
			blockBuilder.stop()
			go func() {
				i.e.Stop()
				config, err := i.createEpochConfig()
				if err != nil {
					i.Config.Logger.Error("Error creating epoch config on epoch change", zap.Error(err))
				}
				// On epoch change, garbage collect the WAL to remove all entries from previous epochs.
				if err := wal.GarbageCollect(math.MaxUint64); err != nil {
					i.Config.Logger.Error("Error garbage collecting epoch config on epoch change", zap.Error(err))
				}
				i.duringEpochChange.Store(false)

				if err := i.startEpoch(config); err != nil {
					i.Config.Logger.Error("Error starting new epoch on epoch change", zap.Error(err))
				}
			}()
			return nil
		},
	}

	epochConfig := simplex.EpochConfig{
		// Parameter config
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
		Comm:                       &Communication{nodes: nodes, Sender: i.Config.Sender, blocked: &i.duringEpochChange},
		BlockBuilder:               blockBuilder,
		BlockDeserializer:          &blockDeserializer{vm: i.Config.VM, msm: msm},
	}
	return epochConfig, nil
}

func constructEpochAndValidatorSet(lastNonSimplexInnerBlockHeight uint64, genesisValidatorSet metadata.NodeBLSMappings, numBlocks uint64, lastBlock ParsedBlock, storage Storage) (metadata.NodeBLSMappings, common.Nodes, uint64, error) {
	epochNum := lastBlock.BlockHeader().Epoch

	var validatorSet metadata.NodeBLSMappings
	var nodes common.Nodes

	switch {
	// If all we have in the ledger is non-Simplex blocks, load the validator set from genesis
	case lastNonSimplexInnerBlockHeight + 1 == numBlocks:
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
			return nil, nil, 0, fmt.Errorf("error retrieving sealing block from storage: %w", err)
		}
		if sealingBlock.Metadata.SimplexEpochInfo.BlockValidationDescriptor == nil {
			return nil, nil, 0, fmt.Errorf("expected sealing block at seq %d, but got a non-sealing block", sealingBlockSeq)
		}
		validatorSet = constructValidatorSetFromSealingBlock(ParsedBlock{StateMachineBlock: sealingBlock})
		nodes = validatorSetToNodes(validatorSet)
	}
	return validatorSet, nodes, epochNum, nil
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

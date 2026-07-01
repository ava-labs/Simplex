// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
)

type Communication struct {
	blocked *atomic.Bool
	nodes   common.Nodes
	Sender
}

func (c *Communication) Validators() common.Nodes {
	return c.nodes
}

func (c *Communication) Broadcast(msg *common.Message) {
	if c.blocked.Load() {
		return
	}
	for _, node := range c.nodes {
		c.Sender.Send(msg, node.Id)
	}
}

type EpochAwareStorage struct {
	msm           *metadata.StateMachine
	OnEpochChange func(seq uint64) error
	Storage
	Epoch uint64
}

func (e *EpochAwareStorage) Retrieve(seq uint64) (common.VerifiedBlock, common.Finalization, error) {
	block, finalization, err := e.Storage.GetBlock(seq)
	if err != nil {
		return nil, common.Finalization{}, err
	}
	parsedBlock := &ParsedBlock{
		msm:               e.msm,
		StateMachineBlock: block,
	}
	return parsedBlock, *finalization, nil
}

func (e *EpochAwareStorage) Index(ctx context.Context, block common.VerifiedBlock, certificate common.Finalization) error {
	if block.BlockHeader().Epoch < e.Epoch {
		// This is a Telock from a previous h, so we ignore it and do not index it.
		return nil
	}
	if err := e.Storage.Index(ctx, block, certificate); err != nil {
		return err
	}
	if block.SealingBlockInfo() != nil {
		// If this is a sealing block, we garbage collect the WAL before returning.
		if err := e.OnEpochChange(block.BlockHeader().Seq); err != nil {
			return err
		}
		// We are now in a new h, so we update the h number to prevent indexing Telocks from the previous h.
		e.Epoch = block.BlockHeader().Seq
	}
	return nil
}

type cachedBlock struct {
	cache *CachedStorage
	*ParsedBlock
}

func (cb *cachedBlock) Verify(ctx context.Context) (common.VerifiedBlock, error) {
	vb, err := cb.ParsedBlock.Verify(ctx)
	if err == nil {
		cb.cache.insertBlock(cb.ParsedBlock)
	}
	return vb, err
}

type CachedStorage struct {
	msm  *metadata.StateMachine
	lock sync.RWMutex
	Storage
	cache map[[32]byte]cachedBlock
}

func NewCachedStorage(storage Storage) *CachedStorage {
	return &CachedStorage{
		Storage: storage,
		cache:   make(map[[32]byte]cachedBlock),
	}
}

func (cs *CachedStorage) RetrieveBlock(seq uint64, digest [32]byte) (metadata.StateMachineBlock, *common.Finalization, error) {
	block, finalization, err := cs.Retrieve(seq, digest)
	if err != nil {
		return metadata.StateMachineBlock{}, nil, err
	}

	return block.(*ParsedBlock).StateMachineBlock, finalization, nil
}

func (cs *CachedStorage) Retrieve(seq uint64, digest [32]byte) (common.VerifiedBlock, *common.Finalization, error) {
	cs.lock.RLock()
	item, exists := cs.cache[digest]
	if exists {
		cs.lock.RUnlock()
		// If the block is cached, it means it's not finalized yet, because upon finalizing the block (indexing)
		// we also remove it from the cache. Therefore, we return nil for the finalization.
		return item.ParsedBlock, nil, nil
	}
	cs.lock.RUnlock()

	// We don't populate the cache here because we populate it externally.

	block, finalization, err := cs.Storage.GetBlock(seq)
	if err != nil {
		return nil, nil, err
	}

	return &ParsedBlock{
		StateMachineBlock: block,
		msm:               cs.msm,
	}, finalization, nil
}

func (cs *CachedStorage) Index(ctx context.Context, block common.VerifiedBlock, certificate common.Finalization) error {
	err := cs.Storage.Index(ctx, block, certificate)

	if err == nil {
		// We delete the block from the cache after it has been indexed because now that it is persisted,
		// we can just lookup by sequence number instead of digest.
		cs.lock.Lock()
		defer cs.lock.Unlock()
		delete(cs.cache, block.BlockHeader().Digest)
	}

	return err
}

func (cs *CachedStorage) insertBlock(block *ParsedBlock) {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	cs.cache[block.Digest()] = cachedBlock{
		ParsedBlock: block,
	}
}

type NoopAuxiliaryInfoApp struct{}

func (n *NoopAuxiliaryInfoApp) IsLegalAppend(versionID metadata.VersionID, nodes metadata.NodeBLSMappings, history [][]byte, x []byte) error {
	if len(x) > 0 {
		return fmt.Errorf("input should be empty")
	}
	return nil
}

func (n *NoopAuxiliaryInfoApp) IsSufficient(versionID metadata.VersionID, nodes metadata.NodeBLSMappings, history [][]byte) (bool, error) {
	return true, nil
}

func (n *NoopAuxiliaryInfoApp) Generate(versionID metadata.VersionID, nodes metadata.NodeBLSMappings, history [][]byte) ([]byte, error) {
	return nil, nil
}

func (n *NoopAuxiliaryInfoApp) DefaultVersionID() metadata.VersionID {
	return 0
}

type BlockBuilderWaiter struct {
	blocked *atomic.Bool
	lock    sync.Mutex
	cancel  context.CancelFunc
	msm     *metadata.StateMachine
	vm      VM
}

func (bw *BlockBuilderWaiter) stop() {
	bw.lock.Lock()
	defer bw.lock.Unlock()
	if bw.cancel != nil {
		bw.cancel()
		bw.cancel = nil
	}
}

func (bw *BlockBuilderWaiter) WaitForPendingBlock(ctx context.Context) {
	if bw.blocked.Load() {
		return
	}

	bw.lock.Lock()
	if bw.cancel != nil {
		bw.cancel()
	}
	ctx, cancel := context.WithCancel(ctx)
	bw.cancel = cancel
	bw.lock.Unlock()
	defer cancel()
	bw.vm.WaitForPendingBlock(ctx)
}

func (bw *BlockBuilderWaiter) BuildBlock(ctx context.Context, metadata common.ProtocolMetadata, blacklist common.Blacklist) (common.VerifiedBlock, bool) {
	if bw.blocked.Load() {
		return nil, false
	}

	block, err := bw.msm.BuildBlock(ctx, metadata, &blacklist)
	if err != nil {
		return nil, false
	}

	pb := ParsedBlock{
		StateMachineBlock: *block,
		msm:               bw.msm,
	}

	return &pb, true
}

type blockDeserializer struct {
	vm  VM
	msm *metadata.StateMachine
}

func (bp *blockDeserializer) DeserializeBlock(ctx context.Context, bytes []byte) (common.Block, error) {
	var rawBlock RawBlock
	if err := rawBlock.UnmarshalCanoto(bytes); err != nil {
		return nil, err
	}

	block, err := bp.vm.ParseBlock(ctx, rawBlock.InnerBlockBytes)
	if err != nil {
		return nil, err
	}
	return &ParsedBlock{
		StateMachineBlock: metadata.StateMachineBlock{
			InnerBlock: block,
			Metadata:   rawBlock.Metadata,
		},
		msm: bp.msm,
	}, nil
}

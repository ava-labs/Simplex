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
	nodes atomic.Value // common.Nodes
	Sender
	Broadcaster
}

func (c *Communication) SetValidators(nodes common.Nodes) {
	c.nodes.Store(nodes)
}

func (c *Communication) Validators() common.Nodes {
	nodes, ok := c.nodes.Load().(common.Nodes)
	if !ok {
		return nil
	}
	return nodes
}

// EpochAwareStorage is a wrapper around Storage that is aware of epoch changes.
// Upon an epoch change, it will ignore blocks from previous epochs
// and will call the onEpochChange callback when a new epoch is detected.
type EpochAwareStorage struct {
	// CachedStorage is used to ensure that we prune the cache on Index.
	*CachedStorage

	msm           *metadata.StateMachine
	onEpochChange func(seq uint64, validators common.Nodes) error
	epoch         uint64
}

func (e *EpochAwareStorage) Retrieve(seq uint64) (common.VerifiedBlock, common.Finalization, error) {
	block, finalization, err := e.GetBlock(seq)
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
	if block.BlockHeader().Epoch < e.epoch {
		// This is a Telock from a previous epoch, so we ignore it and do not index it.
		return nil
	}
	// e.Storage would resolve to the raw storage promoted through CachedStorage,
	// skipping the cache eviction in CachedStorage.Index.
	if err := e.CachedStorage.Index(ctx, block, certificate); err != nil {
		return err
	}
	// This is a sealing block, and it is not the zero block
	if block.SealingBlockInfo() != nil && block.SealingBlockInfo().PrevSealingBlockHash != [32]byte{} {
		if err := e.onEpochChange(block.BlockHeader().Seq, block.SealingBlockInfo().ValidatorSet); err != nil {
			return err
		}
		// We are now in a new epoch, so we update the epoch number to prevent indexing Telocks from the previous epoch.
		e.epoch = block.BlockHeader().Seq
	}
	return nil
}

// cachedBlock is a wrapper around ParsedBlock that caches the block in the CachedStorage upon verification.
// It is needed for the MSM because the MSM needs to be able to retrieve blocks that aren't finalized during its execution.
// These blocks are cached in the CachedStorage upon verification, and removed from the cache upon finalization (indexing).
type cachedBlock struct {
	cache *CachedStorage
	*ParsedBlock
}

func (cb *cachedBlock) Verify(ctx context.Context, verifyOpts ...common.VerifyOptions) (common.VerifiedBlock, error) {
	vb, err := cb.ParsedBlock.Verify(ctx, verifyOpts...)
	if err == nil {
		cb.cache.insertBlock(cb.ParsedBlock)
	}
	return vb, err
}

type CachedStorage struct {
	msm  *metadata.StateMachine
	lock sync.RWMutex
	Storage
	cache map[common.Digest]cachedBlock
}

func NewCachedStorage(storage Storage) *CachedStorage {
	return &CachedStorage{
		Storage: storage,
		cache:   make(map[common.Digest]cachedBlock),
	}
}

func (cs *CachedStorage) RetrieveBlock(seq uint64, digest common.Digest) (metadata.StateMachineBlock, *common.Finalization, error) {
	block, finalization, err := cs.Retrieve(seq, digest)
	if err != nil {
		return metadata.StateMachineBlock{}, nil, err
	}

	return block.(*ParsedBlock).Clone(), finalization, nil
}

func (cs *CachedStorage) Retrieve(seq uint64, digest common.Digest) (common.VerifiedBlock, *common.Finalization, error) {
	cs.lock.RLock()

	item, exists := cs.cache[digest]
	if exists {
		cs.lock.RUnlock()
		return item.ParsedBlock, nil, nil
	}

	for _, cb := range cs.cache {
		if cb.BlockHeader().Seq == seq {
			if cb.Digest() == digest || digest == (common.Digest{}) {
				cs.lock.RUnlock()
				return cb.ParsedBlock, nil, nil
			}
		}
	}
	cs.lock.RUnlock()

	// We don't populate the cache here because we populate it externally.
	block, finalization, err := cs.GetBlock(seq)
	if err != nil {
		return nil, nil, err
	}
	if digest != (common.Digest{}) && block.Digest() != digest {
		return nil, nil, common.ErrBlockNotFound
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

		// We also delete all blocks that are older than the indexed block, because they are now finalized and persisted.
		for digest, cachedBlock := range cs.cache {
			if cachedBlock.BlockHeader().Seq < block.BlockHeader().Seq {
				delete(cs.cache, digest)
			}
		}
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

func (n *NoopAuxiliaryInfoApp) IsLegalAppend(versionID common.VersionID, nodes metadata.NodeBLSMappings, history [][]byte, x []byte) error {
	if len(x) > 0 {
		return fmt.Errorf("input should be empty")
	}
	return nil
}

func (n *NoopAuxiliaryInfoApp) IsSufficient(versionID common.VersionID, nodes metadata.NodeBLSMappings, history [][]byte) (bool, error) {
	return true, nil
}

func (n *NoopAuxiliaryInfoApp) Generate(common.VersionID, metadata.NodeBLSMappings, [][]byte) ([]byte, error) {
	return nil, nil
}

func (n *NoopAuxiliaryInfoApp) DefaultVersionID() common.VersionID {
	return 0
}

type BlockBuilderWaiter struct {
	lock   sync.Mutex
	cancel context.CancelFunc
	msm    *metadata.StateMachine
	vm     VM
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
	block, err := bw.msm.BuildBlock(ctx, metadata, blacklist)
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
	vm VM
	cs *CachedStorage
}

func (bd *blockDeserializer) DeserializeBlock(ctx context.Context, bytes []byte) (common.Block, error) {
	var rawBlock metadata.RawBlock
	if err := rawBlock.UnmarshalCanoto(bytes); err != nil {
		return nil, err
	}

	block, err := bd.vm.ParseBlock(ctx, rawBlock.InnerBlockBytes)
	if err != nil {
		return nil, err
	}
	return &cachedBlock{
		ParsedBlock: &ParsedBlock{
			StateMachineBlock: metadata.StateMachineBlock{
				InnerBlock: block,
				Metadata:   rawBlock.Metadata,
			},
			msm: bd.cs.msm,
		},
		cache: bd.cs,
	}, nil
}

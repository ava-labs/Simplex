// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"fmt"
	"sync"

	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
)

type Communication struct {
	Sender
	Broadcaster
	highestValidator func() common.Nodes
}

func (c *Communication) Validators() common.Nodes {
	return c.highestValidator()
}

// InstanceStorage is a wrapper around Storage that skips indexing Telocks
// and delegates post-index handling to a caller-provided onIndex hook.
type InstanceStorage struct {
	Storage

	msm *metadata.StateMachine

	onIndex func(block *ParsedBlock) error
}

func NewInstanceStorage(storage Storage, msm *metadata.StateMachine, onIndex func(block *ParsedBlock) error) *InstanceStorage {
	return &InstanceStorage{
		Storage: storage,
		msm:     msm,
		onIndex: onIndex,
	}
}

func (s *InstanceStorage) Retrieve(seq uint64) (common.VerifiedBlock, common.Finalization, error) {
	block, finalization, err := s.Storage.GetBlock(seq)
	if err != nil {
		return nil, common.Finalization{}, err
	}
	parsedBlock := &ParsedBlock{
		msm:               s.msm,
		StateMachineBlock: block,
	}
	return parsedBlock, *finalization, nil
}

func (s *InstanceStorage) Index(ctx context.Context, block common.VerifiedBlock, certificate common.Finalization) error {
	pb, ok := block.(*ParsedBlock)
	if !ok {
		return fmt.Errorf("expected ParsedBlock, got %T", block)
	}

	// A Telock only extends time until the epoch transition finalizes, so we never index it.
	if pb.Type() == metadata.BlockTypeTelock {
		return nil
	}

	if err := s.Storage.Index(ctx, block, certificate); err != nil {
		return err
	}

	return s.onIndex(pb)
}

// cachedBlock is a wrapper around ParsedBlock that caches the block in the CachedStorage upon verification.
// It is needed for the MSM because the MSM needs to be able to retrieve blocks that aren't finalized during its execution.
// These blocks are cached in the CachedStorage upon verification, and removed from the cache upon finalization (indexing).
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
	Storage

	lock  sync.RWMutex
	msm   *metadata.StateMachine
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

	return block.(*ParsedBlock).StateMachineBlock, finalization, nil
}

func (cs *CachedStorage) Retrieve(seq uint64, digest common.Digest) (common.VerifiedBlock, *common.Finalization, error) {
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

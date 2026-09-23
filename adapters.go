// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ava-labs/simplex/avalanchego"
	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/ava-labs/simplex/simplex"
)

type Communication struct {
	nodes atomic.Value // common.Nodes
	Sender
	Broadcaster
}

func newCommunication(sender Sender, broadcaster Broadcaster, validators common.Nodes) *Communication {
	c := &Communication{
		Sender:      sender,
		Broadcaster: broadcaster,
	}
	c.SetValidators(validators)
	return c
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

// CallbackStorage is a wrapper around Storage that skips indexing Telocks
// and delegates post-index handling to a caller-provided onIndex hook.
type CallbackStorage struct {
	// CachedStorage is used to ensure that we prune the cache on Index.
	*CachedStorage

	msm *metadata.StateMachine

	onIndex func(block *ParsedBlock) error
}

func NewCallbackStorage(storage *CachedStorage, msm *metadata.StateMachine, onIndex func(block *ParsedBlock) error) *CallbackStorage {
	return &CallbackStorage{
		CachedStorage: storage,
		msm:           msm,
		onIndex:       onIndex,
	}
}

func (s *CallbackStorage) Retrieve(seq uint64) (common.VerifiedBlock, common.Finalization, error) {
	block, finalization, err := s.GetBlock(seq)
	if err != nil {
		return nil, common.Finalization{}, err
	}
	parsedBlock := &ParsedBlock{
		msm:               s.msm,
		StateMachineBlock: block,
	}
	return parsedBlock, *finalization, nil
}

func (s *CallbackStorage) Index(ctx context.Context, block common.VerifiedBlock, certificate common.Finalization) error {
	pb, ok := block.(*ParsedBlock)
	if !ok {
		return fmt.Errorf("expected ParsedBlock, got %T", block)
	}

	// A Telock only extends time until the epoch transition finalizes, so we never index it.
	if pb.Type() == metadata.BlockTypeTelock {
		return nil
	}

	if err := s.CachedStorage.Index(ctx, block, certificate); err != nil {
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
	// A finalized seq is always served from storage, so a same-seq cache entry can never shadow it.
	if seq >= cs.Storage.NumBlocks() {
		if cb, ok := cs.retrieveCached(seq, digest); ok {
			return cb, nil, nil
		}
	}

	// We don't populate the cache here because we populate it externally.
	block, finalization, err := cs.GetBlock(seq)
	if err != nil {
		return nil, nil, err
	}
	pb := &ParsedBlock{
		StateMachineBlock: block,
		msm:               cs.msm,
	}
	if digest != (common.Digest{}) && pb.Digest() != digest {
		return nil, nil, common.ErrBlockNotFound
	}
	return pb, finalization, nil
}

// retrieveCached returns the cached block at seq matching digest. A zero digest
// matches on seq alone, choosing the smallest digest so same-seq forks resolve
// identically on every node.
func (cs *CachedStorage) retrieveCached(seq uint64, digest common.Digest) (*ParsedBlock, bool) {
	cs.lock.RLock()
	defer cs.lock.RUnlock()

	if digest != (common.Digest{}) {
		cb, ok := cs.cache[digest]
		if !ok || cb.BlockHeader().Seq != seq {
			return nil, false
		}
		return cb.ParsedBlock, true
	}

	var found *ParsedBlock
	var foundDigest common.Digest
	for d, cb := range cs.cache {
		if cb.BlockHeader().Seq != seq {
			continue
		}
		if found == nil || bytes.Compare(d[:], foundDigest[:]) < 0 {
			found, foundDigest = cb.ParsedBlock, d
		}
	}
	return found, found != nil
}

func (cs *CachedStorage) Index(ctx context.Context, block common.VerifiedBlock, certificate common.Finalization) error {
	err := cs.Storage.Index(ctx, block, certificate)

	if err == nil {
		// We delete the block from the cache after it has been indexed because now that it is persisted,
		// we can just lookup by sequence number instead of digest.
		cs.lock.Lock()
		defer cs.lock.Unlock()
		delete(cs.cache, block.BlockHeader().Digest)

		// We also delete all blocks that are older than the indexed block, including the finalized block because they are now finalized and persisted.
		for digest, cachedBlock := range cs.cache {
			if cachedBlock.BlockHeader().Seq <= block.BlockHeader().Seq {
				delete(cs.cache, digest)
			}
		}
	}

	return err
}

func (cs *CachedStorage) insertBlock(block *ParsedBlock) {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	// Index writes storage before pruning under the lock, so a block whose seq is
	// already indexed is either rejected here or pruned by that Index.
	if block.BlockHeader().Seq < cs.Storage.NumBlocks() {
		return
	}

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

type blockBuilderWaiter struct {
	lock   sync.Mutex
	cancel context.CancelFunc
	msm    *metadata.StateMachine
	cs     *CachedStorage
	e      *simplex.Epoch
	vm     VM
}

func newBlockBuilderWaiter(msm *metadata.StateMachine, cs *CachedStorage, vm VM) *blockBuilderWaiter {
	return &blockBuilderWaiter{
		msm: msm,
		cs:  cs,
		vm:  vm,
	}
}

func (bw *blockBuilderWaiter) stop() {
	bw.lock.Lock()
	defer bw.lock.Unlock()
	if bw.cancel != nil {
		bw.cancel()
		bw.cancel = nil
	}
}

func (bw *blockBuilderWaiter) WaitForPendingBlock(ctx context.Context) {
	bw.lock.Lock()
	if bw.cancel != nil {
		bw.cancel()
	}
	ctx, cancel := context.WithCancel(ctx)
	bw.cancel = cancel
	bw.lock.Unlock()
	defer cancel()

	md := bw.e.Metadata()
	bw.msm.WaitForPendingBlock(ctx, md)
}

func (bw *blockBuilderWaiter) BuildBlock(ctx context.Context, metadata common.ProtocolMetadata, blacklist common.Blacklist) (common.VerifiedBlock, bool) {
	block, err := bw.msm.BuildBlock(ctx, metadata, blacklist)
	if err != nil {
		return nil, false
	}

	pb := &ParsedBlock{
		StateMachineBlock: *block,
		msm:               bw.msm,
	}

	// Ensure the builders block is in the cache after verification
	bw.cs.insertBlock(pb)

	return pb, true
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

	var innerBlock avalanchego.VMBlock
	if len(rawBlock.InnerBlockBytes) > 0 {
		block, err := bd.vm.ParseBlock(ctx, rawBlock.InnerBlockBytes)
		if err != nil {
			return nil, err
		}
		innerBlock = block
	}

	return &cachedBlock{
		ParsedBlock: &ParsedBlock{
			StateMachineBlock: metadata.StateMachineBlock{
				InnerBlock: innerBlock,
				Metadata:   rawBlock.Metadata,
			},
			msm: bd.cs.msm,
		},
		cache: bd.cs,
	}, nil
}

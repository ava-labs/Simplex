// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/stretchr/testify/require"
)

func TestStateMachineEpochTransition(t *testing.T) {
	validatorSetRetriever := validatorSetRetriever{
		resultMap: map[uint64]NodeBLSMappings{
			100: {{BLSKey: []byte{1}, Weight: 1, NodeID: [20]byte{1}}, {BLSKey: []byte{2}, Weight: 1, NodeID: [20]byte{2}}},
			200: {{BLSKey: []byte{1}, Weight: 1, NodeID: [20]byte{1}}, {BLSKey: []byte{2}, Weight: 2, NodeID: [20]byte{2}},
				{BLSKey: []byte{3}, Weight: 1, NodeID: [20]byte{3}}},
			300: {{BLSKey: []byte{1}, Weight: 1, NodeID: [20]byte{1}}, {BLSKey: []byte{2}, Weight: 2, NodeID: [20]byte{2}},
				{BLSKey: []byte{3}, Weight: 3, NodeID: [20]byte{3}}, {BLSKey: []byte{4}, Weight: 1, NodeID: [20]byte{4}}},
		},
	}

	var pChainHeight atomic.Uint64
	pChainHeight.Store(100)
	node := newMultiEpochNode(t)
	node.sm.GetValidatorSet = validatorSetRetriever.getValidatorSet
	node.sm.GetPChainHeight = func() uint64 {
		return pChainHeight.Load()
	}

	// Create some blocks and finalize them, until we reach height 10
	for node.Height() < 10 {
		node.act()
	}

	// Next, we increase the P-Chain height, which should cause the node to update its validator set and move to the new epoch.
	pChainHeight.Store(200)

	epoch := node.Epoch()
	for node.Epoch() == epoch {
		node.act()
		if flipCoin() {
			node.sm.ApprovalsRetriever = &approvalsRetriever{
				result: []ValidatorSetApproval{{NodeID: [20]byte{1}, PChainHeight: 200, Signature: []byte{1}, AuxInfoSeqDigest: [32]byte{}}},
			}
		} else {
			node.sm.ApprovalsRetriever = &approvalsRetriever{
				result: []ValidatorSetApproval{{NodeID: [20]byte{2}, PChainHeight: 200, Signature: []byte{2}, AuxInfoSeqDigest: [32]byte{}}},
			}
		}
	}

	t.Log("Epoch:", node.Epoch())
	require.Greater(t, node.Epoch(), uint64(1))

	// Finally, we increase the P-Chain height again, which should cause the node to update its validator set and move to the new epoch.

	pChainHeight.Store(300)

	epoch = node.Epoch()
	for node.Epoch() == epoch {
		node.act()
		if flipCoin() {
			node.sm.ApprovalsRetriever = &approvalsRetriever{
				result: []ValidatorSetApproval{{NodeID: [20]byte{2}, PChainHeight: 300, Signature: []byte{2}, AuxInfoSeqDigest: [32]byte{}}},
			}
		} else {
			node.sm.ApprovalsRetriever = &approvalsRetriever{
				result: []ValidatorSetApproval{{NodeID: [20]byte{3}, PChainHeight: 300, Signature: []byte{3}, AuxInfoSeqDigest: [32]byte{}}},
			}
		}
	}

	t.Log("Epoch:", node.Epoch())
	require.Greater(t, node.Epoch(), epoch)
}

func TestStateMachineEpochTransitionEmptyMempool(t *testing.T) {
	validatorSetRetriever := validatorSetRetriever{
		resultMap: map[uint64]NodeBLSMappings{
			100: {{BLSKey: []byte{1}, Weight: 1, NodeID: [20]byte{1}}, {BLSKey: []byte{2}, Weight: 1, NodeID: [20]byte{2}}},
			200: {{BLSKey: []byte{1}, Weight: 1, NodeID: [20]byte{1}}, {BLSKey: []byte{2}, Weight: 2, NodeID: [20]byte{2}},
				{BLSKey: []byte{3}, Weight: 1, NodeID: [20]byte{3}}},
			300: {{BLSKey: []byte{1}, Weight: 1, NodeID: [20]byte{1}}, {BLSKey: []byte{2}, Weight: 2, NodeID: [20]byte{2}},
				{BLSKey: []byte{3}, Weight: 3, NodeID: [20]byte{3}}, {BLSKey: []byte{4}, Weight: 1, NodeID: [20]byte{4}}},
		},
	}

	var pChainHeight uint64 = 100
	node := newMultiEpochNode(t)
	node.sm.MaxBlockBuildingWaitTime = 100 * time.Millisecond
	node.sm.GetValidatorSet = validatorSetRetriever.getValidatorSet
	node.sm.GetPChainHeight = func() uint64 {
		return pChainHeight
	}

	// Create some blocks and finalize them, until we reach height 10
	for node.Height() < 10 {
		node.act()
	}

	// Next, we increase the P-Chain height, which should cause the node to update its validator set and move to the new epoch.
	pChainHeight = 200

	// However, we mark the mempool as empty, which should cause the node to wait until it sees a change in the P-Chain height, rather than building blocks on top of the old epoch.
	node.mempoolEmpty = true

	// We build blocks until the sealing block is finalized.
	for node.lastFinalizedBlock().Metadata.SimplexEpochInfo.BlockValidationDescriptor == nil {
		node.act()
		if flipCoin() {
			node.sm.ApprovalsRetriever = &approvalsRetriever{
				result: []ValidatorSetApproval{{NodeID: [20]byte{1}, PChainHeight: 200, Signature: []byte{1}, AuxInfoSeqDigest: [32]byte{}}},
			}
		} else {
			node.sm.ApprovalsRetriever = &approvalsRetriever{
				result: []ValidatorSetApproval{{NodeID: [20]byte{2}, PChainHeight: 200, Signature: []byte{2}, AuxInfoSeqDigest: [32]byte{}}},
			}
		}
	}

	node.mempoolEmpty = false

	// Build a new block and check that the node has transitioned to the new epoch,
	// rather than building a block on top of the old epoch.
	height := node.Height()

	for node.Height() == height {
		node.act()
	}
	require.Greater(t, node.Epoch(), uint64(1))

	t.Log("Epoch:", node.Epoch())

	epoch := node.Epoch()
	require.Greater(t, epoch, uint64(1))

	// Finally, we increase the P-Chain height again, which should cause the node to update its validator set and move to the new epoch.

	pChainHeight = 300

	for node.Height() < 30 {
		node.act()
		if flipCoin() {
			node.sm.ApprovalsRetriever = &approvalsRetriever{
				result: []ValidatorSetApproval{{NodeID: [20]byte{2}, PChainHeight: 300, Signature: []byte{2}, AuxInfoSeqDigest: [32]byte{}}},
			}
		} else {
			node.sm.ApprovalsRetriever = &approvalsRetriever{
				result: []ValidatorSetApproval{{NodeID: [20]byte{3}, PChainHeight: 300, Signature: []byte{3}, AuxInfoSeqDigest: [32]byte{}}},
			}
		}
	}

	t.Log("Epoch:", node.Epoch())
	require.Greater(t, node.Epoch(), epoch)
	require.Equal(t, node.Height(), uint64(30))
}

type innerBlock struct {
	InnerBlock
	Prev [32]byte
}

type blockState struct {
	block     StateMachineBlock
	finalized bool
}

type multiEpochNode struct {
	t            *testing.T
	sm           *StateMachine
	mempoolEmpty bool
	// blocks holds notarized blocks in order. Finalized blocks always form a
	// prefix: all finalized entries precede all non-finalized entries.
	blocks []blockState
}

func (fn *multiEpochNode) WaitForProgress(ctx context.Context, pChainHeight uint64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
			if fn.sm.GetPChainHeight() != pChainHeight {
				return nil
			}
		}
	}
}

func (fn *multiEpochNode) WaitForPendingBlock(ctx context.Context) {
	if fn.mempoolEmpty {
		<-ctx.Done()
		return
	}
}

func newMultiEpochNode(t *testing.T) *multiEpochNode {
	sm, _ := newStateMachine(t)

	fn := &multiEpochNode{
		t:  t,
		sm: sm,
	}

	fn.sm.BlockBuilder = fn
	fn.sm.PChainProgressListener = fn

	fn.sm.GetBlock = func(opts RetrievingOpts) (StateMachineBlock, *simplex.Finalization, error) {
		if opts.Height == 0 {
			return genesisBlock, nil, nil
		}
		for _, bs := range fn.blocks {
			match := bs.block.Digest() == opts.Digest
			if !match {
				md, err := simplex.ProtocolMetadataFromBytes(bs.block.Metadata.SimplexProtocolMetadata)
				if err != nil {
					return StateMachineBlock{}, nil, err
				}
				match = md.Seq == opts.Height
			}
			if match {
				var fin *simplex.Finalization
				if bs.finalized {
					fin = &simplex.Finalization{}
				}
				return bs.block, fin, nil
			}
		}

		require.Failf(t, "not found block", "height: %d", opts.Height)
		return StateMachineBlock{}, nil, fmt.Errorf("block not found")
	}

	return fn
}

// lastFinalizedBlock returns the most recently finalized block.
// Panics if nothing has been finalized.
func (fn *multiEpochNode) lastFinalizedBlock() StateMachineBlock {
	for i := len(fn.blocks) - 1; i >= 0; i-- {
		if fn.blocks[i].finalized {
			return fn.blocks[i].block
		}
	}
	panic("no finalized block")
}

func (fn *multiEpochNode) Height() uint64 {
	var count uint64
	for _, bs := range fn.blocks {
		if bs.finalized {
			count++
		}
	}
	return count
}

func (fn *multiEpochNode) Epoch() uint64 {
	return fn.blocks[len(fn.blocks)-1].block.Metadata.SimplexEpochInfo.EpochNumber
}

// act randomly either finalizes a notarized block, builds and notarizes a new block, or does nothing.
func (fn *multiEpochNode) act() {
	if fn.canFinalize() && flipCoin() {
		fn.tryFinalizeNextBlock()
		return
	}

	if flipCoin() {
		return
	}

	fn.buildAndNotarizeBlock()
}

func (fn *multiEpochNode) canFinalize() bool {
	return fn.nextUnfinalizedIndex() < len(fn.blocks)
}

func (fn *multiEpochNode) nextUnfinalizedIndex() int {
	for i, bs := range fn.blocks {
		if !bs.finalized {
			return i
		}
	}
	return len(fn.blocks)
}

func (fn *multiEpochNode) tryFinalizeNextBlock() {
	nextIndex := fn.nextUnfinalizedIndex()

	if fn.isNextBlockTelock(nextIndex) {
		return
	}

	fn.blocks[nextIndex].finalized = true
	block := fn.blocks[nextIndex].block

	md, err := simplex.ProtocolMetadataFromBytes(block.Metadata.SimplexProtocolMetadata)
	require.NoError(fn.t, err)

	fn.sm.LatestPersistedHeight = md.Seq
	fn.t.Logf("Finalized block at height %d with epoch %d", md.Seq, block.Metadata.SimplexEpochInfo.EpochNumber)

	// If we just finalized a sealing block, trim trailing Telock blocks.
	if block.Metadata.SimplexEpochInfo.BlockValidationDescriptor != nil {
		fn.blocks = fn.blocks[:nextIndex+1]
		fn.t.Logf("Trimmed notarized blocks, new length: %d", len(fn.blocks))
	}
}

func (fn *multiEpochNode) isNextBlockTelock(nextIndex int) bool {
	if nextIndex == 0 {
		return false
	}
	return fn.blocks[nextIndex].block.Metadata.SimplexEpochInfo.SealingBlockSeq > 0
}

func (fn *multiEpochNode) buildAndNotarizeBlock() {
	block := fn.buildBlock()
	require.NoError(fn.t, fn.sm.VerifyBlock(context.Background(), block))

	fn.blocks = append(fn.blocks, blockState{block: *block})
}

func (fn *multiEpochNode) buildBlock() *StateMachineBlock {
	parentBlock := fn.getParentBlock()

	lastMD, prevBlockDigest := fn.prepareMetadataAndPrevBlockDigest()

	_, finalization, err := fn.sm.GetBlock(RetrievingOpts{
		Digest: prevBlockDigest,
		Height: lastMD.Seq,
	})
	require.NoError(fn.t, err)

	finalizedString := "not finalized"
	if finalization != nil {
		finalizedString = "finalized"
	}

	fn.t.Logf("Building a block on top of %s parent with epoch %d", finalizedString, parentBlock.Metadata.SimplexEpochInfo.EpochNumber)

	block, err := fn.sm.BuildBlock(context.Background(), parentBlock, simplex.ProtocolMetadata{
		Seq:   lastMD.Seq + 1,
		Round: lastMD.Round + 1,
		Prev:  prevBlockDigest,
	}, simplex.Blacklist{})
	require.NoError(fn.t, err)

	return block
}

func (fn *multiEpochNode) prepareMetadataAndPrevBlockDigest() (*simplex.ProtocolMetadata, [32]byte) {
	var lastMD *simplex.ProtocolMetadata
	var err error
	lastBlockDigest := genesisBlock.Digest()
	if len(fn.blocks) > 0 {
		lastBlock := fn.blocks[len(fn.blocks)-1].block
		lastBlockDigest = lastBlock.Digest()
		lastMD, err = simplex.ProtocolMetadataFromBytes(lastBlock.Metadata.SimplexProtocolMetadata)
		require.NoError(fn.t, err)
	} else {
		lastMD = &simplex.ProtocolMetadata{
			Prev: lastBlockDigest,
		}
	}
	return lastMD, lastBlockDigest
}

func (fn *multiEpochNode) BuildBlock(context.Context, uint64) (VMBlock, error) {
	// Count the number of inner blocks in the chain
	var count int
	for _, bs := range fn.blocks {
		if bs.block.InnerBlock != nil {
			count++
		}
	}

	vmBlock := &innerBlock{
		Prev: fn.getLastVMBlockDigest(),
		InnerBlock: InnerBlock{
			Bytes:       randomBuff(10),
			TS:          time.Now(),
			BlockHeight: uint64(count),
		},
	}
	return vmBlock, nil
}

func (fn *multiEpochNode) getParentBlock() StateMachineBlock {
	if len(fn.blocks) > 0 {
		return fn.blocks[len(fn.blocks)-1].block
	}
	gb := genesisBlock.InnerBlock.(*InnerBlock)
	return StateMachineBlock{
		InnerBlock: &innerBlock{
			InnerBlock: *gb,
		},
	}
}

func (fn *multiEpochNode) getLastVMBlockDigest() [32]byte {
	for i := len(fn.blocks) - 1; i >= 0; i-- {
		if fn.blocks[i].block.InnerBlock != nil {
			return fn.blocks[i].block.Digest()
		}
	}
	return genesisBlock.Digest()
}

func randomBuff(n int) []byte {
	buff := make([]byte, n)
	_, err := rand.Read(buff)
	if err != nil {
		panic(err)
	}
	return buff
}

func flipCoin() bool {
	buff := make([]byte, 1)
	_, err := rand.Read(buff)
	if err != nil {
		panic(err)
	}

	lsb := buff[0] & 1

	return lsb == 1
}

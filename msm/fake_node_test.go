// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/stretchr/testify/require"
)

func TestFakeNode(t *testing.T) {
	validatorSetRetriever := validatorSetRetriever{
		resultMap: map[uint64]metadata.NodeBLSMappings{
			100: {{BLSKey: []byte{1}, Weight: 1, NodeID: [20]byte{1}}, {BLSKey: []byte{2}, Weight: 1, NodeID: [20]byte{2}}},
			200: {{BLSKey: []byte{1}, Weight: 1, NodeID: [20]byte{1}}, {BLSKey: []byte{2}, Weight: 2, NodeID: [20]byte{2}},
				{BLSKey: []byte{3}, Weight: 1, NodeID: [20]byte{3}}},
			300: {{BLSKey: []byte{1}, Weight: 1, NodeID: [20]byte{1}}, {BLSKey: []byte{2}, Weight: 2, NodeID: [20]byte{2}},
				{BLSKey: []byte{3}, Weight: 3, NodeID: [20]byte{3}}, {BLSKey: []byte{4}, Weight: 1, NodeID: [20]byte{4}}},
		},
	}

	var pChainHeight uint64 = 100
	node := newFakeNode(t)
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

	for node.Height() < 20 {
		node.act()
		if flipCoin() {
			node.sm.ApprovalsRetriever = &approvalsRetriever{
				result: []metadata.ValidatorSetApproval{{NodeID: [20]byte{1}, PChainHeight: 200, Signature: []byte{1}, AuxInfoSeqDigest: [32]byte{}}},
			}
		} else {
			node.sm.ApprovalsRetriever = &approvalsRetriever{
				result: []metadata.ValidatorSetApproval{{NodeID: [20]byte{2}, PChainHeight: 200, Signature: []byte{2}, AuxInfoSeqDigest: [32]byte{}}},
			}
		}
	}

	t.Log("Epoch:", node.Epoch())

	epoch := node.Epoch()
	require.Greater(t, epoch, uint64(1))
	require.Equal(t, node.Height(), uint64(20))


	// Finally, we increase the P-Chain height again, which should cause the node to update its validator set and move to the new epoch.

	pChainHeight = 300

	for node.Height() < 30 {
		node.act()
		if flipCoin() {
			node.sm.ApprovalsRetriever = &approvalsRetriever{
				result: []metadata.ValidatorSetApproval{{NodeID: [20]byte{2}, PChainHeight: 300, Signature: []byte{2}, AuxInfoSeqDigest: [32]byte{}}},
			}
		} else {
			node.sm.ApprovalsRetriever = &approvalsRetriever{
				result: []metadata.ValidatorSetApproval{{NodeID: [20]byte{3}, PChainHeight: 300, Signature: []byte{3}, AuxInfoSeqDigest: [32]byte{}}},
			}
		}
	}

	t.Log("Epoch:", node.Epoch())
	require.Greater(t, node.Epoch(), epoch)
	require.Equal(t, node.Height(), uint64(30))
}

func TestFakeNodeEmptyMempool(t *testing.T) {
	validatorSetRetriever := validatorSetRetriever{
		resultMap: map[uint64]metadata.NodeBLSMappings{
			100: {{BLSKey: []byte{1}, Weight: 1, NodeID: [20]byte{1}}, {BLSKey: []byte{2}, Weight: 1, NodeID: [20]byte{2}}},
			200: {{BLSKey: []byte{1}, Weight: 1, NodeID: [20]byte{1}}, {BLSKey: []byte{2}, Weight: 2, NodeID: [20]byte{2}},
				{BLSKey: []byte{3}, Weight: 1, NodeID: [20]byte{3}}},
			300: {{BLSKey: []byte{1}, Weight: 1, NodeID: [20]byte{1}}, {BLSKey: []byte{2}, Weight: 2, NodeID: [20]byte{2}},
				{BLSKey: []byte{3}, Weight: 3, NodeID: [20]byte{3}}, {BLSKey: []byte{4}, Weight: 1, NodeID: [20]byte{4}}},
		},
	}

	var pChainHeight uint64 = 100
	node := newFakeNode(t)
	node.mempoolEmpty = true
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

	for node.Height() < 20 {
		node.act()
		if flipCoin() {
			node.sm.ApprovalsRetriever = &approvalsRetriever{
				result: []metadata.ValidatorSetApproval{{NodeID: [20]byte{1}, PChainHeight: 200, Signature: []byte{1}, AuxInfoSeqDigest: [32]byte{}}},
			}
		} else {
			node.sm.ApprovalsRetriever = &approvalsRetriever{
				result: []metadata.ValidatorSetApproval{{NodeID: [20]byte{2}, PChainHeight: 200, Signature: []byte{2}, AuxInfoSeqDigest: [32]byte{}}},
			}
		}
	}

	t.Log("Epoch:", node.Epoch())

	epoch := node.Epoch()
	require.Greater(t, epoch, uint64(1))
	require.Equal(t, node.Height(), uint64(20))


	// Finally, we increase the P-Chain height again, which should cause the node to update its validator set and move to the new epoch.

	pChainHeight = 300

	for node.Height() < 30 {
		node.act()
		if flipCoin() {
			node.sm.ApprovalsRetriever = &approvalsRetriever{
				result: []metadata.ValidatorSetApproval{{NodeID: [20]byte{2}, PChainHeight: 300, Signature: []byte{2}, AuxInfoSeqDigest: [32]byte{}}},
			}
		} else {
			node.sm.ApprovalsRetriever = &approvalsRetriever{
				result: []metadata.ValidatorSetApproval{{NodeID: [20]byte{3}, PChainHeight: 300, Signature: []byte{3}, AuxInfoSeqDigest: [32]byte{}}},
			}
		}
	}

	t.Log("Epoch:", node.Epoch())
	require.Greater(t, node.Epoch(), epoch)
	require.Equal(t, node.Height(), uint64(30))
}

type innerBlock struct {
	metadata.InnerBlock
	Prev [32]byte
}

type fakeNode struct {
	t               *testing.T
	sm              metadata.StateMachine
	mempoolEmpty    bool
	notarizedBlocks []metadata.StateMachineBlock
	finalizedBlocks []metadata.StateMachineBlock
	innerChain      []innerBlock
}

func (fn *fakeNode) WaitForPendingBlock(ctx context.Context) {
	//TODO implement me
	panic("implement me")
}

func newFakeNode(t *testing.T) *fakeNode {
	sm, _ := newStateMachine(t)

	fn := &fakeNode{
		t:  t,
		sm: sm,
	}

	fn.sm.BlockBuilder = fn

	fn.sm.GetBlock = func(opts metadata.RetrievingOpts) (metadata.StateMachineBlock, *simplex.Finalization, error) {
		if opts.Height == 0 {
			return genesisBlock, nil, nil
		}
		for _, block := range fn.finalizedBlocks {
			if block.Digest() == opts.Digest {
				return block, &simplex.Finalization{}, nil
			}
			md, err := simplex.ProtocolMetadataFromBytes(block.Metadata.SimplexProtocolMetadata)
			if err != nil {
				return metadata.StateMachineBlock{}, nil, err
			}
			if md.Seq == opts.Height {
				return block, &simplex.Finalization{}, nil
			}
		}
		for _, block := range fn.notarizedBlocks {
			if block.Digest() == opts.Digest {
				return block, nil, nil
			}
			md, err := simplex.ProtocolMetadataFromBytes(block.Metadata.SimplexProtocolMetadata)
			if err != nil {
				return metadata.StateMachineBlock{}, nil, err
			}
			if md.Seq == opts.Height {
				return block, nil, nil
			}
		}

		require.Failf(t, "not found block", "height: %d", opts.Height)
		return metadata.StateMachineBlock{}, nil, fmt.Errorf("block not found")
	}

	return fn
}

func (fn *fakeNode) Height() uint64 {
	return uint64(len(fn.finalizedBlocks))
}

func (fn *fakeNode) Epoch() uint64 {
	return fn.notarizedBlocks[len(fn.notarizedBlocks)-1].Metadata.SimplexEpochInfo.EpochNumber
}

func (fn *fakeNode) act() {
	if len(fn.notarizedBlocks) > len(fn.finalizedBlocks) && flipCoin() {
		// Check if we manage to finalize a notarized block
		nextIndexToFinalize := len(fn.finalizedBlocks)
		fn.finalizedBlocks = append(fn.finalizedBlocks, fn.notarizedBlocks[nextIndexToFinalize])
		md, err := simplex.ProtocolMetadataFromBytes(fn.finalizedBlocks[len(fn.finalizedBlocks)-1].Metadata.SimplexProtocolMetadata)
		require.NoError(fn.t, err)
		fn.sm.LatestPersistedHeight = md.Seq
		return
	}

	if flipCoin() {
		return
	}

	// Build a new block
	vmBlock, block := fn.buildBlock()
	// Verify it
	require.NoError(fn.t, fn.sm.VerifyBlock(context.Background(), block))

	fn.notarizedBlocks = append(fn.notarizedBlocks, *block)

	if vmBlock != nil {
		fn.innerChain = append(fn.innerChain, *vmBlock.(*innerBlock))
	}
}

func (fn *fakeNode) buildBlock() (metadata.VMBlock, *metadata.StateMachineBlock) {
	parentBlock := fn.getParentBlock()

	lastMD, prevBlockDigest := fn.prepareMetadataAndPrevBlockDigest()

	block, err := fn.sm.BuildBlock(context.Background(), parentBlock, simplex.ProtocolMetadata{
		Seq:   lastMD.Seq + 1,
		Round: lastMD.Round + 1,
		Prev:  prevBlockDigest,
	}, nil)
	require.NoError(fn.t, err)

	return block.InnerBlock, block
}

func (fn *fakeNode) prepareMetadataAndPrevBlockDigest() (*simplex.ProtocolMetadata, [32]byte) {
	var lastMD *simplex.ProtocolMetadata
	var err error
	lastBlockDigest := genesisBlock.Digest()
	if len(fn.notarizedBlocks) > 0 {
		lastBlock := fn.notarizedBlocks[len(fn.notarizedBlocks)-1]
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

func (fn *fakeNode) BuildBlock(context.Context, uint64) (metadata.VMBlock, error) {
	// Count the number of inner blocks in the chain
	var count int
	for _, block := range fn.notarizedBlocks {
		if block.InnerBlock != nil {
			count++
		}
	}

	vmBlock := &innerBlock{
		Prev: fn.getLastVMBlockDigest(),
		InnerBlock: metadata.InnerBlock{
			Bytes:       randomBuff(10),
			TS:          time.Now(),
			BlockHeight: uint64(count),
		},
	}
	return vmBlock, nil
}

func (fn *fakeNode) getParentBlock() metadata.StateMachineBlock {
	var parentBlock metadata.StateMachineBlock
	if len(fn.notarizedBlocks) > 0 {
		parentBlock = fn.notarizedBlocks[len(fn.notarizedBlocks)-1]
	} else {
		gb := genesisBlock.InnerBlock.(*metadata.InnerBlock)
		parentBlock = metadata.StateMachineBlock{
			InnerBlock: &innerBlock{
				InnerBlock: *gb,
			},
		}
	}
	return parentBlock
}

func (fn *fakeNode) getLastVMBlockDigest() [32]byte {
	var lastVMBlockDigest = genesisBlock.Digest()

	notarizedBlocks := fn.notarizedBlocks
	for len(notarizedBlocks) > 0 {
		lastNotarizedBlock := notarizedBlocks[len(notarizedBlocks)-1]
		if lastNotarizedBlock.InnerBlock == nil {
			notarizedBlocks = notarizedBlocks[:len(notarizedBlocks)-1]
			continue
		}
		lastVMBlockDigest = lastNotarizedBlock.Digest()
		break
	}
	return lastVMBlockDigest
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

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/asn1"
	"errors"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/ava-labs/simplex/common"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

// Test helpers

type InnerBlock struct {
	TS          time.Time
	BlockHeight uint64
	Bytes       []byte
}

func (i *InnerBlock) Digest() [32]byte {
	return sha256.Sum256(i.Bytes)
}

func (i *InnerBlock) Height() uint64 {
	return i.BlockHeight
}

func (i *InnerBlock) Timestamp() time.Time {
	return i.TS
}

func (i *InnerBlock) Verify(_ context.Context, _ uint64) error {
	return nil
}

// fakeVMBlock is a minimal VMBlock implementation for tests.
type fakeVMBlock struct {
	height uint64
}

func (f *fakeVMBlock) Digest() [32]byte                         { return [32]byte{} }
func (f *fakeVMBlock) Height() uint64                           { return f.height }
func (f *fakeVMBlock) Timestamp() time.Time                     { return time.Time{} }
func (f *fakeVMBlock) Verify(_ context.Context, _ uint64) error { return nil }

type outerBlock struct {
	finalization *common.Finalization
	block        StateMachineBlock
}

type blockStore map[uint64]*outerBlock

func (bs blockStore) clone() blockStore {
	newStore := make(blockStore)
	maps.Copy(newStore, bs)
	return newStore
}

func (bs blockStore) getBlock(seq uint64, _ [32]byte) (StateMachineBlock, *common.Finalization, error) {
	blk, exits := bs[seq]
	if !exits {
		return StateMachineBlock{}, nil, fmt.Errorf("%w: block %d not found", common.ErrBlockNotFound, seq)
	}
	return blk.block, blk.finalization, nil
}

type approvalsRetriever struct {
	result ValidatorSetApprovals
}

func (a approvalsRetriever) Approvals() ValidatorSetApprovals {
	return a.result
}

type signatureVerifier struct {
	err error
}

func (sv *signatureVerifier) VerifySignature(signature []byte, message []byte, publicKey []byte) error {
	return sv.err
}

type signatureAggregator struct {
	weightByNodeID map[string]uint64
	totalWeight    uint64
}

type aggregatrdSignature struct {
	Signatures [][]byte
}

func (sv *signatureAggregator) Aggregate([]common.Signature) (common.QuorumCertificate, error) {
	panic("unused in tests")
}

func (sv *signatureAggregator) AppendSignatures(existing []byte, sigs ...[]byte) ([]byte, error) {
	all := make([][]byte, 0, len(sigs)+1)
	all = append(all, sigs...)
	if len(existing) > 0 {
		all = append(all, existing)
	}
	return asn1.Marshal(aggregatrdSignature{Signatures: all})
}

func (sv *signatureAggregator) IsQuorum(signers []common.NodeID) bool {
	var sum uint64
	for _, signer := range signers {
		sum += sv.weightByNodeID[string(signer)]
	}
	return sum*3 > sv.totalWeight*2
}

func newSignatureAggregatorCreator() common.SignatureAggregatorCreator {
	return func(weights []common.Node) common.SignatureAggregator {
		s := &signatureAggregator{weightByNodeID: make(map[string]uint64, len(weights))}
		for _, nw := range weights {
			s.weightByNodeID[string(nw.Node)] = nw.Weight
			s.totalWeight += nw.Weight
		}
		return s
	}
}

type noOpPChainListener struct{}

func (n *noOpPChainListener) WaitForProgress(ctx context.Context, _ uint64) error {
	<-ctx.Done()
	return ctx.Err()
}

type blockBuilder struct {
	block VMBlock
	err   error
}

func (bb *blockBuilder) WaitForPendingBlock(_ context.Context) {
	// Block is always ready in tests.
}

func (bb *blockBuilder) BuildBlock(_ context.Context, _ uint64) (VMBlock, error) {
	return bb.block, bb.err
}

type validatorSetRetriever struct {
	result    NodeBLSMappings
	resultMap map[uint64]NodeBLSMappings
	err       error
}

func (vsr *validatorSetRetriever) getValidatorSet(height uint64) (NodeBLSMappings, error) {
	if vsr.resultMap != nil {
		if result, ok := vsr.resultMap[height]; ok {
			return result, vsr.err
		}
	}
	return vsr.result, vsr.err
}

type keyAggregator struct{}

func (ka *keyAggregator) AggregateKeys(keys ...[]byte) ([]byte, error) {
	aggregated := make([]byte, 0)
	for _, key := range keys {
		aggregated = append(aggregated, key...)
	}
	return aggregated, nil
}

var (
	genesisBlock = StateMachineBlock{
		// Genesis block metadata has all zero values
		InnerBlock: &InnerBlock{
			TS:    time.Now(),
			Bytes: []byte{1, 2, 3},
		},
	}
)

type dynamicApprovalsRetriever struct {
	approvals *ValidatorSetApprovals
}

func (d *dynamicApprovalsRetriever) Approvals() ValidatorSetApprovals {
	return *d.approvals
}

func makeChain(t *testing.T, simplexStartHeight uint64, endHeight uint64) []StateMachineBlock {
	startTime := time.Now().Add(-time.Duration(endHeight+2) * time.Second)
	blocks := make([]StateMachineBlock, 0, endHeight+1)
	var round, seq uint64
	for h := uint64(0); h <= endHeight; h++ {
		index := len(blocks)

		if h == 0 {
			blocks = append(blocks, genesisBlock)
			continue
		}

		if h < simplexStartHeight {
			blocks = append(blocks, makeNonSimplexBlock(t, simplexStartHeight, startTime, h))
			continue
		}

		seq = uint64(index)

		blocks = append(blocks, makeNormalSimplexBlock(t, index, blocks, startTime, h, round, seq))
		round++
	}
	return blocks
}

func makeNormalSimplexBlock(t *testing.T, index int, blocks []StateMachineBlock, start time.Time, h uint64, round uint64, seq uint64) StateMachineBlock {
	content := make([]byte, 10)
	_, err := rand.Read(content)
	require.NoError(t, err)

	prev := genesisBlock.Digest()
	if index > 0 {
		prev = blocks[index-1].Digest()
	}

	return StateMachineBlock{
		InnerBlock: &InnerBlock{
			TS:          start.Add(time.Duration(h) * time.Second),
			BlockHeight: h,
			Bytes:       []byte{1, 2, 3},
		},
		Metadata: StateMachineMetadata{
			PChainHeight: 100,
			SimplexProtocolMetadata: (&common.ProtocolMetadata{
				Round: round,
				Seq:   seq,
				Epoch: 1,
				Prev:  prev,
			}).Bytes(),
			SimplexEpochInfo: SimplexEpochInfo{
				PrevSealingBlockHash:  [32]byte{},
				PChainReferenceHeight: 100,
				EpochNumber:           1,
				PrevVMBlockSeq:        uint64(index),
			},
		},
	}
}

func makeNonSimplexBlock(t *testing.T, startHeight uint64, start time.Time, h uint64) StateMachineBlock {
	content := make([]byte, 10)
	_, err := rand.Read(content)
	require.NoError(t, err)

	return StateMachineBlock{
		InnerBlock: &InnerBlock{
			TS:          start.Add(time.Duration(h-startHeight) * time.Second),
			BlockHeight: h,
			Bytes:       []byte{1, 2, 3},
		},
	}
}

type testConfig struct {
	blockStore            blockStore
	approvalsRetriever    approvalsRetriever
	signatureVerifier     signatureVerifier
	signatureAggregator   signatureAggregator
	blockBuilder          blockBuilder
	keyAggregator         keyAggregator
	validatorSetRetriever validatorSetRetriever
}

func newStateMachine(t *testing.T) (*StateMachine, *testConfig) {
	return newStateMachineWithLogger(t, testutil.MakeLogger(t))
}

func newStateMachineWithLogger(tb testing.TB, logger common.Logger) (*StateMachine, *testConfig) {
	bs := make(blockStore)
	bs[0] = &outerBlock{block: genesisBlock}

	var myNodeID nodeID

	var testConfig testConfig
	testConfig.blockStore = bs
	testConfig.validatorSetRetriever.result = NodeBLSMappings{
		{BLSKey: []byte{1}, Weight: 1}, {BLSKey: []byte{2}, Weight: 1},
	}

	smConfig := Config{
		GenesisValidatorSet:             NodeBLSMappings{{BLSKey: []byte{1}, Weight: 1}, {BLSKey: []byte{2}, Weight: 1}},
		LastNonSimplexBlockPChainHeight: 100,
		GetTime:                         time.Now,
		TimeSkewLimit:                   time.Second * 5,
		Logger:                          logger,
		GetBlock:                        testConfig.blockStore.getBlock,
		MaxBlockBuildingWaitTime:        time.Second,
		ApprovalsRetriever:              &testConfig.approvalsRetriever,
		SignatureVerifier:               &testConfig.signatureVerifier,
		SignatureAggregatorCreator:      newSignatureAggregatorCreator(),
		BlockBuilder:                    &testConfig.blockBuilder,
		KeyAggregator:                   &testConfig.keyAggregator,
		GetPChainHeight: func() uint64 {
			return 100
		},
		GetValidatorSet:          testConfig.validatorSetRetriever.getValidatorSet,
		PChainProgressListener:   &noOpPChainListener{},
		LastNonSimplexInnerBlock: genesisBlock.InnerBlock,
		MyNodeID:                 myNodeID[:],
		Signer:                   &testutil.TestSigner{},
		ComputeICMEpoch: func(input ICMEpochInput) ICMEpochInfo {
			// This is just the ACP-181 implementation from avalanchego
			var zeroEpoch ICMEpochInfo
			if input.ParentEpoch == zeroEpoch {
				return ICMEpochInfo{
					PChainEpochHeight: input.ParentPChainHeight,
					EpochNumber:       1,
					EpochStartTime:    uint64(input.ParentTimestamp.Unix()),
				}
			}
			endTime := time.Unix(int64(input.ParentEpoch.EpochStartTime), 0).Add(time.Second)
			if input.ParentTimestamp.Before(endTime) {
				return input.ParentEpoch
			}
			return ICMEpochInfo{
				PChainEpochHeight: input.ParentPChainHeight,
				EpochNumber:       input.ParentEpoch.EpochNumber + 1,
				EpochStartTime:    uint64(input.ParentTimestamp.Unix()),
			}
		},
	}

	sm, err := NewStateMachine(&smConfig)
	require.NoError(tb, err)

	return sm, &testConfig
}

// concatAggregator concatenates signatures for easy verification in tests.
type concatAggregator struct{}

func (concatAggregator) Aggregate([]common.Signature) (common.QuorumCertificate, error) {
	panic("unused in tests")
}

func (concatAggregator) AppendSignatures(existing []byte, sigs ...[]byte) ([]byte, error) {
	result := bytes.Join(sigs, nil)
	return append(result, existing...), nil
}

func (concatAggregator) IsQuorum([]common.NodeID) bool {
	return false
}

type failingAggregator struct{}

func (failingAggregator) Aggregate([]common.Signature) (common.QuorumCertificate, error) {
	panic("unused in tests")
}

var errTestAggregationFailed = errors.New("aggregation failed")

func (failingAggregator) AppendSignatures([]byte, ...[]byte) ([]byte, error) {
	return nil, errTestAggregationFailed
}

func (failingAggregator) IsQuorum([]common.NodeID) bool {
	return false
}

type testBlockStore map[uint64]StateMachineBlock

func (bs testBlockStore) getBlock(seq uint64, _ [32]byte) (StateMachineBlock, *common.Finalization, error) {
	blk, ok := bs[seq]
	if !ok {
		return StateMachineBlock{}, nil, fmt.Errorf("%w: block %d", common.ErrBlockNotFound, seq)
	}
	return blk, nil, nil
}

type testVMBlock struct {
	bytes  []byte
	height uint64
}

func (b *testVMBlock) Digest() [32]byte {
	return sha256.Sum256(b.bytes)
}

func (b *testVMBlock) Height() uint64 {
	return b.height
}

func (b *testVMBlock) Timestamp() time.Time {
	return time.Now()
}

func (b *testVMBlock) Verify(_ context.Context, _ uint64) error {
	return nil
}

type testSigVerifier struct {
	err error
}

func (sv *testSigVerifier) VerifySignature(_, _, _ []byte) error {
	return sv.err
}

type testKeyAggregator struct {
	err error
}

func (ka *testKeyAggregator) AggregateKeys(keys ...[]byte) ([]byte, error) {
	if ka.err != nil {
		return nil, ka.err
	}
	var agg []byte
	for _, k := range keys {
		agg = append(agg, k...)
	}
	return agg, nil
}

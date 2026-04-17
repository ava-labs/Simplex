// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"context"
	"encoding/asn1"
	"fmt"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
)

// fakeVMBlock is a minimal VMBlock implementation for tests.
type fakeVMBlock struct {
	height uint64
}

func (f *fakeVMBlock) Digest() [32]byte               { return [32]byte{} }
func (f *fakeVMBlock) Height() uint64                 { return f.height }
func (f *fakeVMBlock) Timestamp() time.Time           { return time.Time{} }
func (f *fakeVMBlock) Verify(_ context.Context) error { return nil }

type outerBlock struct {
	finalization *simplex.Finalization
	block        StateMachineBlock
}

type blockStore map[uint64]*outerBlock

func (bs blockStore) clone() blockStore {
	newStore := make(blockStore)
	for k, v := range bs {
		newStore[k] = v
	}
	return newStore
}

func (bs blockStore) getBlock(seq uint64, _ [32]byte) (StateMachineBlock, *simplex.Finalization, error) {
	blk, exists := bs[seq]
	if !exists {
		return StateMachineBlock{}, nil, fmt.Errorf("%w: block %d not found", simplex.ErrBlockNotFound, seq)
	}
	return blk.block, blk.finalization, nil
}

type approvalsRetriever struct {
	result ValidatorSetApprovals
}

func (a approvalsRetriever) RetrieveApprovals() ValidatorSetApprovals {
	return a.result
}

type signatureVerifier struct {
	err error
}

func (sv *signatureVerifier) VerifySignature(signature []byte, message []byte, publicKey []byte) error {
	return sv.err
}

type signatureAggregator struct{}

type aggregatrdSignature struct {
	Signatures [][]byte
}

func (sv *signatureAggregator) AggregateSignatures(signatures ...[]byte) ([]byte, error) {
	bytes, err := asn1.Marshal(aggregatrdSignature{Signatures: signatures})
	if err != nil {
		return nil, err
	}
	return bytes, nil
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
	emptyBlacklistBytes = func() []byte {
		var b simplex.Blacklist
		return b.Bytes()
	}()
)

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
	bs := make(blockStore)

	var testConfig testConfig
	testConfig.blockStore = bs
	testConfig.validatorSetRetriever.result = NodeBLSMappings{
		{BLSKey: []byte{1}, Weight: 1}, {BLSKey: []byte{2}, Weight: 1},
	}

	sm := NewStateMachine(Config{
		GetTime:                  time.Now,
		TimeSkewLimit:            time.Second * 5,
		Logger:                   testutil.MakeLogger(t),
		GetBlock:                 testConfig.blockStore.getBlock,
		MaxBlockBuildingWaitTime: time.Second,
		ApprovalsRetriever:       &testConfig.approvalsRetriever,
		SignatureVerifier:        &testConfig.signatureVerifier,
		SignatureAggregator:      &testConfig.signatureAggregator,
		BlockBuilder:             &testConfig.blockBuilder,
		KeyAggregator:            &testConfig.keyAggregator,
		GetPChainHeight: func() uint64 {
			return 100
		},
		GetUpgrades: func() any {
			return nil
		},
		GetValidatorSet:        testConfig.validatorSetRetriever.getValidatorSet,
		PChainProgressListener: &noOpPChainListener{},
	})
	return sm, &testConfig
}

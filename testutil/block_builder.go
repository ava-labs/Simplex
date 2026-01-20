// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
)

type TestBlockBuilder struct {
	// built is a channel that holds built test blocks
	built              chan *TestBlock
	BlockShouldBeBuilt chan struct{}
}

func NewTestBlockBuilder() *TestBlockBuilder {
	return &TestBlockBuilder{
		built:              make(chan *TestBlock, 1),
		BlockShouldBeBuilt: make(chan struct{}, 1),
	}
}

func (t *TestBlockBuilder) WithBuiltBuffer(buffer uint64) *TestBlockBuilder {
	t.built = make(chan *TestBlock, buffer)
	return t
}

func (t *TestBlockBuilder) WithBlockShouldBeBuiltBuffer(buffer uint64) *TestBlockBuilder {
	t.BlockShouldBeBuilt = make(chan struct{}, buffer)
	return t
}

func (t *TestBlockBuilder) BuildBlock(_ context.Context, metadata simplex.ProtocolMetadata, blacklist simplex.Blacklist) (simplex.VerifiedBlock, bool) {
	tb := NewTestBlock(metadata, blacklist)

	select {
	case t.built <- tb:
		return tb, true
	default:
	}
	return tb, true
}

func (t *TestBlockBuilder) GetBuiltBlock() *TestBlock {
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()

	select {
	case b := <-t.built:
		return b
	case <-timeout.C:
		panic("timed out waiting for built block")
	}
}

func (t *TestBlockBuilder) SetBuiltBlock(block *TestBlock) {
	select {
	case t.built <- block:
	default:
		panic("built channel is full")
	}
}

func (t *TestBlockBuilder) WaitForPendingBlock(ctx context.Context) {
	select {
	case <-t.BlockShouldBeBuilt:
	case <-ctx.Done():
	}
}

// testControlledBlockBuilder is a BlockBuilder that only builds a block when
// a control signal is received.
type testControlledBlockBuilder struct {
	t       *testing.T
	control chan struct{}
	TestBlockBuilder
}

// NewTestControlledBlockBuilder returns a BlockBuilder that only builds a block
// when triggerNewBlock is called.
func NewTestControlledBlockBuilder(t *testing.T) *testControlledBlockBuilder {
	return &testControlledBlockBuilder{
		t:                t,
		control:          make(chan struct{}, 1),
		TestBlockBuilder: *NewTestBlockBuilder(),
	}
}

func (t *testControlledBlockBuilder) TriggerNewBlock() {
	select {
	case t.control <- struct{}{}:
	default:
	}
}

func (t *testControlledBlockBuilder) BuildBlock(ctx context.Context, metadata simplex.ProtocolMetadata, blacklist simplex.Blacklist) (simplex.VerifiedBlock, bool) {
	select {
	case <-t.control:
	case <-ctx.Done():
		return nil, false
	}
	return t.TestBlockBuilder.BuildBlock(ctx, metadata, blacklist)
}

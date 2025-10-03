package testutil

import (
	"context"
	"testing"

	"github.com/ava-labs/simplex"
)

type TestBlockBuilder struct {
	Out                chan *TestBlock
	In                 chan *TestBlock
	BlockShouldBeBuilt chan struct{}
}

func NewTestBlockBuilder() *TestBlockBuilder {
	return &TestBlockBuilder{
		Out:                make(chan *TestBlock, 1),
		In:                 make(chan *TestBlock, 1),
		BlockShouldBeBuilt: make(chan struct{}, 1),
	}
}

// BuildBlock builds a new testblock and sends it to the BlockBuilder channel
func (t *TestBlockBuilder) BuildBlock(_ context.Context, metadata simplex.ProtocolMetadata, blacklist simplex.Blacklist) (simplex.VerifiedBlock, bool) {
	if len(t.In) > 0 {
		block := <-t.In
		return block, true
	}

	tb := NewTestBlock(metadata, blacklist)

	select {
	case t.Out <- tb:
	default:
	}

	return tb, true
}

func (t *TestBlockBuilder) WaitForPendingBlock(ctx context.Context) {
	select {
	case <-t.BlockShouldBeBuilt:
	case <-ctx.Done():
	}
}

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

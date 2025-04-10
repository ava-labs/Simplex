package testutil

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"simplex"
	"testing"
)

type TestBlockBuilder struct {
	Out                chan *TestBlock
	In                 chan *TestBlock
	BlockShouldBeBuilt chan struct{}
}

// BuildBlock builds a new testblock and sends it to the BlockBuilder channel
func (t *TestBlockBuilder) BuildBlock(_ context.Context, metadata simplex.ProtocolMetadata) (simplex.VerifiedBlock, bool) {
	if len(t.In) > 0 {
		block := <-t.In
		return block, true
	}

	tb := NewTestBlock(metadata)

	select {
	case t.Out <- tb:
	default:
	}

	return tb, true
}

func (t *TestBlockBuilder) IncomingBlock(ctx context.Context) {
	select {
	case <-t.BlockShouldBeBuilt:
	case <-ctx.Done():
	}
}

type TestBlock struct {
	data              []byte
	metadata          simplex.ProtocolMetadata
	OnVerify          func()
	Digest            [32]byte
	VerificationDelay chan struct{}
}

func (tb *TestBlock) Verify(context.Context) (simplex.VerifiedBlock, error) {
	defer func() {
		if tb.OnVerify != nil {
			tb.OnVerify()
		}
	}()
	if tb.VerificationDelay == nil {
		return tb, nil
	}

	<-tb.VerificationDelay

	return tb, nil
}

func NewTestBlock(metadata simplex.ProtocolMetadata) *TestBlock {
	tb := TestBlock{
		metadata: metadata,
		data:     make([]byte, 32),
	}

	_, err := rand.Read(tb.data)
	if err != nil {
		panic(err)
	}

	tb.computeDigest()

	return &tb
}

func (tb *TestBlock) computeDigest() {
	var bb bytes.Buffer
	bb.Write(tb.Bytes())
	tb.Digest = sha256.Sum256(bb.Bytes())
}

func (t *TestBlock) BlockHeader() simplex.BlockHeader {
	return simplex.BlockHeader{
		ProtocolMetadata: t.metadata,
		Digest:           t.Digest,
	}
}

func (t *TestBlock) Bytes() []byte {
	bh := simplex.BlockHeader{
		ProtocolMetadata: t.metadata,
	}

	mdBuff := bh.Bytes()

	buff := make([]byte, len(t.data)+len(mdBuff)+4)
	binary.BigEndian.PutUint32(buff, uint32(len(t.data)))
	copy(buff[4:], t.data)
	copy(buff[4+len(t.data):], mdBuff)
	return buff
}

// testControlledBlockBuilder is a test block builder that blocks
// block building until a trigger is received
type testControlledBlockBuilder struct {
	t       *testing.T
	control chan struct{}
	TestBlockBuilder
}

func NewTestControlledBlockBuilder(t *testing.T) *testControlledBlockBuilder {
	return &testControlledBlockBuilder{
		t:                t,
		control:          make(chan struct{}, 1),
		TestBlockBuilder: TestBlockBuilder{Out: make(chan *TestBlock, 1), BlockShouldBeBuilt: make(chan struct{}, 1)},
	}
}

func (t *testControlledBlockBuilder) TriggerNewBlock() {
	select {
	case t.control <- struct{}{}:
	default:

	}
}

func (t *testControlledBlockBuilder) BuildBlock(ctx context.Context, metadata simplex.ProtocolMetadata) (simplex.VerifiedBlock, bool) {
	<-t.control
	return t.TestBlockBuilder.BuildBlock(ctx, metadata)
}

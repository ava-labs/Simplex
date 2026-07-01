// block provides the Block and BlockBuilder implementations for the
// Jepsen test node. The design is inspired by testutil/block.go.
package block

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/asn1"
	"sync"

	"github.com/ava-labs/simplex/common"
)

// encodedBlock is the on-wire format for a Block to send through the network, marshaled with encoding/asn1.
type encodedBlock struct {
	Data      []byte
	Metadata  []byte
	Blacklist []byte
}

// Block implements common.Block and common.VerifiedBlock.
type Block struct {
	data     []byte
	header   common.BlockHeader
	blacklist common.Blacklist
}

// newBlock builds a Block and computes its digest.
func newBlock(data []byte, metadata common.ProtocolMetadata, blacklist common.Blacklist) *Block {
	b := &Block{
		data:     data,
		blacklist: blacklist,
	}
	b.header.ProtocolMetadata = metadata

	raw, err := b.bytesWithoutDigest()
	if err != nil {
		panic("block: failed to serialize for digest: " + err.Error())
	}
	b.header.Digest = sha256.Sum256(raw)
	return b
}

func (b *Block) bytesWithoutDigest() ([]byte, error) {
	mdBuf := b.header.ProtocolMetadata.Bytes()
	blBuf := b.blacklist.Bytes()
	data := b.data
	if bytes.Equal(blBuf, data) {
		data = nil
	}
	return asn1.Marshal(encodedBlock{
		Data:      data,
		Metadata:  mdBuf,
		Blacklist: blBuf,
	})
}

// Bytes returns a stable byte encoding of the Block (includes digest in header).
func (b *Block) Bytes() ([]byte, error) {
	mdBuf := b.header.ProtocolMetadata.Bytes()
	blBuf := b.blacklist.Bytes()
	data := b.data
	if bytes.Equal(blBuf, data) {
		data = nil
	}
	return asn1.Marshal(encodedBlock{
		Data:      data,
		Metadata:  mdBuf,
		Blacklist: blBuf,
	})
}

// BlockHeader returns the block header.
func (b *Block) BlockHeader() common.BlockHeader {
	return b.header
}

// Blacklist returns the blacklist state encoded in this block.
func (b *Block) Blacklist() common.Blacklist {
	return b.blacklist
}

// Verify satisfies common.Block, the block is already verified.
func (b *Block) Verify(_ context.Context) (common.VerifiedBlock, error) {
	return b, nil
}

// BlockDeserializer implements common.BlockDeserializer.
type BlockDeserializer struct{}

// DeserializeBlock parses bytes produced by Block.Bytes() into a Block.
func (bd *BlockDeserializer) DeserializeBlock(_ context.Context, buf []byte) (common.Block, error) {
	var enc encodedBlock
	if _, err := asn1.Unmarshal(buf, &enc); err != nil {
		return nil, err
	}

	md, err := common.ProtocolMetadataFromBytes(enc.Metadata)
	if err != nil {
		return nil, err
	}

	var blacklist common.Blacklist
	if err := blacklist.FromBytes(enc.Blacklist); err != nil {
		return nil, err
	}

	b := &Block{
		data:     enc.Data,
		blacklist: blacklist,
	}
	b.header.ProtocolMetadata = *md

	// Recompute the digest so it matches what was produced at build time.
	raw, err := b.bytesWithoutDigest()
	if err != nil {
		return nil, err
	}
	b.header.Digest = sha256.Sum256(raw)
	return b, nil
}

// BlockBuilder implements common.BlockBuilder.
type BlockBuilder struct {
	mu      sync.Mutex
	pending []byte    // first pending transaction
	ready   chan struct{} // closed/sent when a transaction is ready
	notify  chan struct{} // signals waitForPendingBlock
}

// NewBlockBuilder creates a BlockBuilder.
func NewBlockBuilder() *BlockBuilder {
	return &BlockBuilder{
		ready: make(chan struct{}, 1),
		notify: make(chan struct{}, 1),
	}
}

// SubmitTransaction enqueues a transaction, overwriting any previously pending
// transaction that has not yet been built into a committed block.
func (bb *BlockBuilder) SubmitTransaction(data []byte) {
	bb.mu.Lock()
	defer bb.mu.Unlock()
	bb.pending = data
	select {
	case bb.ready <- struct{}{}:
	default:
	}
	select {
	case bb.notify <- struct{}{}:
	default:
	}
}


func (bb *BlockBuilder) WaitForPendingBlock(ctx context.Context) {
	bb.mu.Lock()
	if bb.pending != nil {
		bb.mu.Unlock()
		return
	}
	bb.mu.Unlock()

	select {
	case <-bb.notify:
	case <-ctx.Done():
	}
}

// BuildBlock waits for a pending transaction and constructs a Block.
// Returns (nil, false) if ctx is cancelled before a transaction arrives.
func (bb *BlockBuilder) BuildBlock(ctx context.Context, metadata common.ProtocolMetadata, blacklist common.Blacklist) (common.VerifiedBlock, bool) {
	// Wait until a transaction is available.
	for {
		bb.mu.Lock()
		if bb.pending != nil {
			data := bb.pending
			bb.mu.Unlock()
			return newBlock(data, metadata, blacklist), true
		}
		bb.mu.Unlock()

		select {
		case <-bb.ready:
			// Loop and try to grab the pending transaction.
		case <-ctx.Done():
			return nil, false
		}
	}
}

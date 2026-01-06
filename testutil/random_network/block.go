package random_network

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/asn1"
	"fmt"

	"github.com/ava-labs/simplex"
)

var _ simplex.Block = (*Block)(nil)

type Block struct {
	parent    *Block
	blacklist simplex.Blacklist

	// contents
	txs []*TX

	// protocol metadata
	metadata simplex.ProtocolMetadata
	digest   simplex.Digest

	// mempool access
	mempool *Mempool
}

func NewBlock(metadata simplex.ProtocolMetadata, blacklist simplex.Blacklist, mempool *Mempool, txs []*TX, prev *Block) *Block {
	if prev != nil && metadata.Prev != prev.digest {
		panic("block's prev digest does not match parent's digest")
	}

	b := &Block{
		parent:    prev,
		mempool:   mempool,
		txs:       txs,
		metadata:  metadata,
		blacklist: blacklist,
	}

	b.ComputeAndSetDigest()
	return b
}

func (b *Block) Verify(ctx context.Context) (simplex.VerifiedBlock, error) {
	return b, b.mempool.VerifyBlock(ctx, b)
}

func (b *Block) Blacklist() simplex.Blacklist {
	return b.blacklist
}

func (b *Block) BlockHeader() simplex.BlockHeader {
	return simplex.BlockHeader{
		ProtocolMetadata: b.metadata,
		Digest:           b.digest,
	}
}

type encodedBlock struct {
	ProtocolMetadata []byte
	TXs              []asn1TX
	Blacklist        []byte
}

func (b *Block) Bytes() ([]byte, error) {
	mdBytes := b.metadata.Bytes()

	var asn1TXs []asn1TX
	for _, tx := range b.txs {
		asn1TXs = append(asn1TXs, asn1TX{ID: tx.ID[:]})
	}

	blacklistBytes := b.blacklist.Bytes()

	encodedB := encodedBlock{
		ProtocolMetadata: mdBytes,
		TXs:              asn1TXs,
		Blacklist:        blacklistBytes,
	}

	return asn1.Marshal(encodedB)
}

func (b *Block) containsTX(txID txID) bool {
	for _, tx := range b.txs {
		if tx.ID == txID {
			return true
		}
	}
	return false
}

func (b *Block) ComputeAndSetDigest() {
	var bb bytes.Buffer
	tbBytes, err := b.Bytes()
	if err != nil {
		panic(fmt.Sprintf("failed to serialize test block: %v", err))
	}

	bb.Write(tbBytes)
	b.digest = sha256.Sum256(bb.Bytes())
}

type BlockDeserializer struct{}

var _ simplex.BlockDeserializer = (*BlockDeserializer)(nil)

func (bd *BlockDeserializer) DeserializeBlock(ctx context.Context, buff []byte) (simplex.Block, error) {
	var encodedBlock encodedBlock
	_, err := asn1.Unmarshal(buff, &encodedBlock)
	if err != nil {
		return nil, err
	}

	md, err := simplex.ProtocolMetadataFromBytes(encodedBlock.ProtocolMetadata)
	if err != nil {
		return nil, err
	}

	var blacklist simplex.Blacklist
	if err := blacklist.FromBytes(encodedBlock.Blacklist); err != nil {
		return nil, err
	}

	txs := make([]*TX, len(encodedBlock.TXs))
	for i, asn1Tx := range encodedBlock.TXs {
		tx := asn1Tx.toTX()
		txs[i] = tx
	}

	b := &Block{
		metadata:  *md,
		txs:       txs,
		blacklist: blacklist,
	}

	b.ComputeAndSetDigest()

	return b, nil
}

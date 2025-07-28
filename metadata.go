// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	metadataVersionLen = 1
	metadataEpochLen   = 8
	metadataRoundLen   = 8
	metadataSeqLen     = 8
	metadataPrevLen    = 32
	metadataDigestLen  = 32

	ProtocolMetadataLen = metadataVersionLen + metadataEpochLen + metadataRoundLen + metadataSeqLen + metadataPrevLen
	BlockHeaderLen      = ProtocolMetadataLen + metadataDigestLen
)

const (
	digestFormatSize = 10
)

// ProtocolMetadata encodes information about the protocol state at a given point in time.
type ProtocolMetadata struct {
	// Version defines the version of the protocol this block was created with.
	Version uint8
	// Epoch returns the epoch in which the block was proposed
	Epoch uint64
	// Round returns the round number in which the block was proposed.
	// Can also be an empty block.
	Round uint64
	// Seq is the order of the block among all blocks in the blockchain.
	// Cannot correspond to an empty block.
	Seq uint64
	// Prev returns the digest of the previous data block
	Prev Digest
}

// BlockHeader encodes a succinct and collision-free representation of a block.
// It's included in votes and finalizations in order to convey which block is voted on,
// or which block is finalized.
type BlockHeader struct {
	ProtocolMetadata
	// Digest returns a collision resistant short representation of the block's bytes
	Digest Digest
}

type Digest [metadataDigestLen]byte

func (d Digest) String() string {
	return fmt.Sprintf("%x...", (d)[:digestFormatSize])
}

func (bh BlockHeader) String() string {
	return fmt.Sprintf("BlockHeader{Digest: %s, Prev: %s, Epoch: %d, Round: %d, Seq: %d, Version: %d}",
		bh.Digest.String(), bh.Prev.String(), bh.Epoch, bh.Round, bh.Seq, bh.Version)
}

func (bh *BlockHeader) Equals(other *BlockHeader) bool {
	return bytes.Equal(bh.Digest[:], other.Digest[:]) &&
		bytes.Equal(bh.Prev[:], other.Prev[:]) && bh.Epoch == other.Epoch &&
		bh.Round == other.Round && bh.Seq == other.Seq && bh.Version == other.Version
}

func (bh *BlockHeader) Bytes() []byte {
	buff := make([]byte, BlockHeaderLen)

	mdBytes := bh.ProtocolMetadata.Bytes()
	copy(buff, mdBytes)
	copy(buff[ProtocolMetadataLen:], bh.Digest[:])

	return buff
}

func (bh *BlockHeader) FromBytes(buff []byte) error {
	if len(buff) != BlockHeaderLen {
		return fmt.Errorf("invalid buffer length %d, expected %d", len(buff), BlockHeaderLen)
	}

	md, err := ProtocolMetadataFromBytes(buff[:ProtocolMetadataLen])
	if err != nil {
		return fmt.Errorf("failed to parse ProtocolMetadata: %w", err)
	}
	bh.ProtocolMetadata = *md

	copy(bh.Digest[:], buff[ProtocolMetadataLen:ProtocolMetadataLen+metadataDigestLen])
	return nil
}

// Serializes a ProtocolMetadata from a byte slice.
func ProtocolMetadataFromBytes(buff []byte) (*ProtocolMetadata, error) {
	if len(buff) != ProtocolMetadataLen {
		return nil, fmt.Errorf("invalid buffer length %d, expected %d", len(buff), ProtocolMetadataLen)
	}

	md := &ProtocolMetadata{}
	var pos int
	md.Version = buff[pos]
	pos++
	md.Epoch = binary.BigEndian.Uint64(buff[pos:])
	pos += metadataEpochLen
	md.Round = binary.BigEndian.Uint64(buff[pos:])
	pos += metadataRoundLen
	md.Seq = binary.BigEndian.Uint64(buff[pos:])
	pos += metadataSeqLen
	copy(md.Prev[:], buff[pos:pos+metadataPrevLen])

	return md, nil
}

// Bytes returns a byte encoding of the ProtocolMetadata.
// it is encoded as follows:
// [Version (1 byte), Epoch (8 bytes), Round (8 bytes),
// Seq (8 bytes), Prev (32 bytes)]
func (md *ProtocolMetadata) Bytes() []byte {
	buff := make([]byte, ProtocolMetadataLen)
	var pos int

	buff[pos] = md.Version
	pos++

	binary.BigEndian.PutUint64(buff[pos:], md.Epoch)
	pos += metadataEpochLen

	binary.BigEndian.PutUint64(buff[pos:], md.Round)
	pos += metadataRoundLen

	binary.BigEndian.PutUint64(buff[pos:], md.Seq)
	pos += metadataSeqLen

	copy(buff[pos:], md.Prev[:])

	return buff
}

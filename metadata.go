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

	protocolMetadataLen = metadataVersionLen + metadataEpochLen + metadataRoundLen + metadataSeqLen + metadataPrevLen
	metadataLen         = protocolMetadataLen + metadataDigestLen
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

func (bh *BlockHeader) Equals(other *BlockHeader) bool {
	return bytes.Equal(bh.Digest[:], other.Digest[:]) &&
		bytes.Equal(bh.Prev[:], other.Prev[:]) && bh.Epoch == other.Epoch &&
		bh.Round == other.Round && bh.Seq == other.Seq && bh.Version == other.Version
}

func (bh *BlockHeader) Bytes() []byte {
	buff := make([]byte, metadataLen)
	var pos int

	buff[pos] = bh.Version
	pos++

	copy(buff[pos:], bh.Digest[:])
	pos += metadataDigestLen

	binary.BigEndian.PutUint64(buff[pos:], bh.Epoch)
	pos += metadataEpochLen

	binary.BigEndian.PutUint64(buff[pos:], bh.Round)
	pos += metadataRoundLen

	binary.BigEndian.PutUint64(buff[pos:], bh.Seq)
	pos += metadataSeqLen

	copy(buff[pos:], bh.Prev[:])

	return buff
}

func (bh *BlockHeader) FromBytes(buff []byte) error {
	if len(buff) != metadataLen {
		return fmt.Errorf("invalid buffer length %d, expected %d", len(buff), metadataLen)
	}

	var pos int

	bh.Version = buff[pos]
	pos++

	copy(bh.Digest[:], buff[pos:pos+metadataDigestLen])
	pos += metadataDigestLen

	bh.Epoch = binary.BigEndian.Uint64(buff[pos:])
	pos += metadataEpochLen

	bh.Round = binary.BigEndian.Uint64(buff[pos:])
	pos += metadataRoundLen

	bh.Seq = binary.BigEndian.Uint64(buff[pos:])
	pos += metadataSeqLen

	copy(bh.Prev[:], buff[pos:pos+metadataPrevLen])

	return nil
}

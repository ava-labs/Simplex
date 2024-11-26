// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
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

	metadataLen = metadataVersionLen + metadataDigestLen + metadataEpochLen + metadataRoundLen + metadataSeqLen + metadataPrevLen
)

const (
	digestFormatSize = 10
)

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
	Prev []byte
}

type Metadata struct {
	ProtocolMetadata
	// Digest returns a collision resistant short representation of the block's bytes
	Digest Digest
}

type Digest []byte

func (d Digest) String() string {
	return fmt.Sprintf("%x", []byte(d)[:digestFormatSize])
}

func (m *Metadata) Equals(other *Metadata) bool {
	return bytes.Equal(m.Digest, other.Digest) &&
		bytes.Equal(m.Prev, other.Prev) && m.Epoch == other.Epoch &&
		m.Round == other.Round && m.Seq == other.Seq && m.Version == other.Version
}

func (m *Metadata) Bytes() []byte {
	// Sanity check: check that digest and prev are 32 bytes

	if len(m.Digest) != metadataDigestLen {
		panic(fmt.Sprintf("digest is %d bytes, expected %d", len(m.Digest), metadataDigestLen))
	}

	// Prev block's digest can be nil, or 32 bytes
	if len(m.Prev) != 0 && len(m.Prev) != metadataPrevLen {
		panic(fmt.Sprintf("digest is %d bytes, expected %d", len(m.Prev), metadataPrevLen))
	}

	if len(m.Prev) == 0 {
		m.Prev = make([]byte, metadataPrevLen)
	}

	buff := make([]byte, metadataLen)
	var pos int

	buff[pos] = m.Version
	pos++

	copy(buff[pos:], m.Digest)
	pos += metadataDigestLen

	binary.BigEndian.PutUint64(buff[pos:], m.Epoch)
	pos += metadataEpochLen

	binary.BigEndian.PutUint64(buff[pos:], m.Round)
	pos += metadataRoundLen

	binary.BigEndian.PutUint64(buff[pos:], m.Seq)
	pos += metadataSeqLen

	copy(buff[pos:], m.Prev)

	return buff
}

func (m *Metadata) FromBytes(buff []byte) error {
	if len(buff) != metadataLen {
		return fmt.Errorf("invalid buffer length %d, expected %d", len(buff), metadataLen)
	}

	var pos int

	m.Version = buff[pos]
	pos++

	m.Digest = buff[pos : pos+metadataDigestLen]
	pos += metadataDigestLen

	m.Epoch = binary.BigEndian.Uint64(buff[pos:])
	pos += metadataEpochLen

	m.Round = binary.BigEndian.Uint64(buff[pos:])
	pos += metadataRoundLen

	m.Seq = binary.BigEndian.Uint64(buff[pos:])
	pos += metadataSeqLen

	m.Prev = buff[pos : pos+metadataPrevLen]

	return nil
}

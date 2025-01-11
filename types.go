// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:generate go run github.com/StephenButtolph/canoto/canoto@v0.10.0 --concurrent=false $GOFILE

package simplex

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

const (
	digestLen        = 32
	digestFormatSize = 10
)

type (
	Record struct {
		Block        *BlockRecord  `canoto:"pointer,1,record"`
		Notarization *QuorumRecord `canoto:"pointer,2,record"`
		Finalization *QuorumRecord `canoto:"pointer,3,record"`

		canotoData canotoData_Record
	}
	QuorumRecord struct {
		Header BlockHeader `canoto:"value,1"`
		QC     []byte      `canoto:"bytes,2"`

		canotoData canotoData_QuorumRecord
	}
	BlockRecord struct {
		// TODO: Do we need to include the Digest? Or should we just include the
		// ProtocolMetadata?
		Header  BlockHeader `canoto:"value,1"`
		Payload []byte      `canoto:"bytes,2"`

		canotoData canotoData_BlockRecord
	}

	Message struct {
		Proposal     *BlockMessage            `canoto:"pointer,1,message"`
		Vote         *Vote                    `canoto:"pointer,2,message"`
		Notarization *Notarization            `canoto:"pointer,3,message"`
		Finalize     *Finalization            `canoto:"pointer,4,message"`
		Finalization *FinalizationCertificate `canoto:"pointer,5,message"`

		canotoData canotoData_Message
	}
	BlockMessage struct {
		Block Block // `canoto:"bytes,1"`
		Vote  Vote  `canoto:"value,2"`

		canotoData canotoData_BlockMessage
	}
	Vote struct {
		Vote      ToBeSignedVote `canoto:"value,1"`
		Signature Signature      `canoto:"value,2"`

		canotoData canotoData_Vote
	}
	Notarization struct {
		Vote ToBeSignedVote    `canoto:"value,1"`
		QC   QuorumCertificate // `canoto:"bytes,2"`

		canotoData canotoData_Notarization
	}
	Finalization struct {
		Finalization ToBeSignedFinalization `canoto:"value,1"`
		Signature    Signature              `canoto:"value,2"`

		canotoData canotoData_Finalization
	}
	FinalizationCertificate struct {
		Finalization ToBeSignedFinalization `canoto:"value,1"`
		QC           QuorumCertificate      // `canoto:"bytes,2"`

		canotoData canotoData_FinalizationCertificate
	}

	SignedMessage struct {
		Payload []byte `canoto:"bytes,1"`
		Context string `canoto:"string,2"`

		canotoData canotoData_SignedMessage
	}

	// Signature encodes a signature and the node that signed it, without the
	// message it was signed on.
	Signature struct {
		// Signer is the NodeID of the creator of the signature.
		Signer NodeID `canoto:"bytes,1"`
		// Value is the byte representation of the signature.
		Value []byte `canoto:"bytes,2"`

		canotoData canotoData_Signature
	}

	// BlockHeader encodes a succinct and collision-free representation of a block.
	// It's included in votes and finalizations in order to convey which block is voted on,
	// or which block is finalized.
	BlockHeader struct {
		ProtocolMetadata `canoto:"value,1"`
		// Digest returns a collision resistant short representation of the block's bytes
		Digest Digest `canoto:"fixed bytes,2"`

		canotoData canotoData_BlockHeader
	}

	// ProtocolMetadata encodes information about the protocol state at a given
	// point in time.
	ProtocolMetadata struct {
		// Epoch returns the epoch in which the block was proposed
		Epoch uint64 `canoto:"int,1"`
		// Round returns the round number in which the block was proposed.
		// Can also be an empty block.
		Round uint64 `canoto:"int,2"`
		// Seq is the order of the block among all blocks in the blockchain.
		// Cannot correspond to an empty block.
		Seq uint64 `canoto:"int,3"`
		// Prev returns the digest of the previous data block
		Prev Digest `canoto:"fixed bytes,4"`

		canotoData canotoData_ProtocolMetadata
	}

	Digest [digestLen]byte
	NodeID []byte
)

func (b *BlockHeader) Equals(o *BlockHeader) bool {
	return b.Digest == o.Digest && b.ProtocolMetadata.Equals(&o.ProtocolMetadata)
}

func (p *ProtocolMetadata) Equals(o *ProtocolMetadata) bool {
	return p.Epoch == o.Epoch && p.Round == o.Round && p.Seq == o.Seq && p.Prev == o.Prev
}

func (d Digest) String() string {
	return fmt.Sprintf("%x...", (d)[:digestFormatSize])
}

func (n NodeID) String() string {
	return hex.EncodeToString(n)
}

func (n NodeID) Equals(o NodeID) bool {
	return bytes.Equal(n, o)
}

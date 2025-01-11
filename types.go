// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:generate go run github.com/StephenButtolph/canoto/canoto@v0.10.0 --library=./internal --concurrent=false --import=simplex/internal/canoto $GOFILE

package simplex

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

const (
	digestLen        = 32
	digestFormatSize = 10

	VoteContext     = "vote"
	FinalizeContext = "finalize"
)

type (
	Record struct {
		Block        []byte  `canoto:"bytes,1,record"`
		Notarization *Quorum `canoto:"pointer,2,record"`
		Finalization *Quorum `canoto:"pointer,3,record"`

		canotoData canotoData_Record
	}

	Message struct {
		Proposal     *Proposal `canoto:"pointer,1,message"`
		Vote         *Vote     `canoto:"pointer,2,message"`
		Notarization *Quorum   `canoto:"pointer,3,message"`
		Finalize     *Vote     `canoto:"pointer,4,message"`
		Finalization *Quorum   `canoto:"pointer,5,message"`

		canotoData canotoData_Message
	}
	Proposal struct {
		Block []byte `canoto:"bytes,1"`
		Vote  Vote   `canoto:"value,2"`

		canotoData canotoData_Proposal
	}
	Vote struct {
		Header    BlockHeader `canoto:"value,1"`
		Signature Signature   `canoto:"value,2"`

		canotoData canotoData_Vote
	}
	Quorum struct {
		Header BlockHeader `canoto:"value,1"`
		QC     []byte      `canoto:"bytes,2"`

		canotoData canotoData_Quorum
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

func (v *Vote) Sign(nodeID NodeID, signer Signer, context string) error {
	signature, err := signContext(
		signer,
		v.Header.MarshalCanoto(),
		context,
	)
	if err != nil {
		return err
	}

	v.Signature = Signature{
		Signer: nodeID,
		Value:  signature,
	}
	return nil
}

func (v *Vote) Verify(verifier SignatureVerifier, context string) error {
	return verifyContext(
		v.Signature.Value,
		verifier,
		v.Header.MarshalCanoto(),
		context,
		v.Signature.Signer,
	)
}

func (q *Quorum) Verify(p QCDeserializer, context string) error {
	qc, err := p.DeserializeQuorumCertificate(q.QC)
	if err != nil {
		return err
	}
	return verifyContextQC(qc, q.Header.MarshalCanoto(), context)
}

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

func signContext(signer Signer, msg []byte, context string) ([]byte, error) {
	sm := SignedMessage{
		Payload: msg,
		Context: context,
	}
	toBeSigned := sm.MarshalCanoto()
	return signer.Sign(toBeSigned)
}

func verifyContext(signature []byte, verifier SignatureVerifier, msg []byte, context string, signer NodeID) error {
	sm := SignedMessage{
		Payload: msg,
		Context: context,
	}
	toBeSigned := sm.MarshalCanoto()
	return verifier.Verify(toBeSigned, signature, signer)
}

func verifyContextQC(qc QuorumCertificate, msg []byte, context string) error {
	sm := SignedMessage{
		Payload: msg,
		Context: context,
	}
	toBeSigned := sm.MarshalCanoto()
	return qc.Verify(toBeSigned)
}

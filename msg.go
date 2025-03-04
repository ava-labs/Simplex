// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"encoding/asn1"
	"encoding/binary"
	"fmt"
)

type Message struct {
	BlockMessage            *BlockMessage
	EmptyNotarization       *EmptyNotarization
	VoteMessage             *Vote
	EmptyVoteMessage        *EmptyVote
	Notarization            *Notarization
	Finalization            *Finalization
	FinalizationCertificate *FinalizationCertificate
	ReplicationResponse     *ReplicationResponse
	ReplicationRequest      *ReplicationRequest
}

type ToBeSignedEmptyVote struct {
	ProtocolMetadata
}

func (v *ToBeSignedEmptyVote) Bytes() []byte {
	buff := make([]byte, protocolMetadataLen)
	var pos int

	buff[pos] = v.Version
	pos++

	binary.BigEndian.PutUint64(buff[pos:], v.Epoch)
	pos += metadataEpochLen

	binary.BigEndian.PutUint64(buff[pos:], v.Round)
	pos += metadataRoundLen

	binary.BigEndian.PutUint64(buff[pos:], v.Seq)
	pos += metadataSeqLen

	copy(buff[pos:], v.Prev[:])

	return buff
}

func (v *ToBeSignedEmptyVote) FromBytes(buff []byte) error {
	if len(buff) != protocolMetadataLen {
		return fmt.Errorf("invalid buffer length %d, expected %d", len(buff), metadataLen)
	}

	var pos int

	v.Version = buff[pos]
	pos++

	v.Epoch = binary.BigEndian.Uint64(buff[pos:])
	pos += metadataEpochLen

	v.Round = binary.BigEndian.Uint64(buff[pos:])
	pos += metadataRoundLen

	v.Seq = binary.BigEndian.Uint64(buff[pos:])
	pos += metadataSeqLen

	copy(v.Prev[:], buff[pos:pos+metadataPrevLen])

	return nil
}

func (v *ToBeSignedEmptyVote) Sign(signer Signer) ([]byte, error) {
	context := "ToBeSignedEmptyVote"
	msg := v.Bytes()

	return signContext(signer, msg, context)
}

func (v *ToBeSignedEmptyVote) Verify(signature []byte, verifier SignatureVerifier, signers NodeID) error {
	context := "ToBeSignedEmptyVote"
	msg := v.Bytes()

	return verifyContext(signature, verifier, msg, context, signers)
}

type ToBeSignedVote struct {
	BlockHeader
}

func (v *ToBeSignedVote) Sign(signer Signer) ([]byte, error) {
	context := "ToBeSignedVote"
	msg := v.Bytes()

	return signContext(signer, msg, context)
}

func (v *ToBeSignedVote) Verify(signature []byte, verifier SignatureVerifier, signers NodeID) error {
	context := "ToBeSignedVote"
	msg := v.Bytes()

	return verifyContext(signature, verifier, msg, context, signers)
}

type ToBeSignedFinalization struct {
	BlockHeader
}

func (f *ToBeSignedFinalization) Sign(signer Signer) ([]byte, error) {
	context := "ToBeSignedFinalization"
	msg := f.Bytes()

	return signContext(signer, msg, context)
}

func (f *ToBeSignedFinalization) Verify(signature []byte, verifier SignatureVerifier, signers NodeID) error {
	context := "ToBeSignedFinalization"
	msg := f.Bytes()

	return verifyContext(signature, verifier, msg, context, signers)
}

func signContext(signer Signer, msg []byte, context string) ([]byte, error) {
	sm := SignedMessage{Payload: msg, Context: context}
	toBeSigned, err := asn1.Marshal(sm)
	if err != nil {
		return nil, err
	}
	return signer.Sign(toBeSigned)
}

func verifyContext(signature []byte, verifier SignatureVerifier, msg []byte, context string, signers NodeID) error {
	sm := SignedMessage{Payload: msg, Context: context}
	toBeSigned, err := asn1.Marshal(sm)
	if err != nil {
		return err
	}
	return verifier.Verify(toBeSigned, signature, signers)
}

func verifyContextQC(qc QuorumCertificate, msg []byte, context string) error {
	sm := SignedMessage{Payload: msg, Context: context}
	toBeSigned, err := asn1.Marshal(sm)
	if err != nil {
		return err
	}

	return qc.Verify(toBeSigned)
}

type Vote struct {
	Vote      ToBeSignedVote
	Signature Signature
}

type EmptyVote struct {
	Vote      ToBeSignedEmptyVote
	Signature Signature
}

type Finalization struct {
	Finalization ToBeSignedFinalization
	Signature    Signature
}

type FinalizationCertificate struct {
	Finalization ToBeSignedFinalization
	QC           QuorumCertificate
}

func (fc *FinalizationCertificate) Verify() error {
	context := "ToBeSignedFinalization"
	return verifyContextQC(fc.QC, fc.Finalization.Bytes(), context)
}

type Notarization struct {
	Vote ToBeSignedVote
	QC   QuorumCertificate
}

func (n *Notarization) Verify() error {
	context := "ToBeSignedVote"
	return verifyContextQC(n.QC, n.Vote.Bytes(), context)
}

type BlockMessage struct {
	Block         Block
	VerifiedBlock VerifiedBlock
	Vote          Vote
}

type EmptyNotarization struct {
	Vote ToBeSignedEmptyVote
	QC   QuorumCertificate
}

func (en *EmptyNotarization) Verify() error {
	context := "ToBeSignedEmptyVote"
	return verifyContextQC(en.QC, en.Vote.Bytes(), context)
}

type SignedMessage struct {
	Payload []byte
	Context string
}

// QuorumCertificate is equivalent to a collection of signatures from a quorum of nodes,
type QuorumCertificate interface {
	// Signers returns who participated in creating this QuorumCertificate.
	Signers() []NodeID
	// Verify checks whether the nodes participated in creating this QuorumCertificate,
	// signed the given message.
	Verify(msg []byte) error
	// Bytes returns a raw representation of the given QuorumCertificate.
	Bytes() []byte
}

type ReplicationRequest struct {
	FinalizationCertificateRequest *FinalizationCertificateRequest
	NotarizationRequest            *NotarizationRequest
}

type ReplicationResponse struct {
	FinalizationCertificateResponse *FinalizationCertificateResponse
	NotarizationResponse            *NotarizationResponse
}

// request a finalization certificate for the given sequence number
type FinalizationCertificateRequest struct {
	Sequences []uint64
}

type FinalizedBlock struct {
	Block         Block
	VerifiedBlock VerifiedBlock
	FCert         FinalizationCertificate
}

type FinalizationCertificateResponse struct {
	Data []FinalizedBlock
}

type NotarizationRequest struct {
	// the starting round to request notarizations
	StartRound uint64
}

type NotarizationResponse struct {
	Data []NotarizedBlock
}

// NotarizedBlock represents a block that has a notarization.
// Either an EmptyNotarization must be present or a Block and Notarization.
type NotarizedBlock struct {
	Block             Block
	VerifiedBlock 	  VerifiedBlock
	Notarization      *Notarization
	EmptyNotarization *EmptyNotarization
}

// GetRound gets the round of the notarized block, which will either be
// found in the empty notarization or the block.
func (n NotarizedBlock) GetRound() uint64 {
	if n.EmptyNotarization != nil {
		return n.EmptyNotarization.Vote.Round
	}

	return n.Block.BlockHeader().Round
}

func (n NotarizedBlock) Verify() error {
	if n.EmptyNotarization != nil {
		return n.EmptyNotarization.Verify()
	}

	return n.Notarization.Verify()
}

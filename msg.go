// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import "encoding/asn1"

type Message struct {
	BlockMessage            *BlockMessage
	VoteMessage             *SignedVoteMessage
	Notarization            *Notarization
	Finalization            *SignedFinalizationMessage
	FinalizationCertificate *FinalizationCertificate
}

type Vote struct {
	BlockHeader
}

func (v *Vote) Sign(signer Signer) ([]byte, error) {
	context := "Vote"
	msg := v.Bytes()

	return signContext(signer, msg, context)
}

func (v *Vote) Verify(signature []byte, verifier SignatureVerifier, signers NodeID) error {
	context := "Vote"
	msg := v.Bytes()

	return verifyContext(signature, verifier, msg, context, signers)
}

type Finalization struct {
	BlockHeader
}

func (f *Finalization) Sign(signer Signer) ([]byte, error) {
	context := "Finalization"
	msg := f.Bytes()

	return signContext(signer, msg, context)
}

func (f *Finalization) Verify(signature []byte, verifier SignatureVerifier, signers NodeID) error {
	context := "Finalization"
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

type SignedVoteMessage struct {
	Vote      Vote
	Signature Signature
}

type SignedFinalizationMessage struct {
	Finalization Finalization
	Signature    Signature
}

type FinalizationCertificate struct {
	Finalization Finalization
	QC           QuorumCertificate
}

func (fc *FinalizationCertificate) Verify() error {
	context := "Finalization"
	return verifyContextQC(fc.QC, fc.Finalization.Bytes(), context)
}

type Notarization struct {
	Vote Vote
	QC   QuorumCertificate
}

func (n *Notarization) Verify() error {
	context := "Vote"
	return verifyContextQC(n.QC, n.Vote.Bytes(), context)
}

type BlockMessage struct {
	Block Block
	Vote  SignedVoteMessage
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

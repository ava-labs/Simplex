// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import "encoding/asn1"

type Message[B Block] struct {
	BlockMessage            *BlockMessage[B]
	VoteMessage             *SignedVoteMessage
	Notarization            *Notarization
	Finalization            *SignedFinalizationMessage
	FinalizationCertificate *FinalizationCertificate
}

type Vote struct {
	Metadata
}

func (v *Vote) Sign(signer Signer) ([]byte, error) {
	context := "Vote"
	msg := v.Bytes()

	return signContext(signer, msg, context)
}

func (v *Vote) Verify(signature []byte, verifier SignatureVerifier, signers ...NodeID) error {
	context := "Vote"
	msg := v.Bytes()

	return verifyContext(signature, verifier, msg, context, signers...)
}

type Finalization struct {
	Metadata
}

func (f *Finalization) Sign(signer Signer) ([]byte, error) {
	context := "Finalization"
	msg := f.Bytes()

	return signContext(signer, msg, context)
}

func (f *Finalization) Verify(signature []byte, verifier SignatureVerifier, signers ...NodeID) error {
	context := "Finalization"
	msg := f.Bytes()

	return verifyContext(signature, verifier, msg, context, signers...)
}

func signContext(signer Signer, msg []byte, context string) ([]byte, error) {
	sm := SignedMessage{Payload: msg, Context: context}
	toBeSigned, err := asn1.Marshal(sm)
	if err != nil {
		return nil, err
	}
	return signer.Sign(toBeSigned)
}

func verifyContext(signature []byte, verifier SignatureVerifier, msg []byte, context string, signers ...NodeID) error {
	sm := SignedMessage{Payload: msg, Context: context}
	toBeSigned, err := asn1.Marshal(sm)
	if err != nil {
		return err
	}
	return verifier.Verify(toBeSigned, signature, signers...)
}

type SignedVoteMessage struct {
	Vote      Vote
	Signer    NodeID
	Signature []byte
}

type SignatureSignerPair struct {
	Signer    NodeID
	Signature []byte
}

type SignedFinalizationMessage struct {
	Finalization Finalization
	Signer       NodeID
	Signature    []byte
}

type FinalizationCertificate struct {
	Finalization         Finalization
	AggregatedSignedVote *AggregatedSignedVote
	SignaturesAndSigners []*SignatureSignerPair
}

type Notarization struct {
	Vote                 Vote
	AggregatedSignedVote *AggregatedSignedVote
	SignaturesAndSigners []*SignatureSignerPair
}

type AggregatedSignedVote struct {
	Signers   []NodeID
	Signature []byte
}

type BlockMessage[B Block] struct {
	Block B
}

type SignedMessage struct {
	Payload []byte
	Context string
}

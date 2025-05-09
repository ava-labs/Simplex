// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"encoding/asn1"
	"encoding/binary"
	"fmt"
)

type Message struct {
	BlockMessage                *BlockMessage
	VerifiedBlockMessage        *VerifiedBlockMessage
	EmptyNotarization           *EmptyNotarization
	VoteMessage                 *Vote
	EmptyVoteMessage            *EmptyVote
	Notarization                *Notarization
	Finalization                *Finalization
	FinalizationCertificate     *FinalizationCertificate
	ReplicationResponse         *ReplicationResponse
	VerifiedReplicationResponse *VerifiedReplicationResponse
	ReplicationRequest          *ReplicationRequest
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
	Block Block
	Vote  Vote
}

type VerifiedBlockMessage struct {
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
	Seqs        []uint64 // sequences we are requesting
	LatestRound uint64   // latest round that we are aware of
}

type ReplicationResponse struct {
	Data        []QuorumRound
	LatestRound *QuorumRound
}

type VerifiedReplicationResponse struct {
	Data        []VerifiedQuorumRound
	LatestRound *VerifiedQuorumRound
}

// QuorumRound represents a round that has acheived quorum on either
// (empty notarization), (block & notarization), or (block, finalization certificate)
type QuorumRound struct {
	Block             Block
	Notarization      *Notarization
	FCert             *FinalizationCertificate
	EmptyNotarization *EmptyNotarization
}

// isWellFormed returns an error if the QuorumRound has either
// (block, notarization) or (block, finalization certificate) or
// (empty notarization)
func (q *QuorumRound) IsWellFormed() error {
	if q.EmptyNotarization != nil && q.Block == nil {
		return nil
	} else if q.Block != nil && (q.Notarization != nil || q.FCert != nil) {
		return nil
	}

	return fmt.Errorf("malformed QuorumRound")
}

func (q *QuorumRound) GetRound() uint64 {
	if q.EmptyNotarization != nil {
		return q.EmptyNotarization.Vote.Round
	}

	if q.Block != nil {
		return q.Block.BlockHeader().Round
	}

	return 0
}

func (q *QuorumRound) GetSequence() uint64 {
	if q.EmptyNotarization != nil {
		return q.EmptyNotarization.Vote.Seq
	}

	if q.Block != nil {
		return q.Block.BlockHeader().Seq
	}

	return 0
}

func (q *QuorumRound) Verify() error {
	if err := q.IsWellFormed(); err != nil {
		return err
	}

	if q.EmptyNotarization != nil {
		return q.EmptyNotarization.Verify()
	}

	// ensure the finalization certificate or notarization we get relates to the block
	blockDigest := q.Block.BlockHeader().Digest

	if q.FCert != nil {
		if !bytes.Equal(blockDigest[:], q.FCert.Finalization.Digest[:]) {
			return fmt.Errorf("finalization certificate does not match the block")
		}
		err := q.FCert.Verify()
		if err != nil {
			return err
		}
	}

	if q.Notarization != nil {
		if !bytes.Equal(blockDigest[:], q.Notarization.Vote.Digest[:]) {
			return fmt.Errorf("notarization does not match the block")
		}
		return q.Notarization.Verify()
	}

	return nil
}

// String returns a string representation of the QuorumRound.
// It is meant as a debugging aid for logs.
func (q *QuorumRound) String() string {
	if q != nil {
		err := q.IsWellFormed()
		if err != nil {
			return fmt.Sprintf("QuorumRound{Error: %s}", err)
		} else {
			return fmt.Sprintf("QuorumRound{Round: %d, Seq: %d}", q.GetRound(), q.GetSequence())
		}
	}

	return "QuorumRound{nil}"
}

type VerifiedQuorumRound struct {
	VerifiedBlock     VerifiedBlock
	Notarization      *Notarization
	FCert             *FinalizationCertificate
	EmptyNotarization *EmptyNotarization
}

func (q *VerifiedQuorumRound) GetRound() uint64 {
	if q.EmptyNotarization != nil {
		return q.EmptyNotarization.Vote.Round
	}

	if q.VerifiedBlock != nil {
		return q.VerifiedBlock.BlockHeader().Round
	}

	return 0
}

type VerifiedFinalizedBlock struct {
	VerifiedBlock VerifiedBlock
	FCert         FinalizationCertificate
}

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
	FinalizeVote                *FinalizeVote
	Finalization                *Finalization
	ReplicationResponse         *ReplicationResponse
	VerifiedReplicationResponse *VerifiedReplicationResponse
	ReplicationRequest          *ReplicationRequest
}

type EmptyVoteMetadata struct {
	Round uint64
	Epoch uint64
}

type ToBeSignedEmptyVote struct {
	EmptyVoteMetadata
}

func (v *ToBeSignedEmptyVote) Bytes() []byte {
	bytes := make([]byte, 8+8) // Round + Epoch
	binary.BigEndian.PutUint64(bytes[0:8], v.EmptyVoteMetadata.Round)
	binary.BigEndian.PutUint64(bytes[8:16], v.EmptyVoteMetadata.Epoch)
	return bytes
}

func (v *ToBeSignedEmptyVote) FromBytes(buff []byte) error {
	if len(buff) != 16 {
		return fmt.Errorf("invalid buffer length")
	}

	round := binary.BigEndian.Uint64(buff[0:8])
	epoch := binary.BigEndian.Uint64(buff[8:16])

	v.EmptyVoteMetadata = EmptyVoteMetadata{
		Round: round,
		Epoch: epoch,
	}
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

// Vote represents a signed vote for a block.
type Vote struct {
	Vote      ToBeSignedVote
	Signature Signature
}

// EmptyVote represents a signed vote for an empty block.
type EmptyVote struct {
	Vote      ToBeSignedEmptyVote
	Signature Signature
}

// FinalizeVote represents a vote to finalize a block.
type FinalizeVote struct {
	Finalization ToBeSignedFinalization
	Signature    Signature
}

// Finalization represents a block that has reached quorum on block. This
// means that block can be included in the chain and finalized.
type Finalization struct {
	Finalization ToBeSignedFinalization
	QC           QuorumCertificate
}

func (f *Finalization) Verify() error {
	context := "ToBeSignedFinalization"
	return verifyContextQC(f.QC, f.Finalization.Bytes(), context)
}

// Notarization represents a block that has reached a quorum of votes.
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
	Seqs               []uint64 // sequences we are requesting
	Rounds             []uint64 // rounds we are requesting
	LatestRound        uint64   // latest round that we are aware of
	LatestFinalizedSeq uint64   // latest finalized sequence that we are aware of
}

type ReplicationResponse struct {
	Data        []QuorumRound
	LatestRound *QuorumRound
	LatestSeq   *QuorumRound
}

type VerifiedReplicationResponse struct {
	Data               []VerifiedQuorumRound
	LatestRound        *VerifiedQuorumRound
	LatestFinalizedSeq *VerifiedQuorumRound
}

// QuorumRound represents a round that has achieved quorum on either
// (empty notarization), (block & notarization), or (block, finalization)
type QuorumRound struct {
	Block             Block
	Notarization      *Notarization
	Finalization      *Finalization
	EmptyNotarization *EmptyNotarization
}

// isWellFormed returns an error if the QuorumRound has either
// (block, notarization) or (block, finalization) or
// (empty notarization)
func (q *QuorumRound) IsWellFormed() error {
	if q.Block == nil && q.EmptyNotarization == nil {
		return fmt.Errorf("malformed QuorumRound, empty block and notarization fields")
	}

	if q.Block != nil && (q.Notarization == nil && q.Finalization == nil) {
		return fmt.Errorf("malformed QuorumRound")
	}

	return nil
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
	if q.Block != nil {
		return q.Block.BlockHeader().Seq
	}

	return 0
}

func (q *QuorumRound) VerifyQCConsistentWithBlock() error {
	if err := q.IsWellFormed(); err != nil {
		return err
	}

	if q.Block == nil {
		return nil
	}

	// if an empty notarization is included, ensure the round is equal to the block round
	if q.EmptyNotarization != nil && q.EmptyNotarization.Vote.Round != q.Block.BlockHeader().Round {
		return fmt.Errorf("empty round does not match block round")
	}

	// ensure the finalization or notarization we get relates to the block
	blockDigest := q.Block.BlockHeader().Digest

	if q.Finalization != nil {
		if !bytes.Equal(blockDigest[:], q.Finalization.Finalization.Digest[:]) {
			return fmt.Errorf("finalization does not match the block")
		}
	}

	if q.Notarization != nil {
		if !bytes.Equal(blockDigest[:], q.Notarization.Vote.Digest[:]) {
			return fmt.Errorf("notarization does not match the block")
		}
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
			return fmt.Sprintf("QuorumRound{Round: %d, Seq: %d, Finalized: %t}", q.GetRound(), q.GetSequence(), q.Finalization != nil)
		}
	}

	return "QuorumRound{nil}"
}

type VerifiedQuorumRound struct {
	VerifiedBlock     VerifiedBlock
	Notarization      *Notarization
	Finalization      *Finalization
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
	Finalization  Finalization
}

type verifiableMessage interface {
	Verify() error
}

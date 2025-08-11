// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

//go:generate go run github.com/StephenButtolph/canoto/canoto $GOFILE

import (
	"bytes"
	"encoding/asn1"
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

type canotoEmptyVoteMetadata struct {
	Round uint64 `canoto:"uint,4"`
	Epoch uint64 `canoto:"uint,2"`

	canotoData canotoData_canotoEmptyVoteMetadata
}

type ToBeSignedEmptyVote struct {
	EmptyVoteMetadata
}

func (v *ToBeSignedEmptyVote) Bytes() []byte {
	canotoEV := canotoEmptyVoteMetadata{
		Round: v.EmptyVoteMetadata.Round,
		Epoch: v.EmptyVoteMetadata.Epoch,
	}
	return canotoEV.MarshalCanoto()
}

func (v *ToBeSignedEmptyVote) FromBytes(buff []byte) error {
	var emptyVoteMetadata canotoEmptyVoteMetadata
	if err := emptyVoteMetadata.UnmarshalCanoto(buff); err != nil {
		return fmt.Errorf("failed to unmarshal ToBeSignedEmptyVote: %w", err)
	}

	v.EmptyVoteMetadata = EmptyVoteMetadata{
		Round: emptyVoteMetadata.Round,
		Epoch: emptyVoteMetadata.Epoch,
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
	if q.EmptyNotarization != nil && q.Block == nil {
		return nil
	} else if q.Block != nil && (q.Notarization != nil || q.Finalization != nil) {
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

	// ensure the finalization or notarization we get relates to the block
	blockDigest := q.Block.BlockHeader().Digest

	if q.Finalization != nil {
		if !bytes.Equal(blockDigest[:], q.Finalization.Finalization.Digest[:]) {
			return fmt.Errorf("finalization does not match the block")
		}
		err := q.Finalization.Verify()
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

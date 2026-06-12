// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package common

import (
	"bytes"
	"encoding/asn1"
	"encoding/binary"
	"fmt"
)

type Message struct {
	BlockMessage                *BlockMessage
	VerifiedBlockMessage        *VerifiedBlockMessage
	EmptyNotarization           *RawEmptyNotarization
	VoteMessage                 *Vote
	EmptyVoteMessage            *EmptyVote
	Notarization                *RawNotarization
	FinalizeVote                *FinalizeVote
	Finalization                *RawFinalization
	ReplicationResponse         *ReplicationResponse
	VerifiedReplicationResponse *VerifiedReplicationResponse
	ReplicationRequest          *ReplicationRequest
	BlockDigestRequest          *BlockDigestRequest
}

type EmptyVoteMetadata struct {
	Round uint64
	Epoch uint64
}

type ToBeSignedEmptyVote struct {
	EmptyVoteMetadata
}

func (v *ToBeSignedEmptyVote) Bytes() []byte {
	bytes := make([]byte, 1+8+8) // Version + Epoch + Round
	binary.BigEndian.PutUint64(bytes[1:9], v.EmptyVoteMetadata.Epoch)
	binary.BigEndian.PutUint64(bytes[9:17], v.EmptyVoteMetadata.Round)
	return bytes
}

func (v *ToBeSignedEmptyVote) FromBytes(buff []byte) error {
	if len(buff) != 17 {
		return fmt.Errorf("invalid buffer length, expected 17, got %d", len(buff))
	}

	epoch := binary.BigEndian.Uint64(buff[1:9])
	round := binary.BigEndian.Uint64(buff[9:17])

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

func (v *Vote) Signer() NodeID {
	return v.Signature.Signer
}

// EmptyVote represents a signed vote for an empty block.
type EmptyVote struct {
	Vote      ToBeSignedEmptyVote
	Signature Signature
}

func (v *EmptyVote) Signer() NodeID {
	return v.Signature.Signer
}

// FinalizeVote represents a vote to finalize a block.
type FinalizeVote struct {
	Finalization ToBeSignedFinalization
	Signature    Signature
}

func (v *FinalizeVote) Signer() NodeID {
	return v.Signature.Signer
}

// RawFinalization represents a finalization that contains a QuorumCertificate that has not been parsed yet.
// It is used to represent finalizations that are received from the network and have not been verified yet.
type RawFinalization struct {
	Finalization ToBeSignedFinalization
	QC           RawQuorumCertificate
}

// Finalization represents a block that has reached quorum on block. This
// means that block can be included in the chain and finalized.
type Finalization struct {
	Finalization ToBeSignedFinalization
	QC           QuorumCertificate
}

// Raw returns a RawFinalization with the same content but an unparsed QC.
func (f *Finalization) Raw() *RawFinalization {
	return &RawFinalization{
		Finalization: f.Finalization,
		QC:           f.QC,
	}
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

// Raw returns a RawNotarization with the same content but an unparsed QC.
func (n *Notarization) Raw() *RawNotarization {
	return &RawNotarization{
		Vote: n.Vote,
		QC:   n.QC,
	}
}

// RawNotarization is a notarization that contains a QuorumCertificate that has not been parsed yet.
// It is used to represent notarizations that are received from the network and have not been verified yet.
type RawNotarization struct {
	Vote ToBeSignedVote
	QC   RawQuorumCertificate
}

type BlockMessage struct {
	Block Block
	Vote  Vote
}

type VerifiedBlockMessage struct {
	VerifiedBlock VerifiedBlock
	Vote          Vote
}

// RawEmptyNotarization is an empty notarization that contains a QuorumCertificate that has not been parsed yet.
// It is used to represent notarizations that are received from the network and have not been verified yet.
type RawEmptyNotarization struct {
	Vote ToBeSignedEmptyVote
	QC   RawQuorumCertificate
}

type EmptyNotarization struct {
	Vote ToBeSignedEmptyVote
	QC   QuorumCertificate
}

// Raw returns a RawEmptyNotarization with the same content but an unparsed QC.
func (en *EmptyNotarization) Raw() *RawEmptyNotarization {
	return &RawEmptyNotarization{
		Vote: en.Vote,
		QC:   en.QC,
	}
}

func (en *EmptyNotarization) Verify() error {
	context := "ToBeSignedEmptyVote"
	return verifyContextQC(en.QC, en.Vote.Bytes(), context)
}

type SignedMessage struct {
	Payload []byte
	Context string
}

// RawQuorumCertificate is an unparsed QuorumCertificate: it exposes its signers
// and raw bytes but cannot verify a message until it has been parsed into a
// QuorumCertificate via a QCDeserializer.
type RawQuorumCertificate interface {
	Signers() []NodeID
	Bytes() []byte
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
	Data        []RawQuorumRound
	LatestRound *RawQuorumRound
	LatestSeq   *RawQuorumRound
}

type VerifiedReplicationResponse struct {
	Data               []VerifiedQuorumRound
	LatestRound        *VerifiedQuorumRound
	LatestFinalizedSeq *VerifiedQuorumRound
}

// RawQuorumRound is the same as QuorumRound but with RawNotarization and RawFinalization instead of Notarization and Finalization.
// It is used to represent QuorumRounds that are received from the network and have not been verified yet.
type RawQuorumRound struct {
	Block             Block
	Notarization      *RawNotarization
	Finalization      *RawFinalization
	EmptyNotarization *RawEmptyNotarization
}

// ToQuorumRound parses the raw QuorumCertificates contained in the round using
// the given QCDeserializer and returns the resulting QuorumRound. It returns an
// error if any of the QCs fail to parse.
func (rqr *RawQuorumRound) ToQuorumRound(qd QCDeserializer) (*QuorumRound, error) {
	var notarization *Notarization
	var finalization *Finalization
	var emptyNotarization *EmptyNotarization
	var err error

	if rqr.Notarization != nil {
		notarization = &Notarization{
			Vote: rqr.Notarization.Vote,
		}
		notarization.QC, err = qd.ParseQuorumCertificate(rqr.Notarization.QC)
		if err != nil {
			return nil, fmt.Errorf("failed to parse notarization QC: %w", err)
		}
	}

	if rqr.Finalization != nil {
		finalization = &Finalization{
			Finalization: rqr.Finalization.Finalization,
		}
		finalization.QC, err = qd.ParseQuorumCertificate(rqr.Finalization.QC)
		if err != nil {
			return nil, fmt.Errorf("failed to parse finalization QC: %w", err)
		}
	}

	if rqr.EmptyNotarization != nil {
		emptyNotarization = &EmptyNotarization{
			Vote: rqr.EmptyNotarization.Vote,
		}
		emptyNotarization.QC, err = qd.ParseQuorumCertificate(rqr.EmptyNotarization.QC)
		if err != nil {
			return nil, fmt.Errorf("failed to parse empty notarization QC: %w", err)
		}
	}

	return &QuorumRound{
		Block:             rqr.Block,
		Notarization:      notarization,
		Finalization:      finalization,
		EmptyNotarization: emptyNotarization,
	}, nil
}

// GetRound returns the round of the RawQuorumRound, or 0 if it cannot be determined.
func (rqr *RawQuorumRound) GetRound() uint64 {
	if rqr.EmptyNotarization != nil {
		return rqr.EmptyNotarization.Vote.Round
	}

	if rqr.Block != nil {
		return rqr.Block.BlockHeader().Round
	}

	return 0
}

// GetSequence returns the sequence of the RawQuorumRound's block, or 0 if it has no block.
func (rqr *RawQuorumRound) GetSequence() uint64 {
	if rqr.Block != nil {
		return rqr.Block.BlockHeader().Seq
	}

	return 0
}

// String returns a string representation of the QuorumRound.
// It is meant as a debugging aid for logs.
func (rqr *RawQuorumRound) String() string {
	if rqr != nil {
		err := rqr.IsWellFormed()
		if err != nil {
			return fmt.Sprintf("QuorumRound{Error: %s}", err)
		} else {
			return fmt.Sprintf("QuorumRound{Round: %d, Seq: %d, Finalized: %t}", rqr.GetRound(), rqr.GetSequence(), rqr.Finalization != nil)
		}
	}

	return "QuorumRound{nil}"
}

// IsWellFormed returns an error if the RawQuorumRound is not in one of these
// formats: (block, notarization), (block, finalization), or (empty notarization).
func (rqr *RawQuorumRound) IsWellFormed() error {
	if rqr.Block == nil && rqr.EmptyNotarization == nil {
		return fmt.Errorf("malformed QuorumRound, empty block and notarization fields")
	}

	if rqr.Block != nil && (rqr.Notarization == nil && rqr.Finalization == nil) {
		return fmt.Errorf("malformed QuorumRound, block but no notarization or finalization")
	}

	return nil
}

// QuorumRound represents a round that has achieved quorum on either
// (empty notarization), (block & notarization), or (block, finalization)
type QuorumRound struct {
	Block             Block
	Notarization      *Notarization
	Finalization      *Finalization
	EmptyNotarization *EmptyNotarization
}

// Raw returns a RawQuorumRound with the same content but unparsed QCs.
func (q *QuorumRound) Raw() *RawQuorumRound {
	var rawNotarization *RawNotarization
	var rawFinalization *RawFinalization
	var rawEmptyNotarization *RawEmptyNotarization

	if q.Notarization != nil {
		rawNotarization = &RawNotarization{
			Vote: q.Notarization.Vote,
			QC:   q.Notarization.QC,
		}
	}

	if q.Finalization != nil {
		rawFinalization = &RawFinalization{
			Finalization: q.Finalization.Finalization,
			QC:           q.Finalization.QC,
		}
	}

	if q.EmptyNotarization != nil {
		rawEmptyNotarization = &RawEmptyNotarization{
			Vote: q.EmptyNotarization.Vote,
			QC:   q.EmptyNotarization.QC,
		}
	}

	return &RawQuorumRound{
		Block:             q.Block,
		Notarization:	  rawNotarization,
		Finalization:      rawFinalization,
		EmptyNotarization: rawEmptyNotarization,
	}
}

// isWellFormed returns an error if the QuorumRound is not in one
// one of these formats
// (block, notarization), (block, finalization) or (empty notarization)
func (q *QuorumRound) IsWellFormed() error {
	if q.Block == nil && q.EmptyNotarization == nil {
		return fmt.Errorf("malformed QuorumRound, empty block and notarization fields")
	}

	if q.Block != nil && (q.Notarization == nil && q.Finalization == nil) {
		return fmt.Errorf("malformed QuorumRound, block but no notarization or finalization")
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

type BlockDigestRequest struct {
	Seq    uint64
	Digest Digest
}

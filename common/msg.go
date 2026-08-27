// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package common

import (
	"bytes"
	"encoding/asn1"
	"encoding/binary"
	"fmt"

	"github.com/ava-labs/simplex/avalanchego"
)

type Message struct {
	// Simplex Messages
	BlockMessage      *BlockMessage
	EmptyNotarization *EmptyNotarization
	VoteMessage       *Vote
	EmptyVoteMessage  *EmptyVote
	Notarization      *Notarization
	FinalizeVote      *FinalizeVote
	Finalization      *Finalization

	// Replication Messages
	ReplicationResponse *ReplicationResponse
	ReplicationRequest  *ReplicationRequest
	BlockDigestRequest  *BlockDigestRequest

	// Verified Messages
	VerifiedBlockMessage        *VerifiedBlockMessage
	VerifiedReplicationResponse *VerifiedReplicationResponse

	// Epoch Transition Messages
	AuxiliaryInfo           *AuxiliaryInfo
	EpochTransitionApproval *ValidatorSetApproval
}

func (m *Message) IsReplicationMessage() bool {
	switch {
	case m.ReplicationResponse != nil:
		return true
	case m.ReplicationRequest != nil:
		return true
	case m.VerifiedReplicationResponse != nil:
		return true
	case m.BlockDigestRequest != nil:
		return true
	default:
		return false
	}
}

type EmptyVoteMetadata struct {
	Round uint64
	Epoch uint64
}

type ToBeSignedEmptyVote struct {
	EmptyVoteMetadata
}

const emptyVoteLen = 1 + 8 + 8 // Version + Epoch + Round

func (v *ToBeSignedEmptyVote) Bytes() []byte {
	bytes := make([]byte, emptyVoteLen)
	binary.BigEndian.PutUint64(bytes[1:9], v.Epoch)
	binary.BigEndian.PutUint64(bytes[9:17], v.Round)
	return bytes
}

func (v *ToBeSignedEmptyVote) FromBytes(buff []byte) error {
	if len(buff) != emptyVoteLen {
		return fmt.Errorf("invalid buffer length, expected %d, got %d", emptyVoteLen, len(buff))
	}

	epoch := binary.BigEndian.Uint64(buff[1:9])
	round := binary.BigEndian.Uint64(buff[9:17])

	v.EmptyVoteMetadata = EmptyVoteMetadata{
		Round: round,
		Epoch: epoch,
	}
	return nil
}

func (v *ToBeSignedEmptyVote) Size() int {
	return emptyVoteLen
}

func (v *ToBeSignedEmptyVote) Sign(signer Signer) ([]byte, error) {
	context := "ToBeSignedEmptyVote"
	msg := v.Bytes()

	return signContext(signer, msg, context)
}

func (v *ToBeSignedEmptyVote) Verify(signature []byte, verifier SignatureVerifier, pk []byte) error {
	context := "ToBeSignedEmptyVote"
	msg := v.Bytes()

	return verifyContext(signature, verifier, msg, context, pk)
}

type ToBeSignedVote struct {
	BlockHeader
}

func (v *ToBeSignedVote) Sign(signer Signer) ([]byte, error) {
	context := "ToBeSignedVote"
	msg := v.Bytes()

	return signContext(signer, msg, context)
}

func (v *ToBeSignedVote) Verify(signature []byte, verifier SignatureVerifier, pk []byte) error {
	context := "ToBeSignedVote"
	msg := v.Bytes()

	return verifyContext(signature, verifier, msg, context, pk)
}

type ToBeSignedFinalization struct {
	BlockHeader
}

func (f *ToBeSignedFinalization) Sign(signer Signer) ([]byte, error) {
	context := "ToBeSignedFinalization"
	msg := f.Bytes()

	return signContext(signer, msg, context)
}

func (f *ToBeSignedFinalization) Verify(signature []byte, verifier SignatureVerifier, pk []byte) error {
	context := "ToBeSignedFinalization"
	msg := f.Bytes()

	return verifyContext(signature, verifier, msg, context, pk)
}

func signContext(signer Signer, msg []byte, context string) ([]byte, error) {
	sm := SignedMessage{Payload: msg, Context: context}
	toBeSigned, err := asn1.Marshal(sm)
	if err != nil {
		return nil, err
	}
	return signer.Sign(toBeSigned)
}

func verifyContext(signature []byte, verifier SignatureVerifier, msg []byte, context string, pk []byte) error {
	sm := SignedMessage{Payload: msg, Context: context}
	toBeSigned, err := asn1.Marshal(sm)
	if err != nil {
		return err
	}
	return verifier.VerifySignature(toBeSigned, signature, pk)
}

func verifyContextQC(qc QuorumCertificate, msg []byte, context string, nodes Nodes) error {
	sm := SignedMessage{Payload: msg, Context: context}
	toBeSigned, err := asn1.Marshal(sm)
	if err != nil {
		return err
	}

	return qc.Verify(toBeSigned, nodes)
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

// Finalization represents a block that has reached quorum on block. This
// means that block can be included in the chain and finalized.
type Finalization struct {
	Finalization ToBeSignedFinalization
	QC           QuorumCertificate
}

func (f *Finalization) Verify(nodes Nodes) error {
	context := "ToBeSignedFinalization"
	return verifyContextQC(f.QC, f.Finalization.Bytes(), context, nodes)
}

func (f *Finalization) Size() int {
	return f.Finalization.Size() + f.QC.Size()
}

// Notarization represents a block that has reached a quorum of votes.
type Notarization struct {
	Vote ToBeSignedVote
	QC   QuorumCertificate
}

func (n *Notarization) Verify(nodes Nodes) error {
	context := "ToBeSignedVote"
	return verifyContextQC(n.QC, n.Vote.Bytes(), context, nodes)
}

func (n *Notarization) Size() int {
	return n.Vote.Size() + n.QC.Size()
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

func (en *EmptyNotarization) Verify(nodes Nodes) error {
	context := "ToBeSignedEmptyVote"
	return verifyContextQC(en.QC, en.Vote.Bytes(), context, nodes)
}
func (en *EmptyNotarization) Size() int {
	return en.Vote.Size() + en.QC.Size()
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
	Verify(msg []byte, nodes Nodes) error
	// Bytes returns a raw representation of the given QuorumCertificate.
	Bytes() []byte
	// Size returns the number of bytes
	Size() int
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

	if q.Finalization != nil && q.Block == nil {
		return fmt.Errorf("malformed QuorumRound, finalization but no block")
	}

	if q.Notarization != nil && q.Block == nil {
		return fmt.Errorf("malformed QuorumRound, notarization but no block")
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

func (q *VerifiedQuorumRound) Size() int {
	var size int

	if q.VerifiedBlock != nil {
		size += q.VerifiedBlock.Size()
	}
	if q.Notarization != nil {
		size += q.Notarization.Size()
	}
	if q.Finalization != nil {
		size += q.Finalization.Size()
	}
	if q.EmptyNotarization != nil {
		size += q.EmptyNotarization.Size()
	}
	return size
}

type VerifiedFinalizedBlock struct {
	VerifiedBlock VerifiedBlock
	Finalization  Finalization
}

type BlockDigestRequest struct {
	Seq    uint64
	Digest Digest
}

// VersionID is an identifier for applications that care about epoch changes.
type VersionID uint32

//go:generate go run github.com/StephenButtolph/canoto/canoto msg.go

// AuxiliaryInfo defines application-specific information for applications that might care about epoch change,
// such as distributed key generation.
type AuxiliaryInfo struct {
	// The epoch this Auxiliary info is associated with
	Epoch uint64 `canoto:"uint,1"`

	// Version is an identifier that identifies the application.
	// Can be used for backward-compatibility and upgrade purposes.
	Version VersionID `canoto:"uint,2"`

	// Data is opaque bytes that can be used by applications to encode any information that describes
	// the current state for the application.
	Data []byte `canoto:"bytes,3"`

	canotoData canotoData_AuxiliaryInfo
}

// Clone returns a copy of the AuxiliaryInfo.
func (ai *AuxiliaryInfo) Clone() AuxiliaryInfo {
	return AuxiliaryInfo{
		Epoch:   ai.Epoch,
		Version: ai.Version,
		Data:    ai.Data,
	}
}

// ValidatorSetApproval is an approval from a validator
type ValidatorSetApproval struct {
	NodeID        avalanchego.NodeID
	AuxInfoDigest [32]byte
	PChainHeight  uint64
	Signature     []byte
}

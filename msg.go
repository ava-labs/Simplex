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
// cancel wait for blocancel wait for block notarization 0
// [03-26|22:53:33.012] INFO Simplex/epoch.go:2097 Moving to a new round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "old round": 0, "new round": 1, "leader": "0100000000000000"}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:1138 Broadcast notarization {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 0, "digest": "2d16e6463c77d3fd64d0..."}
// calling start round on leader 0200000000000000 1
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:1952 Monitoring progress {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 1}
// registering wait for block should be built 1 1
// registered wait for block should be built 1 1
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:519 Received finalization message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0100000000000000", "round": 0}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:790 Counting finalizations {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 1, "votes": 1}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0400000000000000", "round": 1}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0100000000000000", "round": 1}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0200000000000000", "round": 1}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0300000000000000", "round": 1}
// [03-26|22:53:33.012] DEBUG Simplex/monitor.go:79 Executing f {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "taskID": 0}
// wait for block should be built 1 1
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:519 Received finalization message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0200000000000000", "round": 0}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:790 Counting finalizations {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 1, "votes": 2}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:519 Received finalization message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0300000000000000", "round": 0}
// [03-26|22:53:33.012] DEBUG Simplex/notarization.go:87 Collected finalization from node {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "NodeID": "0100000000000000", "round": 0}
// [03-26|22:53:33.012] DEBUG Simplex/notarization.go:87 Collected finalization from node {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "NodeID": "0200000000000000", "round": 0}
// [03-26|22:53:33.012] DEBUG Simplex/notarization.go:87 Collected finalization from node {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "NodeID": "0300000000000000", "round": 0}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:837 Received enough finalizations to finalize a block {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 0}
// [03-26|22:53:33.012] INFO Simplex/epoch.go:954 Committed block {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 0, "sequence": 0, "digest": "2d16e6463c77d3fd64d0..."}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:830 Progressing rounds due to commit {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 1, "current round": 1}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:869 Broadcast finalization certificate {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 0, "digest": "2d16e6463c77d3fd64d0..."}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:519 Received finalization message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0400000000000000", "round": 0}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:559 Received finalization for an already finalized round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 0}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:1290 Received block message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0200000000000000", "round": 1}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:1304 Handling block message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "digest": "b7860ca2209475a7f1c1...", "round": 1}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:1393 Scheduling block verification {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 1}
// [03-26|22:53:33.012] DEBUG Simplex/sched.go:136 Scheduling new ready task {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "dependency": "2d16e6463c77d3fd64d0..."}
// [03-26|22:53:33.012] INFO Simplex/notarization.go:46 Collected Quorum of votes {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 1, "votes": 3}
// [03-26|22:53:33.012] DEBUG Simplex/notarization.go:50 Collected vote from node {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "NodeID": "0200000000000000"}
// [03-26|22:53:33.012] DEBUG Simplex/notarization.go:50 Collected vote from node {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "NodeID": "0300000000000000"}
// [03-26|22:53:33.012] DEBUG Simplex/notarization.go:50 Collected vote from node {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "NodeID": "0100000000000000"}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:1216 Received notarization message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0200000000000000", "round": 1}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:1237 Received a notarization for a future round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 1}
// [03-26|22:53:33.012] DEBUG Simplex/sched.go:96 Woken up from sleep {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "ready tasks": 1}
// [03-26|22:53:33.012] DEBUG Simplex/sched.go:106 Running task {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "remaining ready tasks": 0}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:1511 Block verification started {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 1}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:1534 Persisted block to WAL {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 1, "digest": "b7860ca2209475a7f1c1..."}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0300000000000000", "round": 1}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0400000000000000", "round": 1}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0100000000000000", "round": 1}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:1216 Received notarization message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0200000000000000", "round": 1}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:1106 Persisted notarization to WAL {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "size": 129, "round": 1, "digest": "b7860ca2209475a7f1c1..."}
// cancel wait for block notarization 1
// cancel wait for block should be built 1 1
// [03-26|22:53:33.012] INFO Simplex/epoch.go:2097 Moving to a new round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "old round": 1, "new round": 2, "leader": "0200000000000000"}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:1138 Broadcast notarization {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 1, "digest": "b7860ca2209475a7f1c1..."}
// calling start round on leader 0300000000000000 2
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:1952 Monitoring progress {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 2}
// registering wait for block should be built 2 2
// registered wait for block should be built 2 2
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:519 Received finalization message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0100000000000000", "round": 1}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:790 Counting finalizations {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 2, "votes": 1}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0300000000000000", "round": 2}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0400000000000000", "round": 2}
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0100000000000000", "round": 2}
// eating up ctx
// [03-26|22:53:33.012] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0200000000000000", "round": 2}
// cancelled 2 1
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0400000000000000", "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0100000000000000", "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0200000000000000", "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0300000000000000", "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/monitor.go:81 Task executed {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "taskID": 0}
// [03-26|22:53:33.013] DEBUG Simplex/monitor.go:79 Executing f {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "taskID": 1}
// wait for block should be built 2 2
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2046 Broadcasting vote {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 1, "digest": "b7860ca2209475a7f1c1..."}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:652 Received vote message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0100000000000000", "round": 1, "digest": "b7860ca2209475a7f1c1..."}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1515 Block verification ended {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 1, "elapsed": "141.25µs"}
// [03-26|22:53:33.013] DEBUG Simplex/sched.go:108 Task finished execution {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "taskID": "b7860ca2209475a7f1c1..."}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:519 Received finalization message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0200000000000000", "round": 1}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:790 Counting finalizations {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 2, "votes": 2}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:519 Received finalization message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0300000000000000", "round": 1}
// [03-26|22:53:33.013] DEBUG Simplex/notarization.go:87 Collected finalization from node {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "NodeID": "0100000000000000", "round": 1}
// [03-26|22:53:33.013] DEBUG Simplex/notarization.go:87 Collected finalization from node {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "NodeID": "0200000000000000", "round": 1}
// [03-26|22:53:33.013] DEBUG Simplex/notarization.go:87 Collected finalization from node {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "NodeID": "0300000000000000", "round": 1}
// [03-26|22:53:33.013] DEBUG Simplex/sched.go:113 Enqueued newly ready tasks {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "number of ready tasks": 0}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:837 Received enough finalizations to finalize a block {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 1}
// [03-26|22:53:33.013] INFO Simplex/epoch.go:954 Committed block {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 1, "sequence": 1, "digest": "b7860ca2209475a7f1c1..."}
// [03-26|22:53:33.013] DEBUG Simplex/sched.go:94 No ready tasks, going to sleep {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:830 Progressing rounds due to commit {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 2, "current round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:869 Broadcast finalization certificate {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 1, "digest": "b7860ca2209475a7f1c1..."}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:519 Received finalization message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0400000000000000", "round": 1}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:559 Received finalization for an already finalized round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 1}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1290 Received block message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0300000000000000", "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1304 Handling block message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "digest": "da5c197b1b712f36615f...", "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1393 Scheduling block verification {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/sched.go:136 Scheduling new ready task {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "dependency": "b7860ca2209475a7f1c1..."}
// [03-26|22:53:33.013] INFO Simplex/notarization.go:46 Collected Quorum of votes {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 2, "votes": 3}
// [03-26|22:53:33.013] DEBUG Simplex/notarization.go:50 Collected vote from node {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "NodeID": "0100000000000000"}
// [03-26|22:53:33.013] DEBUG Simplex/notarization.go:50 Collected vote from node {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "NodeID": "0200000000000000"}
// [03-26|22:53:33.013] DEBUG Simplex/sched.go:96 Woken up from sleep {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "ready tasks": 1}
// [03-26|22:53:33.013] DEBUG Simplex/notarization.go:50 Collected vote from node {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "NodeID": "0300000000000000"}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1216 Received notarization message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0200000000000000", "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1237 Received a notarization for a future round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/sched.go:106 Running task {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "remaining ready tasks": 0}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1511 Block verification started {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1534 Persisted block to WAL {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 2, "digest": "da5c197b1b712f36615f..."}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0100000000000000", "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1216 Received notarization message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0200000000000000", "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1106 Persisted notarization to WAL {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "size": 129, "round": 2, "digest": "da5c197b1b712f36615f..."}
// cancel wait for block notarization 2
// cancel wait for block should be built 2 2
// [03-26|22:53:33.013] INFO Simplex/epoch.go:2097 Moving to a new round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "old round": 2, "new round": 3, "leader": "0300000000000000"}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1138 Broadcast notarization {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 2, "digest": "da5c197b1b712f36615f..."}
// calling start round on leader 0400000000000000 3
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1952 Monitoring progress {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 3}
// registering wait for block should be built 3 3
// registered wait for block should be built 3 3
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:519 Received finalization message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0100000000000000", "round": 2}
// eating up ctx
// cancelled 3 2
// [03-26|22:53:33.013] DEBUG Simplex/monitor.go:81 Task executed {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "taskID": 1}
// [03-26|22:53:33.013] DEBUG Simplex/monitor.go:79 Executing f {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "taskID": 2}
// wait for block should be built 3 3
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:790 Counting finalizations {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 3, "votes": 1}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0100000000000000", "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0200000000000000", "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0300000000000000", "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0400000000000000", "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0400000000000000", "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0200000000000000", "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0300000000000000", "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0400000000000000", "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0100000000000000", "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2046 Broadcasting vote {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 2, "digest": "da5c197b1b712f36615f..."}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:652 Received vote message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0100000000000000", "round": 2, "digest": "da5c197b1b712f36615f..."}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1515 Block verification ended {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 2, "elapsed": "113.125µs"}
// [03-26|22:53:33.013] DEBUG Simplex/sched.go:108 Task finished execution {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "taskID": "da5c197b1b712f36615f..."}
// [03-26|22:53:33.013] DEBUG Simplex/sched.go:113 Enqueued newly ready tasks {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "number of ready tasks": 0}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:519 Received finalization message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0200000000000000", "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/sched.go:94 No ready tasks, going to sleep {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:790 Counting finalizations {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 3, "votes": 2}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:519 Received finalization message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0300000000000000", "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/notarization.go:87 Collected finalization from node {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "NodeID": "0300000000000000", "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/notarization.go:87 Collected finalization from node {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "NodeID": "0100000000000000", "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/notarization.go:87 Collected finalization from node {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "NodeID": "0200000000000000", "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:837 Received enough finalizations to finalize a block {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 2}
// [03-26|22:53:33.013] INFO Simplex/epoch.go:954 Committed block {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 2, "sequence": 2, "digest": "da5c197b1b712f36615f..."}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:830 Progressing rounds due to commit {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 3, "current round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:869 Broadcast finalization certificate {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 2, "digest": "da5c197b1b712f36615f..."}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:519 Received finalization message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0400000000000000", "round": 2}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:559 Received finalization for an already finalized round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 2}
// asserting wal size
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1290 Received block message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0400000000000000", "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1304 Handling block message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "digest": "4cdd371a46f09fa10e98...", "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1393 Scheduling block verification {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/sched.go:136 Scheduling new ready task {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "dependency": "da5c197b1b712f36615f..."}
// injecting votes for block of rond 3 3
// [03-26|22:53:33.013] DEBUG Simplex/sched.go:96 Woken up from sleep {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "ready tasks": 1}
// [03-26|22:53:33.013] DEBUG Simplex/sched.go:106 Running task {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "remaining ready tasks": 0}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1511 Block verification started {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1534 Persisted block to WAL {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 3, "digest": "4cdd371a46f09fa10e98..."}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0200000000000000", "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0300000000000000", "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0100000000000000", "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2046 Broadcasting vote {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 3, "digest": "4cdd371a46f09fa10e98..."}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:652 Received vote message {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0100000000000000", "round": 3, "digest": "4cdd371a46f09fa10e98..."}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1059 Counting votes {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 3, "votes": 2, "from": "[0100000000000000 0400000000000000]"}
// sending timeout
// eating up block building
// [03-26|22:53:33.013] INFO Simplex/epoch.go:1978 It is time to build a block {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0300000000000000", "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0100000000000000", "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:2200 No future messages received for this round {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "from": "0200000000000000", "round": 3}
// [03-26|22:53:33.013] DEBUG Simplex/epoch.go:1515 Block verification ended {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 3, "elapsed": "66.958µs"}
// [03-26|22:53:33.013] DEBUG Simplex/sched.go:108 Task finished execution {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "taskID": "4cdd371a46f09fa10e98..."}
// [03-26|22:53:33.013] DEBUG Simplex/monitor.go:122 Scheduling task {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "timeout": "5s", "deadline": "[03-26|22:53:33.012]"}
// [03-26|22:53:33.013] DEBUG Simplex/monitor.go:81 Task executed {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "taskID": 2}
// waiting for block proposer timeout 3
// [03-26|22:53:33.013] DEBUG Simplex/sched.go:113 Enqueued newly ready tasks {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "number of ready tasks": 0}
// [03-26|22:53:33.013] DEBUG Simplex/sched.go:94 No ready tasks, going to sleep {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1}
// [03-26|22:53:33.013] DEBUG Simplex/monitor.go:61 Ticked {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "taskID": 3, "time": "[03-26|22:53:34.012]"}
// [03-26|22:53:33.024] DEBUG Simplex/monitor.go:61 Ticked {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "taskID": 4, "time": "[03-26|22:53:35.012]"}
// [03-26|22:53:33.035] DEBUG Simplex/monitor.go:61 Ticked {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "taskID": 5, "time": "[03-26|22:53:36.012]"}
// [03-26|22:53:33.046] DEBUG Simplex/monitor.go:61 Ticked {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "taskID": 6, "time": "[03-26|22:53:37.012]"}
// [03-26|22:53:33.057] DEBUG Simplex/monitor.go:64 Executing f {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "taskID": 7, "deadline": "[03-26|22:53:38.012]"}
// proposal wait time expired
// [03-26|22:53:33.057] INFO Simplex/epoch.go:1913 Timed out on block agreement {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 3, "leader": "0400000000000000"}
// [03-26|22:53:33.057] DEBUG Simplex/epoch.go:1931 Persisted empty vote to WAL {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 3, "size": 59}
// [03-26|22:53:33.057] DEBUG Simplex/epoch.go:981 Could not find empty vote with a quorum or more votes {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "round": 3}
// proposal wait time expired done
// [03-26|22:53:33.057] DEBUG Simplex/monitor.go:66 Executed f {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "taskID": 7, "time": "[03-26|22:53:38.012]", "deadline": "[03-26|22:53:38.012]"}
// [03-26|22:53:33.057] DEBUG Simplex/monitor.go:70 Ticked {"test": "TestEpochLeaderFailoverAfterProposal", "node": 1, "taskID": 7, "time": "[03-26|22:53:38.012]"}
// running crash and restart execution
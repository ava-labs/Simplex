// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package crypto

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"

	blst "github.com/supranational/blst/bindings/go"

	"github.com/ava-labs/simplex/common"
)

// dst is the domain tag used for BLS12-381 signatures.
// Inspired by AvalancheGo's bls.go.
var dst = []byte("BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_")

// Signer implements common.Signer using BLS12-381.
type Signer struct {
	sk *blst.SecretKey
	pk *blst.P1Affine
}

// NewSigner generates a fresh BLS key pair.
func NewSigner() (*Signer, error) {
	ikm := make([]byte, 32)
	if _, err := rand.Read(ikm); err != nil {
		return nil, fmt.Errorf("failed to generate random key material: %w", err)
	}
	sk := blst.KeyGen(ikm)
	pk := new(blst.P1Affine).From(sk)
	return &Signer{sk: sk, pk: pk}, nil
}

// NewSignerFromBytes loads a BLS signer from serialized secret key bytes.
func NewSignerFromBytes(skBytes []byte) (*Signer, error) {
	sk := new(blst.SecretKey).Deserialize(skBytes)
	if sk == nil {
		return nil, errors.New("failed to deserialize BLS secret key")
	}
	pk := new(blst.P1Affine).From(sk)
	return &Signer{sk: sk, pk: pk}, nil
}

// SecretKeyBytes returns the serialized secret key for persistence.
func (s *Signer) SecretKeyBytes() []byte {
	return s.sk.Serialize()
}

// NodeID returns the compressed public key as a common.NodeID.
func (s *Signer) NodeID() common.NodeID {
	return common.NodeID(s.pk.Compress())
}

// Sign signs the given message using BLS with domain separation.
func (s *Signer) Sign(message []byte) ([]byte, error) {
	sig := new(blst.P2Affine).Sign(s.sk, message, dst)
	if sig == nil {
		return nil, errors.New("BLS signing failed")
	}
	return sig.Compress(), nil
}

// Verifier implements common.SignatureVerifier using BLS12-381.
type Verifier struct {
	nodeID2PK map[string]*blst.P1Affine
}

// NewVerifier builds a Verifier from the given node list.
func NewVerifier(nodes []common.Node) *Verifier {
	m := make(map[string]*blst.P1Affine, len(nodes))
	for _, n := range nodes {
		pk := new(blst.P1Affine).Uncompress(n.Node)
		if pk != nil {
			m[string(n.Node)] = pk
		}
	}
	return &Verifier{nodeID2PK: m}
}

// Verify checks that the given signature on message was made by signer.
func (v *Verifier) Verify(message []byte, signature []byte, signer common.NodeID) error {
	pk, ok := v.nodeID2PK[string(signer)]
	if !ok {
		return fmt.Errorf("signer not found: %x", []byte(signer))
	}
	sig := new(blst.P2Affine).Uncompress(signature)
	if sig == nil {
		return errors.New("failed to deserialize BLS signature")
	}
	if !sig.Verify(false, pk, false, message, dst) {
		return errors.New("BLS signature verification failed")
	}
	return nil
}

// Aggregator implements common.SignatureAggregator using BLS12-381.
type Aggregator struct {
	nodes     []common.Node
	nodeID2PK map[string]*blst.P1Affine
}

// NewAggregator builds an Aggregator from the given node list.
func NewAggregator(nodes []common.Node) *Aggregator {
	m := make(map[string]*blst.P1Affine, len(nodes))
	for _, n := range nodes {
		pk := new(blst.P1Affine).Uncompress(n.Node)
		if pk != nil {
			m[string(n.Node)] = pk
		}
	}
	return &Aggregator{nodes: nodes, nodeID2PK: m}
}

// Aggregate aggregates several signatures into a QuorumCertificate.
func (a *Aggregator) Aggregate(sigs []common.Signature) (common.QuorumCertificate, error) {
	if len(sigs) == 0 {
		return nil, errors.New("no signatures to aggregate")
	}

	p2s := make([]*blst.P2Affine, 0, len(sigs))
	pks := make([]*blst.P1Affine, 0, len(sigs))
	signers := make([]common.NodeID, 0, len(sigs))

	for _, sig := range sigs {
		p2 := new(blst.P2Affine).Uncompress(sig.Value)
		if p2 == nil {
			return nil, fmt.Errorf("failed to deserialize signature from %x", []byte(sig.Signer))
		}
		pk, ok := a.nodeID2PK[string(sig.Signer)]
		if !ok {
			return nil, fmt.Errorf("signer not found: %x", []byte(sig.Signer))
		}
		p2s = append(p2s, p2)
		pks = append(pks, pk)
		signers = append(signers, sig.Signer)
	}

	agg := new(blst.P2Aggregate)
	if !agg.Aggregate(p2s, false) {
		return nil, errors.New("BLS aggregation failed")
	}
	aggSig := agg.ToAffine()

	return &BlsQuorumCertificate{
		signers: signers,
		aggSig:  aggSig,
		pks:     pks,
	}, nil
}

// AppendSignatures appends new individual signature bytes to an existing aggregated signature.
// If existing is empty, it aggregates the given signatures.
func (a *Aggregator) AppendSignatures(existing []byte, newSigs ...[]byte) ([]byte, error) {
	var p2s []*blst.P2Affine

	if len(existing) > 0 {
		p2 := new(blst.P2Affine).Uncompress(existing)
		if p2 == nil {
			return nil, errors.New("failed to deserialize existing aggregated signature")
		}
		p2s = append(p2s, p2)
	}

	for _, s := range newSigs {
		p2 := new(blst.P2Affine).Uncompress(s)
		if p2 == nil {
			return nil, errors.New("failed to deserialize signature to append")
		}
		p2s = append(p2s, p2)
	}

	if len(p2s) == 0 {
		return nil, errors.New("no signatures to aggregate")
	}

	agg := new(blst.P2Aggregate)
	if !agg.Aggregate(p2s, false) {
		return nil, errors.New("BLS aggregation failed")
	}
	return agg.ToAffine().Compress(), nil
}

// IsQuorum returns true if the given signers constitute a quorum (2f+1).
func (a *Aggregator) IsQuorum(signers []common.NodeID) bool {
	return len(signers) >= quorum(len(a.nodes))
}

// quorum returns the minimum number of nodes needed for a quorum (2f+1).
func quorum(n int) int {
	return 2*((n-1)/3) + 1
}

// BlsQuorumCertificate implements common.QuorumCertificate using BLS aggregation.
type BlsQuorumCertificate struct {
	signers []common.NodeID
	aggSig  *blst.P2Affine
	pks     []*blst.P1Affine
}

// Signers returns who participated in creating this QuorumCertificate.
func (q *BlsQuorumCertificate) Signers() []common.NodeID {
	return q.signers
}

// Verify checks whether the aggregated signature is valid for msg.
func (q *BlsQuorumCertificate) Verify(msg []byte) error {
	if len(q.pks) == 0 {
		return errors.New("no public keys in quorum certificate")
	}
	if !q.aggSig.FastAggregateVerify(false, q.pks, msg, dst) {
		return errors.New("BLS aggregate signature verification failed")
	}
	return nil
}

// Bytes serializes the QC as:
// [2-byte num_signers][48-byte pubkey each][96-byte agg sig]
func (q *BlsQuorumCertificate) Bytes() []byte {
	n := len(q.signers)
	buf := make([]byte, 2+n*48+96)
	binary.BigEndian.PutUint16(buf[0:2], uint16(n))
	for i, pk := range q.pks {
		copy(buf[2+i*48:], pk.Compress())
	}
	copy(buf[2+n*48:], q.aggSig.Compress())
	return buf
}

// QCDeserializer implements common.QCDeserializer.
type QCDeserializer struct {
	nodeID2PK map[string]*blst.P1Affine
}

// NewQCDeserializer creates a QCDeserializer from the given node list.
func NewQCDeserializer(nodes []common.Node) *QCDeserializer {
	m := make(map[string]*blst.P1Affine, len(nodes))
	for _, n := range nodes {
		pk := new(blst.P1Affine).Uncompress(n.Node)
		if pk != nil {
			m[string(n.Node)] = pk
		}
	}
	return &QCDeserializer{nodeID2PK: m}
}

// DeserializeQuorumCertificate parses the given bytes into a BlsQuorumCertificate.
// Format: [2-byte num_signers][48-byte pubkey each][96-byte agg sig]
func (d *QCDeserializer) DeserializeQuorumCertificate(data []byte) (common.QuorumCertificate, error) {
	if len(data) < 2 {
		return nil, errors.New("QC data too short")
	}
	n := int(binary.BigEndian.Uint16(data[0:2]))
	expectedLen := 2 + n*48 + 96
	if len(data) != expectedLen {
		return nil, fmt.Errorf("QC data length mismatch: expected %d, got %d", expectedLen, len(data))
	}

	signers := make([]common.NodeID, n)
	pks := make([]*blst.P1Affine, n)
	for i := 0; i < n; i++ {
		pkBytes := data[2+i*48 : 2+(i+1)*48]
		pk := new(blst.P1Affine).Uncompress(pkBytes)
		if pk == nil {
			return nil, fmt.Errorf("failed to deserialize public key %d", i)
		}
		pks[i] = pk
		signers[i] = common.NodeID(append([]byte(nil), pkBytes...))
	}

	sigBytes := data[2+n*48:]
	sig := new(blst.P2Affine).Uncompress(sigBytes)
	if sig == nil {
		return nil, errors.New("failed to deserialize aggregated signature")
	}

	return &BlsQuorumCertificate{
		signers: signers,
		aggSig:  sig,
		pks:     pks,
	}, nil
}

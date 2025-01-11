// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

const (
	voteContext         = "ToBeSignedVote"
	finalizationContext = "ToBeSignedFinalization"
)

type ToBeSignedVote struct {
	BlockHeader
}

func (v *ToBeSignedVote) Sign(signer Signer) ([]byte, error) {
	return signContext(signer, v.MarshalCanoto(), voteContext)
}

func (v *ToBeSignedVote) Verify(signature []byte, verifier SignatureVerifier, signers NodeID) error {
	return verifyContext(signature, verifier, v.MarshalCanoto(), voteContext, signers)
}

type ToBeSignedFinalization struct {
	BlockHeader
}

func (f *ToBeSignedFinalization) Sign(signer Signer) ([]byte, error) {
	return signContext(signer, f.MarshalCanoto(), finalizationContext)
}

func (f *ToBeSignedFinalization) Verify(signature []byte, verifier SignatureVerifier, signers NodeID) error {
	return verifyContext(signature, verifier, f.MarshalCanoto(), finalizationContext, signers)
}

func signContext(signer Signer, msg []byte, context string) ([]byte, error) {
	sm := SignedMessage{
		Payload: msg,
		Context: context,
	}
	toBeSigned := sm.MarshalCanoto()
	return signer.Sign(toBeSigned)
}

func verifyContext(signature []byte, verifier SignatureVerifier, msg []byte, context string, signers NodeID) error {
	sm := SignedMessage{
		Payload: msg,
		Context: context,
	}
	toBeSigned := sm.MarshalCanoto()
	return verifier.Verify(toBeSigned, signature, signers)
}

func verifyContextQC(qc QuorumCertificate, msg []byte, context string) error {
	sm := SignedMessage{
		Payload: msg,
		Context: context,
	}
	toBeSigned := sm.MarshalCanoto()
	return qc.Verify(toBeSigned)
}

func (fc *FinalizationCertificate) Verify() error {
	return verifyContextQC(fc.QC, fc.Finalization.MarshalCanoto(), finalizationContext)
}

func (n *Notarization) Verify() error {
	return verifyContextQC(n.QC, n.Vote.MarshalCanoto(), voteContext)
}

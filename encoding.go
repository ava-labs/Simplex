// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"encoding/asn1"
	"encoding/binary"
	"errors"
	"fmt"
	"simplex/record"
	. "simplex/record"
)

type QuorumRecord struct {
	Signatures [][]byte
	Signers    [][]byte
	Vote       []byte
}

func FinalizationCertificateFromRecord(record Record) (FinalizationCertificate, error) {
	var nr QuorumRecord
	_, err := asn1.Unmarshal(record.Payload, &nr)
	if err != nil {
		return FinalizationCertificate{}, err
	}

	signers := make([]NodeID, 0, len(nr.Signers))
	for _, signer := range nr.Signers {
		signers = append(signers, signer)
	}

	var fCert FinalizationCertificate
	var finalization Finalization
	// why is the finalization stored in a Vote field?
	if err := finalization.FromBytes(nr.Vote); err != nil {
		return FinalizationCertificate{}, err
	}
	fCert.Finalization = finalization

	fCert.SignaturesAndSigners = make([]*SignatureSignerPair, 0, len(nr.Signatures))
	for i, sig := range nr.Signatures {
		fCert.SignaturesAndSigners = append(fCert.SignaturesAndSigners, &SignatureSignerPair{
			Signature: sig,
			Signer:    signers[i],
		})
	}

	return fCert, nil
}

func NewQuorumRecord(signatures [][]byte, signers []NodeID, rawVote []byte, recordType uint16) Record {
	var qr QuorumRecord
	qr.Signatures = signatures
	qr.Vote = rawVote

	qr.Signers = make([][]byte, 0, len(signers))
	for _, signer := range signers {
		qr.Signers = append(qr.Signers, signer)
	}

	payload, err := asn1.Marshal(qr)
	if err != nil {
		panic(err)
	}

	return Record{
		Size:    uint32(len(payload)),
		Payload: payload,
		Type:    recordType,
	}
}

func NotarizationFromRecord(record Record) (Notarization, error) {
	var nr QuorumRecord
	_, err := asn1.Unmarshal(record.Payload, &nr)
	if err != nil {
		return Notarization{}, err
	}

	signers := make([]NodeID, 0, len(nr.Signers))
	for _, signer := range nr.Signers {
		signers = append(signers, signer)
	}

	var vote Vote
	if err := vote.FromBytes(nr.Vote); err != nil {
		return Notarization{}, err
	}

	var notarization Notarization
	notarization.Vote = vote
	notarization.AggregatedSignedVote = &AggregatedSignedVote{
		Signers:   signers,
		Signature: nr.Signatures[0],
	}
	notarization.SignaturesAndSigners = make([]*SignatureSignerPair, 0, len(nr.Signatures))
	for i, sig := range nr.Signatures {
		notarization.SignaturesAndSigners = append(notarization.SignaturesAndSigners, &SignatureSignerPair{
			Signature: sig,
			Signer:    signers[i],
		})
	}

	return  notarization, nil
}

// metadata size + blockdata size + metadata bytes + blockData bytes
func BlockRecord(md Metadata, blockData []byte) Record {
	mdBytes := md.Bytes()

	mdSizeBuff := make([]byte, 4)
	binary.BigEndian.PutUint32(mdSizeBuff, uint32(len(mdBytes)))

	blockDataSizeBuff := make([]byte, 4)
	binary.BigEndian.PutUint32(blockDataSizeBuff, uint32(len(blockData)))

	buff := make([]byte, len(mdBytes)+len(blockData)+len(mdSizeBuff)+len(blockDataSizeBuff))
	copy(buff, mdSizeBuff)
	copy(buff[4:], blockDataSizeBuff)
	copy(buff[8:], mdBytes)
	copy(buff[8+len(mdBytes):], blockData)

	return Record{
		Type:    record.BlockRecordType,
		Size:    uint32(len(buff)),
		Payload: buff,
	}
}

func blockFromRecord(r Record) (Metadata, []byte, error) {
	buff := r.Payload

	if len(buff) < 8 {
		return Metadata{}, nil, errors.New("buffer too small, expected 8 bytes")
	}

	mdSizeBuff := binary.BigEndian.Uint32(buff)
	blockDataSizeBuff := binary.BigEndian.Uint32(buff[4:])

	buff = buff[8:]

	expectedBuffSize := int(mdSizeBuff + blockDataSizeBuff)

	if len(buff) < expectedBuffSize {
		return Metadata{}, nil, fmt.Errorf("buffer too small, expected %d bytes", expectedBuffSize)
	}

	mdBuff := buff[:mdSizeBuff]

	var md Metadata
	if err := md.FromBytes(mdBuff); err != nil {
		return Metadata{}, nil, fmt.Errorf("failed to deserialize block metadata: %w", err)
	}

	payload := buff[mdSizeBuff:]

	return md, payload, nil
}

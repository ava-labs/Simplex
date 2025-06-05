// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"encoding/asn1"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ava-labs/simplex/record"
)

type QuorumRecord struct {
	QC   []byte
	Vote []byte
}

func FinalizationFromRecord(record []byte, qd QCDeserializer) (Finalization, error) {
	qcBytes, finalization, err := parseFinalizationRecord(record)
	if err != nil {
		return Finalization{}, err
	}

	qc, err := qd.DeserializeQuorumCertificate(qcBytes)
	if err != nil {
		return Finalization{}, err
	}

	return Finalization{
		Finalization: finalization,
		QC:           qc,
	}, nil
}

func parseFinalizationRecord(payload []byte) ([]byte, ToBeSignedFinalization, error) {
	payload = payload[2:]
	var nr QuorumRecord
	_, err := asn1.Unmarshal(payload, &nr)
	if err != nil {
		return nil, ToBeSignedFinalization{}, err
	}

	var finalization ToBeSignedFinalization
	if err := finalization.FromBytes(nr.Vote); err != nil {
		return nil, ToBeSignedFinalization{}, err
	}

	return nr.QC, finalization, nil
}

func NewQuorumRecord(qc []byte, rawVote []byte, recordType uint16) []byte {
	var qr QuorumRecord
	qr.QC = qc
	qr.Vote = rawVote

	payload, err := asn1.Marshal(qr)
	if err != nil {
		panic(err)
	}

	buff := make([]byte, len(payload)+2)
	binary.BigEndian.PutUint16(buff, recordType)
	copy(buff[2:], payload)

	return buff
}

// ParseNotarizationRecordBytes parses a notarization record into the bytes of the QC and the vote
func ParseNotarizationRecord(r []byte) ([]byte, ToBeSignedVote, error) {
	recordType := binary.BigEndian.Uint16(r)
	if recordType != record.NotarizationRecordType {
		return nil, ToBeSignedVote{}, fmt.Errorf("expected record type %d, got %d", record.NotarizationRecordType, recordType)
	}

	record := r[2:]
	var nr QuorumRecord
	_, err := asn1.Unmarshal(record, &nr)
	if err != nil {
		return nil, ToBeSignedVote{}, err
	}

	var vote ToBeSignedVote
	if err := vote.FromBytes(nr.Vote); err != nil {
		return nil, ToBeSignedVote{}, err
	}

	return nr.QC, vote, nil
}

func NotarizationFromRecord(record []byte, qd QCDeserializer) (Notarization, error) {
	qcBytes, vote, err := ParseNotarizationRecord(record)
	if err != nil {
		return Notarization{}, err
	}

	qc, err := qd.DeserializeQuorumCertificate(qcBytes)
	if err != nil {
		return Notarization{}, err
	}

	return Notarization{
		Vote: vote,
		QC:   qc,
	}, nil
}

func BlockRecord(bh BlockHeader, blockData []byte) []byte {
	mdBytes := bh.Bytes()

	mdSizeBuff := make([]byte, 4)
	binary.BigEndian.PutUint32(mdSizeBuff, uint32(len(mdBytes)))

	blockDataSizeBuff := make([]byte, 4)
	binary.BigEndian.PutUint32(blockDataSizeBuff, uint32(len(blockData)))

	buff := make([]byte, len(mdBytes)+len(blockData)+len(mdSizeBuff)+len(blockDataSizeBuff)+2)
	binary.BigEndian.PutUint16(buff, record.BlockRecordType)
	copy(buff[2:], mdSizeBuff)
	copy(buff[6:], blockDataSizeBuff)
	copy(buff[10:], mdBytes)
	copy(buff[10+len(mdBytes):], blockData)

	return buff
}

func BlockFromRecord(blockDeserializer BlockDeserializer, record []byte) (VerifiedBlock, error) {
	_, payload, err := ParseBlockRecord(record)
	if err != nil {
		return nil, err
	}

	return blockDeserializer.DeserializeBlock(payload)
}

func ParseBlockRecord(buff []byte) (BlockHeader, []byte, error) {
	recordType := binary.BigEndian.Uint16(buff)
	if recordType != record.BlockRecordType {
		return BlockHeader{}, nil, fmt.Errorf("expected record type %d, got %d", record.BlockRecordType, recordType)
	}

	buff = buff[2:]
	if len(buff) < 8 {
		return BlockHeader{}, nil, errors.New("buffer too small, expected 8 bytes")
	}

	mdSizeBuff := binary.BigEndian.Uint32(buff)
	blockDataSizeBuff := binary.BigEndian.Uint32(buff[4:])

	buff = buff[8:]

	expectedBuffSize := int(mdSizeBuff + blockDataSizeBuff)

	if len(buff) < expectedBuffSize {
		return BlockHeader{}, nil, fmt.Errorf("buffer too small, expected %d bytes", expectedBuffSize)
	}

	mdBuff := buff[:mdSizeBuff]

	var bh BlockHeader
	if err := bh.FromBytes(mdBuff); err != nil {
		return BlockHeader{}, nil, fmt.Errorf("failed to deserialize block metadata: %w", err)
	}

	payload := buff[mdSizeBuff:]

	return bh, payload, nil
}

func ParseEmptyNotarizationRecord(buff []byte) ([]byte, ToBeSignedEmptyVote, error) {
	recordType := binary.BigEndian.Uint16(buff[:2])
	if recordType != record.EmptyNotarizationRecordType {
		return nil, ToBeSignedEmptyVote{}, fmt.Errorf("expected record type %d, got %d", record.NotarizationRecordType, recordType)
	}

	record := buff[2:]
	var nr QuorumRecord
	_, err := asn1.Unmarshal(record, &nr)
	if err != nil {
		return nil, ToBeSignedEmptyVote{}, err
	}

	var vote ToBeSignedEmptyVote
	if err := vote.FromBytes(nr.Vote); err != nil {
		return nil, ToBeSignedEmptyVote{}, err
	}

	return nr.QC, vote, nil
}

func NewEmptyVoteRecord(emptyVote ToBeSignedEmptyVote) []byte {
	payload := emptyVote.Bytes()
	buff := make([]byte, len(payload)+2)
	binary.BigEndian.PutUint16(buff, record.EmptyVoteRecordType)
	copy(buff[2:], payload)

	return buff
}

func ParseEmptyVoteRecord(rawEmptyVote []byte) (ToBeSignedEmptyVote, error) {
	if len(rawEmptyVote) < 2 {
		return ToBeSignedEmptyVote{}, errors.New("expected at least two bytes")
	}

	recordType := binary.BigEndian.Uint16(rawEmptyVote[:2])

	if recordType != record.EmptyVoteRecordType {
		return ToBeSignedEmptyVote{}, fmt.Errorf("expected record type %d, got %d", record.EmptyVoteRecordType, recordType)
	}

	var emptyVote ToBeSignedEmptyVote
	if err := emptyVote.FromBytes(rawEmptyVote[2:]); err != nil {
		return ToBeSignedEmptyVote{}, err
	}

	return emptyVote, nil
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ava-labs/simplex/record"
)

type QuorumRecord struct {
	QC   []byte
	Vote []byte
}

func (qr *QuorumRecord) FromBytes(buff []byte) error {
	if len(buff) < 4 {
		return errors.New("buffer too small to contain vote length")
	}

	voteLen := binary.BigEndian.Uint32(buff[0:4])
	if len(buff) < int(4+voteLen) {
		return errors.New("buffer too small to contain vote")
	}

	if int(voteLen) > len(buff)-4 {
		return errors.New("vote length exceeds buffer size")
	}

	qr.Vote = make([]byte, voteLen)
	copy(qr.Vote, buff[4:4+voteLen])

	qr.QC = make([]byte, len(buff)-int(4+voteLen))
	copy(qr.QC, buff[4+voteLen:])

	return nil
}

func (qr *QuorumRecord) Bytes() []byte {
	voteLenBuff := make([]byte, 4)
	binary.BigEndian.PutUint32(voteLenBuff, uint32(len(qr.Vote)))

	buff := make([]byte, 4+len(qr.QC)+len(qr.Vote))
	copy(buff[0:4], voteLenBuff)
	copy(buff[4:4+len(qr.Vote)], qr.Vote)
	copy(buff[4+len(qr.Vote):], qr.QC)

	return buff
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
	if err := nr.FromBytes(payload); err != nil {
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

	payload := qr.Bytes()

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
	if err := nr.FromBytes(record); err != nil {
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

	buff := make([]byte, len(mdBytes)+len(blockData)+2)
	binary.BigEndian.PutUint16(buff, record.BlockRecordType)
	copy(buff[2:], mdBytes)
	copy(buff[2+BlockHeaderLen:], blockData)

	return buff
}

func BlockFromRecord(ctx context.Context, blockDeserializer BlockDeserializer, record []byte) (Block, error) {
	_, payload, err := ParseBlockRecord(record)
	if err != nil {
		return nil, err
	}

	return blockDeserializer.DeserializeBlock(ctx, payload)
}

func ParseBlockRecord(buff []byte) (BlockHeader, []byte, error) {
	recordType := binary.BigEndian.Uint16(buff)
	if recordType != record.BlockRecordType {
		return BlockHeader{}, nil, fmt.Errorf("expected record type %d, got %d", record.BlockRecordType, recordType)
	}

	buff = buff[2:]
	if len(buff) < BlockHeaderLen {
		return BlockHeader{}, nil, fmt.Errorf("buffer too small, expected %d bytes", BlockHeaderLen+2)
	}

	var bh BlockHeader
	if err := bh.FromBytes(buff[:BlockHeaderLen]); err != nil {
		return BlockHeader{}, nil, fmt.Errorf("failed to deserialize block metadata: %w", err)
	}

	buff = buff[BlockHeaderLen:]

	if len(buff) == 0 {
		return BlockHeader{}, nil, fmt.Errorf("buffer too small, expected block data but gone none")
	}

	return bh, buff, nil
}

func ParseEmptyNotarizationRecord(buff []byte) ([]byte, ToBeSignedEmptyVote, error) {
	recordType := binary.BigEndian.Uint16(buff[:2])
	if recordType != record.EmptyNotarizationRecordType {
		return nil, ToBeSignedEmptyVote{}, fmt.Errorf("expected record type %d, got %d", record.NotarizationRecordType, recordType)
	}

	record := buff[2:]
	var nr QuorumRecord
	if err := nr.FromBytes(record); err != nil {
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

func BlockRecordRetentionTerm(record []byte) (uint64, error) {
	if len(record) < 19 {
		return 0, fmt.Errorf("record too short to extract round, expected at least 19 bytes, got %d", len(record))
	}

	var pos int
	// First 2 bytes are for record type
	pos += 2
	// The next 9 bytes are for version and epoch.
	pos += 9
	// The next 8 bytes are for round.
	round := binary.BigEndian.Uint64(record[pos : pos+8])
	return round, nil
}

func QuorumRecordRetentionTerm(record []byte) (uint64, error) {
	if len(record) < 23 {
		return 0, fmt.Errorf("record too short to extract round, expected at least 23 bytes, got %d", len(record))
	}

	var pos int
	// First 2 bytes are for record type
	pos += 2
	// Next 4 bytes are for the size of the vote
	pos += 4
	// The next 9 bytes are for version and epoch.
	pos += 9
	// The next 8 bytes are for round.
	round := binary.BigEndian.Uint64(record[pos : pos+8])
	return round, nil
}

func EmptyVoteRecordRetentionTerm(record []byte) (uint64, error) {
	if len(record) < 19 {
		return 0, fmt.Errorf("record too short to extract round, expected at least 23 bytes, got %d", len(record))
	}

	var pos int
	// First 2 bytes are for record type
	pos += 2
	// The next 9 bytes are for version and epoch.
	pos += 9
	// The next 8 bytes are for round.
	round := binary.BigEndian.Uint64(record[pos : pos+8])
	return round, nil
}

type WALRetentionReader struct{}

func (wrr *WALRetentionReader) RetentionTerm(entry []byte) (uint64, error) {
	if len(entry) < 2 {
		return 0, fmt.Errorf("entry too short to extract record type, expected at least 2 bytes, got %d", len(entry))
	}

	recordType := binary.BigEndian.Uint16(entry[:2])
	switch recordType {
	case record.BlockRecordType:
		return BlockRecordRetentionTerm(entry)
	case record.NotarizationRecordType:
		return QuorumRecordRetentionTerm(entry)
	case record.EmptyNotarizationRecordType:
		return QuorumRecordRetentionTerm(entry)
	case record.EmptyVoteRecordType:
		return EmptyVoteRecordRetentionTerm(entry)
	default:
		return 0, fmt.Errorf("unknown record type %d for retention term extraction", recordType)
	}
}

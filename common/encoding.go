// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package common

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	maxAllocationSize = 100_000_000 // Arbitrary limit to prevent excessive memory allocation, should never happen in practice
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

// ParseNotarizationRecord parses a notarization record into the bytes of the QC and the vote
func ParseNotarizationRecord(r []byte) ([]byte, ToBeSignedVote, error) {
	recordType := binary.BigEndian.Uint16(r)
	if recordType != NotarizationRecordType {
		return nil, ToBeSignedVote{}, fmt.Errorf("expected record type %d, got %d", NotarizationRecordType, recordType)
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

func BlockRecord(bh BlockHeader, blockData []byte) ([]byte, error) {
	mdBytes := bh.Bytes()
	mdSize := len(mdBytes)
	buffSize := mdSize + len(blockData) + 2 + 4

	if buffSize > maxAllocationSize { // Arbitrary limit to prevent excessive memory allocation, should never happen in practice
		return nil, fmt.Errorf("block record size exceeds allowed size: %d", buffSize)
	}

	buff := make([]byte, buffSize)
	// 2 bytes for record type, 4 bytes for block header length, the rest is for the block header and block data
	binary.BigEndian.PutUint16(buff, BlockRecordType)
	binary.BigEndian.PutUint32(buff[2:], uint32(mdSize))
	copy(buff[6:], mdBytes)
	copy(buff[6+mdSize:], blockData)

	return buff, nil
}

func BlockFromRecord(ctx context.Context, blockDeserializer BlockDeserializer, record []byte) (Block, error) {
	_, payload, err := ParseBlockRecord(record)
	if err != nil {
		return nil, err
	}

	return blockDeserializer.DeserializeBlock(ctx, payload)
}

func ParseBlockRecord(buff []byte) (BlockHeader, []byte, error) {
	initialSize := len(buff)

	if len(buff) < 2 {
		return BlockHeader{}, nil, fmt.Errorf("buffer too small, expected at least 2 bytes for record type")
	}
	recordType := binary.BigEndian.Uint16(buff)
	if recordType != BlockRecordType {
		return BlockHeader{}, nil, fmt.Errorf("expected record type %d, got %d", BlockRecordType, recordType)
	}

	buff = buff[2:]

	if len(buff) < 4 {
		return BlockHeader{}, nil, fmt.Errorf("buffer too small, expected at least 4 bytes for metadata size")
	}

	mdSize := binary.BigEndian.Uint32(buff[0:4]) // Read metadata size
	if mdSize > maxAllocationSize {              // Arbitrary limit to prevent excessive memory allocation
		return BlockHeader{}, nil, fmt.Errorf("metadata size too large: %d bytes", mdSize)
	}

	buff = buff[4:] // Move past the metadata size field

	if len(buff) < int(mdSize) {
		return BlockHeader{}, nil, fmt.Errorf("buffer too small, expected %d bytes for block metadata but got %d", 6+mdSize, initialSize)
	}

	var blockHeader BlockHeader
	if err := blockHeader.FromBytes(buff[:mdSize]); err != nil {
		return BlockHeader{}, nil, fmt.Errorf("failed to deserialize block metadata: %w", err)
	}

	// The rest of the buffer is the block data
	buff = buff[mdSize:]

	if len(buff) == 0 {
		return BlockHeader{}, nil, fmt.Errorf("buffer too small, expected block data but gone none")
	}

	return blockHeader, buff, nil
}

func ParseEmptyNotarizationRecord(buff []byte) ([]byte, ToBeSignedEmptyVote, error) {
	recordType := binary.BigEndian.Uint16(buff[:2])
	if recordType != EmptyNotarizationRecordType {
		return nil, ToBeSignedEmptyVote{}, fmt.Errorf("expected record type %d, got %d", NotarizationRecordType, recordType)
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
	binary.BigEndian.PutUint16(buff, EmptyVoteRecordType)
	copy(buff[2:], payload)

	return buff
}

func ParseEmptyVoteRecord(rawEmptyVote []byte) (ToBeSignedEmptyVote, error) {
	if len(rawEmptyVote) < 2 {
		return ToBeSignedEmptyVote{}, errors.New("expected at least two bytes")
	}

	recordType := binary.BigEndian.Uint16(rawEmptyVote[:2])

	if recordType != EmptyVoteRecordType {
		return ToBeSignedEmptyVote{}, fmt.Errorf("expected record type %d, got %d", EmptyVoteRecordType, recordType)
	}

	var emptyVote ToBeSignedEmptyVote
	if err := emptyVote.FromBytes(rawEmptyVote[2:]); err != nil {
		return ToBeSignedEmptyVote{}, err
	}

	return emptyVote, nil
}

func BlockRecordRetentionTerm(record []byte) (uint64, error) {
	initialSize := len(record)

	var pos int
	// First 2 bytes are for record type
	pos += 2
	// The next 4 bytes are for metadata size
	pos += 4

	if len(record) < pos {
		return 0, fmt.Errorf("record too short to extract metadata size, expected at least %d bytes, got %d", pos, initialSize)
	}

	metadataSize := binary.BigEndian.Uint32(record[2:6])

	if metadataSize > maxAllocationSize {
		return 0, fmt.Errorf("metadata size too large: %d bytes", metadataSize)
	}

	record = record[6:] // Move the slice to start after the metadata size field

	if len(record) < int(metadataSize) {
		return 0, fmt.Errorf("record too short to extract round, expected at least %d bytes, got %d", 6+int(metadataSize), initialSize)
	}

	var blockHeader BlockHeader
	if err := blockHeader.FromBytes(record[0:metadataSize]); err != nil {
		return 0, fmt.Errorf("failed to deserialize block metadata: %w", err)
	}

	return blockHeader.Round, nil
}

func notarizationQuorumRecordRetentionTerm(record []byte) (uint64, error) {
	initialSize := len(record)

	var pos int
	// First 2 bytes are for record type
	pos += 2

	// Next 4 bytes are for the size of the vote
	pos += 4

	if len(record) < pos {
		return 0, fmt.Errorf("record too short to extract vote size, expected at least %d bytes, got %d", pos, initialSize)
	}

	voteSize := binary.BigEndian.Uint32(record[2:6])

	if voteSize > maxAllocationSize {
		return 0, fmt.Errorf("vote size too large: %d bytes", voteSize)
	}

	record = record[pos:] // Move the slice to start after the vote size field

	if len(record) < int(voteSize) {
		return 0, fmt.Errorf("record too short to extract vote, expected at least %d bytes, got %d", 6+int(voteSize), initialSize)
	}

	record = record[:voteSize] // Trim everything but the vote, we don't need anything else to extract the round

	var vote ToBeSignedVote
	if err := vote.FromBytes(record); err != nil {
		return 0, fmt.Errorf("failed to deserialize vote: %w", err)
	}

	return vote.Round, nil
}

func emptyNotarizationQuorumRecordRetentionTerm(record []byte) (uint64, error) {
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
	case BlockRecordType:
		return BlockRecordRetentionTerm(entry)
	case NotarizationRecordType:
		return notarizationQuorumRecordRetentionTerm(entry)
	case EmptyNotarizationRecordType:
		return emptyNotarizationQuorumRecordRetentionTerm(entry)
	case EmptyVoteRecordType:
		return EmptyVoteRecordRetentionTerm(entry)
	default:
		return 0, fmt.Errorf("unknown record type %d for retention term extraction", recordType)
	}
}

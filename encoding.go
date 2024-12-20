// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"encoding/asn1"
	"encoding/binary"
	"errors"
	"fmt"
	"simplex/record"
)

type QuorumRecord struct {
	Signatures [][]byte
	Signers    [][]byte
	Vote       []byte
}

func finalizationFromRecord(payload []byte) ([][]byte, []NodeID, Finalization, error) {
	var nr QuorumRecord
	_, err := asn1.Unmarshal(payload, &nr)
	if err != nil {
		return nil, nil, Finalization{}, err
	}

	signers := make([]NodeID, 0, len(nr.Signers))
	for _, signer := range nr.Signers {
		signers = append(signers, signer)
	}

	var finalization Finalization
	if err := finalization.FromBytes(nr.Vote); err != nil {
		return nil, nil, Finalization{}, err
	}

	return nr.Signatures, signers, finalization, nil
}

func quorumRecord(signatures [][]byte, signers []NodeID, rawVote []byte, recordType uint16) []byte {
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

	buff := make([]byte, len(payload)+2)
	binary.BigEndian.PutUint16(buff, recordType)
	copy(buff[2:], payload)

	return buff
}

func notarizationFromRecord(record []byte) ([][]byte, []NodeID, Vote, error) {
	record = record[2:]
	var nr QuorumRecord
	_, err := asn1.Unmarshal(record, &nr)
	if err != nil {
		return nil, nil, Vote{}, err
	}

	signers := make([]NodeID, 0, len(nr.Signers))
	for _, signer := range nr.Signers {
		signers = append(signers, signer)
	}

	var vote Vote
	if err := vote.FromBytes(nr.Vote); err != nil {
		return nil, nil, Vote{}, err
	}

	return nr.Signatures, signers, vote, nil
}

func blockRecord(md Metadata, blockData []byte) []byte {
	mdBytes := md.Bytes()

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

func blockFromRecord(buff []byte) (Metadata, []byte, error) {
	buff = buff[2:]
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

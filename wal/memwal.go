// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type InMemWAL struct {
	bb bytes.Buffer
	t  testing.TB
	// CompactAt is how many bytes may be appended before Compact rewrites the log.
	CompactAt        int
	uncompactedBytes int
}

func (wal *InMemWAL) Close() error {
	return nil
}

func NewMemWAL(t testing.TB) *InMemWAL {
	return &InMemWAL{
		t:         t,
		CompactAt: DefaultCompactionThreshold,
	}
}

func (wal *InMemWAL) Append(b []byte) error {
	w := &wal.bb
	err := writeRecord(w, b)
	require.NoError(wal.t, err)
	wal.uncompactedBytes += recordSizeLen + len(b) + recordChecksumLen
	return err
}

func (wal *InMemWAL) Compact(keep func([]byte) bool) error {
	if wal.uncompactedBytes < wal.CompactAt {
		return nil
	}
	records, err := wal.ReadAll()
	if err != nil {
		return err
	}
	wal.bb.Reset()
	for _, record := range records {
		if keep(record) {
			require.NoError(wal.t, writeRecord(&wal.bb, record))
		}
	}
	wal.uncompactedBytes = 0
	return nil
}

func (wal *InMemWAL) ReadAll() ([][]byte, error) {
	r := bytes.NewBuffer(wal.bb.Bytes())
	var res [][]byte
	for r.Len() > 0 {
		payload, _, err := readRecord(r, int64(r.Len()))
		if err != nil {
			return nil, fmt.Errorf("failed reading in-memory record: %w", err)
		}
		res = append(res, payload)
	}
	return res, nil
}

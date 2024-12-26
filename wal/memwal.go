// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"bytes"
	"fmt"
)

type InMemWAL bytes.Buffer

func (wal *InMemWAL) Append(b []byte) error {
	w := (*bytes.Buffer)(wal)
	return writeRecord(w, b)
}

func (wal *InMemWAL) ReadAll() ([][]byte, error) {
	r := bytes.NewBuffer((*bytes.Buffer)(wal).Bytes())
	var res [][]byte
	for r.Len() > 0 {
		payload, _, err := readRecord(r, uint32(r.Len()))
		if err != nil {
			return nil, fmt.Errorf("failed reading in-memory record: %w", err)
		}
		res = append(res, payload)
	}
	return res, nil
}

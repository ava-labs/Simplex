// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"bytes"
	"fmt"
	"sync"
)

type InMemWAL struct {
	mu    sync.Mutex
	buffer bytes.Buffer
}


func (wal *InMemWAL) Append(b []byte) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	
	w := (*bytes.Buffer)(&wal.buffer)
	return writeRecord(w, b)
}

func (wal *InMemWAL) ReadAll() ([][]byte, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	r := bytes.NewBuffer((*bytes.Buffer)(&wal.buffer).Bytes())
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

// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"bytes"
	"fmt"
	"simplex"
)

var _ simplex.WriteAheadLog = &InMemWAL{}

type InMemWAL bytes.Buffer

func (wal *InMemWAL) Append(record *simplex.Record) error {
	w := (*bytes.Buffer)(wal)
	_, err := w.Write(record.Bytes())
	return err
}

func (wal *InMemWAL) ReadAll() ([]simplex.Record, error) {
	res := make([]simplex.Record, 0, 100)

	r := (*bytes.Buffer)(wal)
	var bytesRead int

	total := r.Len()

	for bytesRead < total {
		var record simplex.Record
		n, err := record.FromBytes(r)
		if err != nil {
			return []simplex.Record{}, fmt.Errorf("failed reading record: %v", err)
		}

		bytesRead += n
		res = append(res, record)
	}

	return res, nil
}

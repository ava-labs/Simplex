// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"bytes"
	"fmt"

	"simplex/record"
)

type InMemWAL bytes.Buffer

func (wal *InMemWAL) Append(record *record.Record) {
	w := (*bytes.Buffer)(wal)
	w.Write(record.Bytes())
}

func (wal *InMemWAL) ReadAll() []record.Record {
	r := bytes.NewBuffer((*bytes.Buffer)(wal).Bytes())
	res := make([]record.Record, 0, 100)
	for r.Len() > 0 {
		var record record.Record
		if _, err := record.FromBytes(r); err != nil {
			panic(fmt.Sprintf("failed reading record: %v", err))
		}
		res = append(res, record)
	}
	return res
}

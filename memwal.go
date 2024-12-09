// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"fmt"
	"sniplex/record"
)

type InMemWAL bytes.Buffer

func (wal *InMemWAL) Append(record *record.Record) {
	w := (*bytes.Buffer)(wal)
	w.Write(record.Bytes())
}

func (wal *InMemWAL) ReadAll() []record.Record {
	res := make([]record.Record, 0, 100)

	r := (*bytes.Buffer)(wal)
	var bytesRead int

	total := r.Len()

	for bytesRead < total {
		var record record.Record
		n, err := record.FromBytes(r)
		if err != nil {
			panic(fmt.Sprintf("failed reading record: %v", err))
		}

		bytesRead += n
		res = append(res, record)
	}

	return res
}

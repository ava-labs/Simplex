// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package record

const (
	UndefinedRecordType uint16 = iota
	BlockRecordType
	NotarizationRecordType
	EmptyVoteRecordType
	EmptyNotarizationRecordType
	FinalizationRecordType
)

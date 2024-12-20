// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package record


const (
	UndefinedRecordType uint16 = iota
	BlockRecordType
	NotarizationRecordType
	FinalizationRecordType

	recordChecksumLen = 8
	recordSizeLen     = 4
	recordVersionLen  = 1
	recordTypeLen     = 2

	maxBlockSize = 100_000_000 // ~ 100MB
)

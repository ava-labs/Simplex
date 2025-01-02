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
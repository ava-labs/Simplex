package random_network

import "time"

type FuzzConfig struct {
	// The minimum and maximum number of nodes in the network. Default is between 3 and 10.
	MinNodes int
	MaxNodes int

	// The minimum and maximum number delay (in milliseconds) to introduce between
	// when a transaction is created and when it is submitted to the network.
	// Default is between 10ms and 100ms.
	MinTxIssuanceDelay time.Duration
	MaxTxIssuanceDelay time.Duration

	// The probability that a transaction verification will fail. Default is 1%.
	TxVVerificationFailure int

	// The minimum and maximum number of transactions per block. Default is between 5 and 20.
	MinTxsPerBlock int
	MaxTxsPerBlock int

	// The number of blocks that must be finalized before ending the fuzz test. Default is 100.
	NumFinalizedBlocks int

	RandomSeed int64

	// Frequency at which to update time in the network. Default is 100 MS.
	TimeUpdateFrequency time.Duration

	// randomly crashes up to f nodes every crashInterval. if set to 0, no crashes occur. Default is 800ms.
	CrashInterval time.Duration
}

func DefaultFuzzConfig() *FuzzConfig {
	return &FuzzConfig{
		MinNodes:               3,
		MaxNodes:               10,
		MinTxIssuanceDelay:             10 * time.Millisecond,
		MaxTxIssuanceDelay:             100 * time.Millisecond,
		TxVVerificationFailure: 1,
		MinTxsPerBlock:         5,
		MaxTxsPerBlock:         20,
		NumFinalizedBlocks:     100,
		RandomSeed:             time.Now().UnixNano(),
		TimeUpdateFrequency:    100 * time.Millisecond,
		CrashInterval:          800 * time.Millisecond,
	}
}

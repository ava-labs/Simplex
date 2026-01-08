package random_network

import "time"

type FuzzConfig struct {
	// The minimum and maximum number of nodes in the network. Default is between 3 and 10.
	MinNodes int
	MaxNodes int

	// The minimum and maximum number delay (in milliseconds) to introduce between
	// when a transaction is created and when it is submitted to the network.
	// Default is between 10ms and 100ms.
	MinTxDelay time.Duration
	MaxTxDelay time.Duration

	// The probability that a transaction verification will fail. Default is 1%.
	TxVVerificationFailure int

	// The minimum and maximum number of transactions per block. Default is between 5 and 20.
	MinTxsPerBlock int
	MaxTxsPerBlock int

	// The number of blocks that must be finalized before ending the fuzz test. Default is 1000.
	NumFinalizedBlocks int

	RandomSeed int64
}

func DefaultFuzzConfig() *FuzzConfig {
	return &FuzzConfig{
		MinNodes:               3,
		MaxNodes:               10,
		MinTxDelay:             10 * time.Millisecond,
		MaxTxDelay:             100 * time.Millisecond,
		TxVVerificationFailure: 1,
		MinTxsPerBlock:         5,
		MaxTxsPerBlock:         20,
		NumFinalizedBlocks:     1000,
		RandomSeed:             time.Now().UnixNano(),
	}
}

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

	// The probability that a transaction verification will fail. Default is .1%.
	TxVerificationFailure float64

	// The minimum and maximum number of transactions per block. Default is between 5 and 20.
	MinTxsPerBlock int
	MaxTxsPerBlock int

	// The number of blocks that must be finalized before ending the fuzz test. Default is 100.
	NumFinalizedBlocks int

	RandomSeed int64

	// Frequency at which to update time in the network. Default is 100 MS.
	TimeUpdateFrequency time.Duration
	// Amount of time to increment on each time update. Default is 500 MS.
	TimeUpdateAmount time.Duration

	// randomly crashes up to f nodes every crashInterval. if set to 0, no crashes occur. Default is 800ms.
	CrashInterval time.Duration

	// Optional directory for writing logs to files. If empty, logs only to console.
	// When set, creates main.log for network logs and {nodeID-short}.log for each node.
	// NodeID is represented as a 16-character hex string (first 8 bytes).
	// Default is "tmp".
	LogDirectory string
}

func DefaultFuzzConfig() *FuzzConfig {
	return &FuzzConfig{
		MinNodes:              3,
		MaxNodes:              10,
		MinTxIssuanceDelay:    10 * time.Millisecond,
		MaxTxIssuanceDelay:    100 * time.Millisecond,
		TxVerificationFailure: .001,
		MinTxsPerBlock:        5,
		MaxTxsPerBlock:        20,
		NumFinalizedBlocks:    100,
		RandomSeed:            time.Now().UnixMilli(),
		TimeUpdateFrequency:   100 * time.Millisecond,
		TimeUpdateAmount:      500 * time.Millisecond,
		CrashInterval:         800 * time.Millisecond,
		LogDirectory:          "tmp",
	}
}

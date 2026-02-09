// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package random_network

import "time"

type FuzzConfig struct {
	// The minimum and maximum number of nodes in the network. Default is between 3 and 10.
	MinNodes int
	MaxNodes int

	// The probability that a transaction verification will fail. Default is .1%.
	TxVerificationFailure float64

	// The minimum and maximum number of transactions to be issued at a block. Default is between 5 and 20.
	MinTxsPerIssue int
	MaxTxsPerIssue int

	// Number of transactions per block. Default is 15.
	TxsPerBlock int

	// The number of blocks that must be finalized before ending the fuzz test. Default is 100.
	NumFinalizedBlocks int

	RandomSeed int64

	// Chance that a node will be randomly crashed. Default is 10%.
	NodeCrashPercentage float64

	// Chance that a crashed node will be restarted. Default is 50%.
	NodeRecoverPercentage float64

	// Amount to advance the time by. Default is 1000ms.
	AdvanceTimeTickAmount time.Duration

	// Creates main.log for network logs and {nodeID-short}.log for each node.
	// NodeID is represented as a 16-character hex string (first 8 bytes).
	// Default is "tmp".
	LogDirectory string
}

func DefaultFuzzConfig() *FuzzConfig {
	return &FuzzConfig{
		MinNodes:              3,
		MaxNodes:              10,
		TxVerificationFailure: .001,
		MinTxsPerIssue:        5,
		MaxTxsPerIssue:        20,
		TxsPerBlock:           15,
		NumFinalizedBlocks:    100,
		RandomSeed:            time.Now().UnixMilli(),
		NodeCrashPercentage:   0.1,
		NodeRecoverPercentage: 0.5,
		AdvanceTimeTickAmount: 1000 * time.Millisecond,
		LogDirectory:          "tmp",
	}
}

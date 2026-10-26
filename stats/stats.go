// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stats

type ValidatorCounters struct {
	RoundNum  uint64
	LastAcceptedBlockSeq    uint64
	RoundCount int
	EmptyVoteCount int
	FutureMessageCount int
	ReplicatedSeqs int
	ReplicatedRounds int
}
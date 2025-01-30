// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"context"
	. "simplex"
	"simplex/testutil"
	"simplex/wal"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestEpochLeaderFailover(t *testing.T) {
	timeoutDetected := make(chan struct{})

	l := testutil.MakeLogger(t, 1)
	l.Intercept(func(entry zapcore.Entry) error {
		if entry.Message == `Timed out on block agreement` {
			close(timeoutDetected)
		}
		return nil
	})

	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	start := time.Now()

	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		StartTime:           start,
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal.NewMemWAL(t),
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	// Run through 3 blocks, to make the block proposals be:
	// 1 --> 2 --> 3 --> X (node 4 doesn't propose a block)

	// Then, don't do anything and wait for our node
	// to start complaining about a block not being notarized

	// TODO: in future PRs we will expand this test to also do the actual agreement on the empty block

	rounds := uint64(3)

	for round := uint64(0); round < rounds; round++ {
		notarizeAndFinalizeRound(t, nodes, round, e, bb, quorum, storage)
	}

	bb.blockShouldBeBuilt <- struct{}{}

	waitForEvent(start, e, timeoutDetected)
}

func waitForEvent(start time.Time, e *Epoch, events chan struct{}) {
	now := start
	for {
		now = now.Add(e.EpochConfig.MaxProposalWait / 5)
		e.AdvanceTime(now)
		select {
		case <-events:
			return
		case <-time.After(time.Millisecond * 10):
			continue
		}
	}
}

func TestEpochLeaderFailoverNotNeeded(t *testing.T) {
	var timedOut atomic.Bool

	l := testutil.MakeLogger(t, 1)
	l.Intercept(func(entry zapcore.Entry) error {
		if entry.Message == `Timed out on block agreement` {
			timedOut.Store(true)
		}
		return nil
	})

	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	wal := newTestWAL(t)

	start := time.Now()

	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		StartTime:           start,
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	// Run through 3 blocks, to make the block proposals be:
	// 1 --> 2 --> 3 --> 4 (node 4 proposes a block eventually but not immediately

	rounds := uint64(3)

	for round := uint64(0); round < rounds; round++ {
		notarizeAndFinalizeRound(t, nodes, round, e, bb, quorum, storage)
	}
	bb.blockShouldBeBuilt <- struct{}{}
	e.AdvanceTime(start.Add(conf.MaxProposalWait / 2))

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)

	block := <-bb.out

	vote, err := newTestVote(block, nodes[3])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[3])
	require.NoError(t, err)

	// start at one since our node has already voted
	for i := 1; i < quorum; i++ {
		injectTestVote(t, e, block, nodes[i])
	}

	wal.assertNotarization(3)

	e.AdvanceTime(start.Add(conf.MaxProposalWait / 2))
	e.AdvanceTime(start.Add(conf.MaxProposalWait / 2))

	require.False(t, timedOut.Load())
}

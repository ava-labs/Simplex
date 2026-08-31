// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/ava-labs/simplex/common"
	. "github.com/ava-labs/simplex/simplex"
	"github.com/ava-labs/simplex/testutil"

	"github.com/stretchr/testify/require"
)

// decliningStorage wraps an InMemStorage and deliberately does not persist the blocks whose
// sequence is in declined. This mirrors the production storage, which never persists a Telock:
// a block that takes up a sequence in the dying epoch but is superseded by the first block of
// the next epoch. When reportSkip is set the skip is reported with ErrBlockNotIndexed, as the
// Storage contract requires; otherwise the skip is hidden behind a nil error.
type decliningStorage struct {
	*testutil.InMemStorage
	declined   map[uint64]struct{}
	reportSkip bool
}

func (d *decliningStorage) Index(ctx context.Context, block VerifiedBlock, certificate Finalization) error {
	if _, declined := d.declined[block.BlockHeader().Seq]; declined {
		if d.reportSkip {
			return fmt.Errorf("%w: seq %d is never persisted", ErrBlockNotIndexed, block.BlockHeader().Seq)
		}
		return nil
	}
	return d.InMemStorage.Index(ctx, block, certificate)
}

// TestEpochDoesNotCommitBlockTheStorageDeclinedToIndex asserts that a block the storage did
// not persist is not treated as committed.
//
// A nil error from Storage.Index is the engine's only signal that a block is durable at
// Storage.NumBlocks()-1. If the epoch advances e.lastBlock and e.round for a block that is not
// in storage, then nextSeqToCommit() - which is derived from the storage - stays behind, and
// the two can never reconcile: the node builds on, and advertises as finalized, a block that no
// node can ever commit at that sequence.
//
// The epoch is driven with four blocks whose finalizations arrive in order, with the storage
// declining the seq-2 block, both when it reports the skip and when it hides it behind a nil
// error. Only the blocks before it may be committed, the round must not advance past the
// sequence that was not committed, and a replicating peer must be told that the last committed
// block is the one actually in storage.
func TestEpochDoesNotCommitBlockTheStorageDeclinedToIndex(t *testing.T) {
	const (
		numBlocks   = uint64(4)
		declinedSeq = uint64(2)
	)

	for _, tc := range []struct {
		name       string
		reportSkip bool
	}{
		{name: "storage reports the skip", reportSkip: true},
		{name: "storage hides the skip behind a nil error", reportSkip: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes := []NodeID{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}}
			quorum := Quorum(len(nodes))
			blacklist := Blacklist{NodeCount: uint16(len(nodes)), SuspectedNodes: SuspectedNodes{}, Updates: []BlacklistUpdate{}}

			// The node under test leads a round beyond the ones exercised here, so it never proposes.
			epochNode := LeaderForRound(nodes, numBlocks+1)

			comm := &recordingComm{
				Communication:     testutil.NewNoopComm(NodeIDs(nodes)),
				SentMessages:      make(chan *Message, 1000),
				BroadcastMessages: make(chan *Message, 1000),
			}
			conf, _, inMemStorage := testutil.DefaultTestNodeEpochConfig(t, epochNode, comm, testutil.NewTestBlockBuilder())
			conf.ReplicationEnabled = true
			storage := &decliningStorage{
				InMemStorage: inMemStorage,
				declined:     map[uint64]struct{}{declinedSeq: {}},
				reportSkip:   tc.reportSkip,
			}
			conf.Storage = storage
			if !tc.reportSkip {
				// A storage that breaks the Index contract is reported as a warning, which is
				// expected here.
				conf.Logger.(*testutil.TestLogger).Silence()
			}

			e, err := NewEpoch(conf)
			require.NoError(t, err)
			t.Cleanup(e.Stop)
			require.NoError(t, e.Start())

			blocks := make([]*testutil.TestBlock, numBlocks)
			var prev Digest
			for i := uint64(0); i < numBlocks; i++ {
				blocks[i] = testutil.NewTestBlock(ProtocolMetadata{Round: i, Seq: i, Prev: prev}, blacklist)
				prev = blocks[i].BlockHeader().Digest
			}

			// Deliver every block as a proposal from the node that leads its round.
			for i := uint64(0); i < numBlocks; i++ {
				leader := LeaderForRound(nodes, i)
				require.NoError(t, e.HandleMessage(&Message{
					BlockMessage: &BlockMessage{Block: blocks[i], Vote: mustVote(t, blocks[i], leader)},
				}, leader))
			}

			sigAggr := e.SignatureAggregatorCreator(conf.Comm.Validators())

			// Finalize the blocks preceding the declined one, they are committed as usual.
			for i := uint64(0); i < declinedSeq; i++ {
				finalization, _ := testutil.NewFinalizationRecord(t, sigAggr, blocks[i], nodes[:quorum])
				require.NoError(t, e.HandleMessage(&Message{Finalization: &finalization}, nodes[0]))
				inMemStorage.WaitForBlockCommit(i)
			}
			require.Equal(t, declinedSeq, e.Metadata().Round, "the epoch should be in the round following the last committed block")

			// Finalize the declined block, and then the blocks after it.
			for i := declinedSeq; i < numBlocks; i++ {
				finalization, _ := testutil.NewFinalizationRecord(t, sigAggr, blocks[i], nodes[:quorum])
				require.NoError(t, e.HandleMessage(&Message{Finalization: &finalization}, nodes[0]))
			}

			// The declined block is not in storage, so it was not committed, and neither is anything
			// after it: the sequence it occupies is still the next sequence to commit.
			inMemStorage.EnsureNoBlockCommit(t, declinedSeq)
			require.Equal(t, declinedSeq, storage.NumBlocks(), "only the blocks preceding the declined one should be committed")

			// The round must not advance for a block that was not committed, otherwise the epoch
			// rejects the proposal that does belong at this sequence.
			require.Equal(t, declinedSeq, e.Metadata().Round, "the round must not advance past a block that was not committed")

			// A replicating peer must be told about the last block that is actually committed,
			// never about the block that was not persisted.
			require.NoError(t, e.HandleMessage(&Message{
				ReplicationRequest: &ReplicationRequest{LatestFinalizedSeq: 1},
			}, nodes[1]))
			latestFinalized := awaitLatestFinalizedSeq(t, comm)
			require.Equal(t, declinedSeq-1, latestFinalized.Finalization.Finalization.Seq,
				"the epoch must advertise the last committed block as its latest finalized sequence")
			require.Equal(t, blocks[declinedSeq-1].BlockHeader().Digest, latestFinalized.VerifiedBlock.BlockHeader().Digest)
		})
	}
}

// TestEpochRejectsFinalizationsFromPreviousEpochs asserts that a finalization for a block of an
// earlier epoch is not committed, whether it arrives as a finalization message or inside a
// replication response.
//
// Every sequence an epoch still has to commit belongs to that epoch or to a later one, so a
// finalization for an earlier epoch can only refer to a block that this epoch will never
// commit - such as a block of the previous epoch that was superseded by the epoch transition
// and was therefore never persisted. Its quorum certificate is not enough to reject it: the
// signers of a previous epoch may still form a quorum of the current validator set.
func TestEpochRejectsFinalizationsFromPreviousEpochs(t *testing.T) {
	const (
		previousEpoch = uint64(1)
		currentEpoch  = uint64(2)
		// The sequence of the first block of the current epoch, which the stale block also claims.
		contestedSeq = uint64(1)
	)

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	blacklist := Blacklist{NodeCount: uint16(len(nodes)), SuspectedNodes: SuspectedNodes{}, Updates: []BlacklistUpdate{}}

	comm := &recordingComm{
		Communication: testutil.NewNoopComm(NodeIDs(nodes)),
		SentMessages:  make(chan *Message, 1000),
	}
	conf, _, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], comm, testutil.NewTestBlockBuilder())
	conf.ReplicationEnabled = true
	conf.Epoch = currentEpoch

	// Seed the storage with the last block of the previous epoch, so the epoch starts where the
	// previous one left off, as a freshly transitioned node does.
	sealingBlock := testutil.NewTestBlock(ProtocolMetadata{Round: 0, Seq: 0, Epoch: previousEpoch}, blacklist)
	sealingFinalization, _ := testutil.NewFinalizationRecord(t, &testutil.TestSignatureAggregator{N: len(nodes)}, sealingBlock, nodes[:quorum])
	require.NoError(t, storage.Index(t.Context(), sealingBlock, sealingFinalization))

	e, err := NewEpoch(conf)
	require.NoError(t, err)
	e.Epoch = currentEpoch
	t.Cleanup(e.Stop)
	require.NoError(t, e.Start())

	require.Equal(t, currentEpoch, e.Epoch)
	roundBeforeReplay := e.Metadata().Round

	// A block of the previous epoch that claims the sequence the current epoch is about to
	// commit, finalized by a quorum of the (still overlapping) validator set.
	staleBlock := testutil.NewTestBlock(ProtocolMetadata{
		Round: roundBeforeReplay,
		Seq:   contestedSeq,
		Epoch: previousEpoch,
		Prev:  sealingBlock.BlockHeader().Digest,
	}, blacklist)
	sigAggr := e.SignatureAggregatorCreator(conf.Comm.Validators())
	staleFinalization, _ := testutil.NewFinalizationRecord(t, sigAggr, staleBlock, nodes[:quorum])

	staleNotarization, err := testutil.NewNotarization(e.Logger, sigAggr, staleBlock, nodes[:quorum])
	require.NoError(t, err)

	// Replay it through every path that could adopt it: a notarized quorum round, a finalized
	// quorum round, a proposal, and a plain finalization message.
	staleLeader := LeaderForRound(nodes, roundBeforeReplay)
	for _, msg := range []*Message{
		{ReplicationResponse: &ReplicationResponse{Data: []QuorumRound{{Block: staleBlock, Notarization: &staleNotarization}}}},
		{ReplicationResponse: &ReplicationResponse{Data: []QuorumRound{{Block: staleBlock, Finalization: &staleFinalization}}}},
		{BlockMessage: &BlockMessage{Block: staleBlock, Vote: mustVote(t, staleBlock, staleLeader)}},
		{Finalization: &staleFinalization},
	} {
		from := nodes[1]
		if msg.BlockMessage != nil {
			from = staleLeader
		}
		require.NoError(t, e.HandleMessage(msg, from))
	}

	// Storage, the commit cursor and the round are all left untouched.
	storage.EnsureNoBlockCommit(t, contestedSeq)
	require.Equal(t, contestedSeq, storage.NumBlocks())
	require.Equal(t, roundBeforeReplay, e.Metadata().Round)
	require.Equal(t, sealingBlock.BlockHeader().Digest, e.Metadata().Prev,
		"the epoch must keep building on the last committed block")
}

func mustVote(t *testing.T, block testutil.AnyBlock, from NodeID) Vote {
	t.Helper()
	vote, err := testutil.NewTestVote(block, from)
	require.NoError(t, err)
	return *vote
}

// awaitLatestFinalizedSeq returns the LatestFinalizedSeq of the first replication response
// the epoch sends.
func awaitLatestFinalizedSeq(t *testing.T, comm *recordingComm) *VerifiedQuorumRound {
	t.Helper()

	timeout := time.After(30 * time.Second)
	for {
		select {
		case msg := <-comm.SentMessages:
			if msg.VerifiedReplicationResponse != nil && msg.VerifiedReplicationResponse.LatestFinalizedSeq != nil {
				return msg.VerifiedReplicationResponse.LatestFinalizedSeq
			}
		case <-timeout:
			require.FailNow(t, "timed out waiting for a replication response with a latest finalized sequence")
			return nil
		}
	}
}

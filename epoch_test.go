// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/binary"
	"fmt"
	"math"
	rand2 "math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/record"
	"github.com/ava-labs/simplex/testutil"
	"github.com/ava-labs/simplex/wal"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestEpochHandleNotarizationFutureRound(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	// Create the two blocks ahead of time
	blocks := createBlocks(t, nodes, bb, 2)
	firstBlock := blocks[0].VerifiedBlock.(*testBlock)
	secondBlock := blocks[1].VerifiedBlock.(*testBlock)
	bb.out = make(chan *testBlock, 1)
	bb.in = make(chan *testBlock, 1)

	storage := newInMemStorage()

	wal := newTestWAL(t)

	quorum := Quorum(len(nodes))
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
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

	// Load the first block into the block builder, so it will not create its own block but use the pre-built one.
	// Drain the out channel before loading it
	//for len(bb.out) > 0 {
	//	<-bb.out
	//}
	bb.in <- firstBlock
	bb.out <- firstBlock

	// Create a notarization for round 1 which is a future round because we haven't gone through round 0 yet.
	notarization, err := newNotarization(l, &testSignatureAggregator{}, secondBlock, nodes)
	require.NoError(t, err)

	// Give the node the notarization message before receiving the first block
	e.HandleMessage(&Message{
		Notarization: &notarization,
	}, nodes[1])

	// Run through round 0
	notarizeAndFinalizeRound(t, e, bb)

	// Emulate round 1 by sending the block
	vote, err := newTestVote(secondBlock, nodes[1])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: secondBlock,
		},
	}, nodes[1])
	require.NoError(t, err)

	// The node should store the notarization of the second block once it gets the block.
	wal.assertNotarization(1)

	for i := 1; i < quorum; i++ {
		injectTestFinalizeVote(t, e, secondBlock, nodes[i])
	}

	blockCommitted := storage.waitForBlockCommit(1)
	require.Equal(t, secondBlock, blockCommitted)
}

// TestEpochIndexFinalization ensures that we properly index past finalizations when
// there have been empty rounds
func TestEpochIndexFinalization(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}

	storage := newInMemStorage()
	wal := newTestWAL(t)

	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
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
	firstBlock, _ := advanceRoundFromNotarization(t, e, bb)
	advanceRoundFromFinalization(t, e, bb)

	require.Equal(t, uint64(2), e.Metadata().Round)
	require.Equal(t, uint64(2), e.Metadata().Seq)
	require.Equal(t, uint64(0), e.Storage.Height())

	advanceRoundFromEmpty(t, e)
	require.Equal(t, uint64(3), e.Metadata().Round)
	require.Equal(t, uint64(2), e.Metadata().Seq)
	require.Equal(t, uint64(0), e.Storage.Height())

	advanceRoundFromFinalization(t, e, bb)
	require.Equal(t, uint64(4), e.Metadata().Round)
	require.Equal(t, uint64(3), e.Metadata().Seq)
	require.Equal(t, uint64(0), e.Storage.Height())

	// at this point we are waiting on finalization of seq 0.
	// when we receive that finalization, we should commit the rest of the finalizations for seqs
	// 1 & 2

	finalization, _ := newFinalizationRecord(t, conf.Logger, conf.SignatureAggregator, firstBlock, e.Comm.ListNodes())
	injectTestFinalization(t, e, &finalization, nodes[1])

	storage.waitForBlockCommit(2)
}

func TestEpochConsecutiveProposalsDoNotGetVerified(t *testing.T) {
	l := testutil.MakeLogger(t, 1)

	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	wal := newTestWAL(t)

	nodes := []NodeID{{1}, {2}, {3}, {4}}

	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[1],
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

	leader := nodes[0]

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	require.Equal(t, md.Round, md.Seq)

	onlyVerifyOnce := make(chan struct{})
	block := <-bb.out
	block.onVerify = func() {
		close(onlyVerifyOnce)
	}

	vote, err := newTestVote(block, leader)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(DefaultMaxPendingBlocks)

	for i := 0; i < DefaultMaxPendingBlocks; i++ {
		go func() {
			defer wg.Done()

			err := e.HandleMessage(&Message{
				BlockMessage: &BlockMessage{
					Vote:  *vote,
					Block: block,
				},
			}, leader)
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	select {
	case <-onlyVerifyOnce:
	case <-time.After(time.Minute):
		require.Fail(t, "timeout waiting for shouldOnlyBeClosedOnce")
	}
}

// TestEpochIncreasesRoundAfterFinalization ensures that the epochs round is incremented
// if we receive an finalization for the current round(even if it is not the next seq to commit)
func TestEpochIncreasesRoundAfterFinalization(t *testing.T) {
	l := testutil.MakeLogger(t, 1)

	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	wal := newTestWAL(t)

	nodes := []NodeID{{1}, {2}, {3}, {4}, {5}, {6}}

	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[2],
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

	block, _ := advanceRoundFromNotarization(t, e, bb)
	advanceRoundFromFinalization(t, e, bb)
	require.Equal(t, uint64(2), e.Metadata().Round)
	require.Equal(t, uint64(0), storage.Height())

	// create the finalized block
	finalization, _ := newFinalizationRecord(t, l, conf.SignatureAggregator, block, nodes)
	injectTestFinalization(t, e, &finalization, nodes[1])

	storage.waitForBlockCommit(1)
	require.Equal(t, uint64(2), e.Metadata().Round)
	require.Equal(t, uint64(2), storage.Height())

	// we are the leader, ensure we can continue & propose a block
	notarizeAndFinalizeRound(t, e, bb)
}

func TestEpochNotarizeTwiceThenFinalize(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	wal := newTestWAL(t)

	nodes := []NodeID{{1}, {2}, {3}, {4}}

	recordedMessages := make(chan *Message, 100)
	comm := &recordingComm{Communication: noopComm(nodes), BroadcastMessages: recordedMessages}

	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                comm,
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	// Round 0
	block0 := <-bb.out

	injectTestVote(t, e, block0, nodes[1])
	injectTestVote(t, e, block0, nodes[2])
	wal.assertNotarization(0)

	// Round 1
	emptyNote := newEmptyNotarization(nodes, 1, 0)
	err = e.HandleMessage(&Message{
		EmptyNotarization: emptyNote,
	}, nodes[1])
	require.NoError(t, err)
	emptyRecord := wal.assertNotarization(1)
	require.Equal(t, record.EmptyNotarizationRecordType, emptyRecord)

	// Round 2
	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	block1 := <-bb.out

	vote, err := newTestVote(block1, nodes[2])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block1,
		},
	}, nodes[1])
	require.NoError(t, err)

	injectTestVote(t, e, block1, nodes[3])
	wal.assertNotarization(2)

	// Round 3
	md = e.Metadata()
	_, ok = bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	block2 := <-bb.out

	vote, err = newTestVote(block2, nodes[3])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block2,
		},
	}, nodes[3])
	require.NoError(t, err)

	injectTestVote(t, e, block2, nodes[2])
	wal.assertNotarization(3)
	require.Equal(t, uint64(0), storage.Height())

	// drain the recorded messages
	for len(recordedMessages) > 0 {
		<-recordedMessages
	}

	blocks := make(map[uint64]*testBlock)
	blocks[0] = block0
	blocks[2] = block1

	var wg sync.WaitGroup
	wg.Add(1)

	finish := make(chan struct{})
	// Once the node sends a finalizeVote message, send it finalizeVote messages as a response
	go func() {
		defer wg.Done()
		for {
			select {
			case <-finish:
				return
			case msg := <-recordedMessages:
				if msg.FinalizeVote != nil {
					round := msg.FinalizeVote.Finalization.Round
					if block, ok := blocks[round]; ok {
						injectTestFinalizeVote(t, e, block, nodes[1])
						injectTestFinalizeVote(t, e, block, nodes[2])
					}
				}
			}
		}
	}()

	injectTestFinalizeVote(t, e, block2, nodes[1])
	injectTestFinalizeVote(t, e, block2, nodes[2])

	storage.waitForBlockCommit(0)
	storage.waitForBlockCommit(1)
	storage.waitForBlockCommit(2)

	close(finish)
	wg.Wait()
}

func TestEpochFinalizeThenNotarize(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	wal := newTestWAL(t)

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
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

	t.Run("commit without notarization, only with finalization", func(t *testing.T) {
		for round := 0; round < 100; round++ {
			advanceRoundFromFinalization(t, e, bb)
			storage.waitForBlockCommit(uint64(round))
		}
	})

	t.Run("notarization after commit without notarizations", func(t *testing.T) {
		// leader is the proposer of the new block for the given round
		leader := LeaderForRound(nodes, uint64(100))
		// only create blocks if we are not the node running the epoch
		if !leader.Equals(e.ID) {
			md := e.Metadata()
			_, ok := bb.BuildBlock(context.Background(), md)
			require.True(t, ok)
		}

		block := <-bb.out

		vote, err := newTestVote(block, nodes[0])
		require.NoError(t, err)
		err = e.HandleMessage(&Message{
			BlockMessage: &BlockMessage{
				Vote:  *vote,
				Block: block,
			},
		}, nodes[0])
		require.NoError(t, err)

		for i := 1; i < quorum; i++ {
			injectTestVote(t, e, block, nodes[i])
		}

		wal.assertNotarization(100)
	})

}

func TestEpochSimpleFlow(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 newTestWAL(t),
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	rounds := uint64(100)
	for round := uint64(0); round < rounds; round++ {
		notarizeAndFinalizeRound(t, e, bb)
	}
}

func TestEpochStartedTwice(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
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
	require.ErrorIs(t, e.Start(), ErrAlreadyStarted)
}

func advanceRoundFromEmpty(t *testing.T, e *Epoch) {
	leader := LeaderForRound(e.Comm.ListNodes(), e.Metadata().Round)
	require.False(t, e.ID.Equals(leader), "epoch cannot be the leader for the empty round")

	emptyNote := newEmptyNotarization(e.Comm.ListNodes(), e.Metadata().Round, e.Metadata().Seq)
	err := e.HandleMessage(&Message{
		EmptyNotarization: emptyNote,
	}, leader)

	require.NoError(t, err)

	emptyRecord := e.WAL.(*testWAL).assertNotarization(emptyNote.Vote.Round)
	require.Equal(t, record.EmptyNotarizationRecordType, emptyRecord)
}

func advanceRoundFromNotarization(t *testing.T, e *Epoch, bb *testBlockBuilder) (VerifiedBlock, *Notarization) {
	return advanceRound(t, e, bb, true, false)
}

func advanceRoundFromFinalization(t *testing.T, e *Epoch, bb *testBlockBuilder) VerifiedBlock {
	block, _ := advanceRound(t, e, bb, false, true)
	return block
}

func notarizeAndFinalizeRound(t *testing.T, e *Epoch, bb *testBlockBuilder) (VerifiedBlock, *Notarization) {
	return advanceRound(t, e, bb, true, true)
}

// advanceRound progresses [e] to a new round. If [notarize] is set, the round will progress due to a notarization.
// If [finalize] is set, the round will advance and the block will be indexed to storage.
func advanceRound(t *testing.T, e *Epoch, bb *testBlockBuilder, notarize bool, finalize bool) (VerifiedBlock, *Notarization) {
	require.True(t, notarize || finalize, "must either notarize or finalize a round to advance")
	nextSeqToCommit := e.Storage.Height()
	nodes := e.Comm.ListNodes()
	quorum := Quorum(len(nodes))
	// leader is the proposer of the new block for the given round
	leader := LeaderForRound(nodes, e.Metadata().Round)
	// only create blocks if we are not the node running the epoch
	isEpochNode := leader.Equals(e.ID)
	if !isEpochNode {
		md := e.Metadata()
		_, ok := bb.BuildBlock(context.Background(), md)
		require.True(t, ok)
	}

	block := <-bb.out

	if !isEpochNode {
		// send node a message from the leader
		vote, err := newTestVote(block, leader)
		require.NoError(t, err)
		err = e.HandleMessage(&Message{
			BlockMessage: &BlockMessage{
				Vote:  *vote,
				Block: block,
			},
		}, leader)
		require.NoError(t, err)
	}

	var notarization *Notarization
	if notarize {
		// start at one since our node has already voted
		n, err := newNotarization(e.Logger, e.SignatureAggregator, block, nodes[0:quorum])
		injectTestNotarization(t, e, n, nodes[1])

		e.WAL.(*testWAL).assertNotarization(block.metadata.Round)
		require.NoError(t, err)
		notarization = &n
	}

	if finalize {
		for i := 0; i <= quorum; i++ {
			if nodes[i].Equals(e.ID) {
				continue
			}
			injectTestFinalizeVote(t, e, block, nodes[i])
		}

		if nextSeqToCommit != block.metadata.Seq {
			waitToEnterRound(t, e, block.metadata.Round+1)
			return block, notarization
		}

		blockFromStorage := e.Storage.(*InMemStorage).waitForBlockCommit(block.metadata.Seq)
		require.Equal(t, block, blockFromStorage)
	}

	return block, notarization
}

func FuzzEpochInterleavingMessages(f *testing.F) {
	f.Fuzz(func(t *testing.T, seed int64) {
		testEpochInterleavingMessages(t, seed)
	})
}

func TestEpochInterleavingMessages(t *testing.T) {
	buff := make([]byte, 8)

	for i := 0; i < 100; i++ {
		_, err := rand.Read(buff)
		require.NoError(t, err)
		seed := int64(binary.BigEndian.Uint64(buff))
		testEpochInterleavingMessages(t, seed)
	}
}

func testEpochInterleavingMessages(t *testing.T, seed int64) {
	l := testutil.MakeLogger(t, 1)
	rounds := 10

	bb := &testBlockBuilder{in: make(chan *testBlock, rounds)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
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

	var protocolMetadata ProtocolMetadata

	callbacks := createCallbacks(t, rounds, protocolMetadata, nodes, e, bb)

	require.NoError(t, e.Start())

	r := rand2.New(rand2.NewSource(seed))
	for i, index := range r.Perm(len(callbacks)) {
		t.Log("Called callback", i, "out of", len(callbacks))
		callbacks[index]()
	}

	for i := 0; i < rounds; i++ {
		t.Log("Waiting for commit of round", i)
		storage.waitForBlockCommit(uint64(i))
	}
}

func createCallbacks(t *testing.T, rounds int, protocolMetadata ProtocolMetadata, nodes []NodeID, e *Epoch, bb *testBlockBuilder) []func() {
	blocks := make([]VerifiedBlock, 0, rounds)

	callbacks := make([]func(), 0, rounds*4+len(blocks))

	for i := 0; i < rounds; i++ {
		block := newTestBlock(protocolMetadata)
		blocks = append(blocks, block)

		protocolMetadata.Seq++
		protocolMetadata.Round++
		protocolMetadata.Prev = block.BlockHeader().Digest

		leader := LeaderForRound(nodes, uint64(i))

		if !leader.Equals(e.ID) {
			vote, err := newTestVote(block, leader)
			require.NoError(t, err)

			callbacks = append(callbacks, func() {
				t.Log("Injecting block", block.BlockHeader().Round)
				e.HandleMessage(&Message{
					BlockMessage: &BlockMessage{
						Block: block,
						Vote:  *vote,
					},
				}, leader)
			})
		} else {
			bb.in <- block
		}

		for j := 1; j <= 2; j++ {
			node := nodes[j]
			vote, err := newTestVote(block, node)
			require.NoError(t, err)
			msg := Message{
				VoteMessage: vote,
			}

			callbacks = append(callbacks, func() {
				t.Log("Injecting vote for round",
					msg.VoteMessage.Vote.Round, msg.VoteMessage.Vote.Digest, msg.VoteMessage.Signature.Signer)
				err := e.HandleMessage(&msg, node)
				require.NoError(t, err)
			})
		}

		for j := 1; j <= 2; j++ {
			node := nodes[j]
			vote := newTestFinalizeVote(t, block, node)
			msg := Message{
				FinalizeVote: vote,
			}
			callbacks = append(callbacks, func() {
				t.Log("Injecting finalized vote for round", msg.FinalizeVote.Finalization.Round, msg.FinalizeVote.Finalization.Digest)
				err := e.HandleMessage(&msg, node)
				require.NoError(t, err)
			})
		}
	}
	return callbacks
}

func TestEpochBlockSentTwice(t *testing.T) {
	l := testutil.MakeLogger(t, 1)

	var tooFarMsg, alreadyReceivedMsg bool

	l.Intercept(func(entry zapcore.Entry) error {
		if entry.Message == "Got block of a future round" {
			tooFarMsg = true
		}

		if entry.Message == "Already received a proposal from this node for the round" {
			alreadyReceivedMsg = true
		}

		return nil
	})

	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	wal := newTestWAL(t)

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[1],
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

	md := e.Metadata()
	md.Round = 2

	b, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)

	block := b.(Block)

	vote, err := newTestVote(block, nodes[2])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[2])
	require.NoError(t, err)

	wal.assertWALSize(0)
	require.True(t, tooFarMsg)

	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[2])
	require.NoError(t, err)

	wal.assertWALSize(0)
	require.True(t, alreadyReceivedMsg)

}

func TestEpochQCSignedByNonExistentNodes(t *testing.T) {
	l := testutil.MakeLogger(t, 1)

	var wg sync.WaitGroup
	wg.Add(6)

	//defer wg.Wait()

	unknownNotarizationChan := make(chan struct{})
	unknownEmptyNotarizationChan := make(chan struct{})
	unknownFinalizationChan := make(chan struct{})
	doubleNotarizationChan := make(chan struct{})
	doubleEmptyNotarizationChan := make(chan struct{})
	doubleFinalizationChan := make(chan struct{})

	callbacks := map[string]func(){
		"Notarization quorum certificate contains an unknown signer": func() {
			wg.Done()
			close(unknownNotarizationChan)
		},
		"Empty notarization quorum certificate contains an unknown signer": func() {
			wg.Done()
			close(unknownEmptyNotarizationChan)
		},
		"Finalization Quorum Certificate contains an unknown signer": func() {
			wg.Done()
			close(unknownFinalizationChan)
		},
		"A node has signed the notarization twice": func() {
			wg.Done()
			close(doubleNotarizationChan)
		},
		"A node has signed the empty notarization twice": func() {
			wg.Done()
			close(doubleEmptyNotarizationChan)
		},
		"Finalization signed twice by the same node": func() {
			wg.Done()
			close(doubleFinalizationChan)
		},
	}

	l.Intercept(func(entry zapcore.Entry) error {
		for key, f := range callbacks {
			if strings.Contains(entry.Message, key) {
				f()
			}
		}
		return nil
	})

	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	wal := newTestWAL(t)

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
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

	block := <-bb.out

	wal.assertWALSize(1)

	t.Run("notarization with unknown signer isn't taken into account", func(t *testing.T) {
		notarization, err := newNotarization(l, &testSignatureAggregator{}, block, []NodeID{{2}, {3}, {5}})
		require.NoError(t, err)

		err = e.HandleMessage(&Message{
			Notarization: &notarization,
		}, nodes[1])
		require.NoError(t, err)

		time.Sleep(time.Second)
		rawWAL, err := wal.WriteAheadLog.ReadAll()
		require.NoError(t, err)
		fmt.Println(">>>", len(rawWAL))

		wal.assertWALSize(1)
	})

	t.Run("notarization with double signer isn't taken into account", func(t *testing.T) {
		notarization, err := newNotarization(l, &testSignatureAggregator{}, block, []NodeID{{2}, {3}, {2}})
		require.NoError(t, err)

		err = e.HandleMessage(&Message{
			Notarization: &notarization,
		}, nodes[1])
		require.NoError(t, err)

		wal.assertWALSize(1)
	})

	t.Run("empty notarization with unknown signer isn't taken into account", func(t *testing.T) {
		var qc testQC
		for i, n := range []NodeID{{2}, {3}, {5}} {
			qc = append(qc, Signature{Signer: n, Value: []byte{byte(i)}})
		}

		err = e.HandleMessage(&Message{
			EmptyNotarization: &EmptyNotarization{
				Vote: ToBeSignedEmptyVote{ProtocolMetadata: ProtocolMetadata{
					Round: 0,
					Seq:   0,
				}},
				QC: qc,
			},
		}, nodes[1])
		require.NoError(t, err)

		wal.assertWALSize(1)
	})

	t.Run("empty notarization with double signer isn't taken into account", func(t *testing.T) {
		var qc testQC
		for i, n := range []NodeID{{2}, {3}, {2}} {
			qc = append(qc, Signature{Signer: n, Value: []byte{byte(i)}})
		}

		err = e.HandleMessage(&Message{
			EmptyNotarization: &EmptyNotarization{
				Vote: ToBeSignedEmptyVote{ProtocolMetadata: ProtocolMetadata{
					Round: 0,
					Seq:   0,
				}},
				QC: qc,
			},
		}, nodes[1])
		require.NoError(t, err)

		wal.assertWALSize(1)
	})

	t.Run("finalization with unknown signer isn't taken into account", func(t *testing.T) {
		finalization, _ := newFinalizationRecord(t, l, &testSignatureAggregator{}, block, []NodeID{{2}, {3}, {5}})

		err = e.HandleMessage(&Message{
			Finalization: &finalization,
		}, nodes[1])
		require.NoError(t, err)

		storage.ensureNoBlockCommit(t, 0)
	})

	t.Run("finalization with double signer isn't taken into account", func(t *testing.T) {
		finalization, _ := newFinalizationRecord(t, l, &testSignatureAggregator{}, block, []NodeID{{2}, {3}, {3}})

		err = e.HandleMessage(&Message{
			Finalization: &finalization,
		}, nodes[1])
		require.NoError(t, err)

		storage.ensureNoBlockCommit(t, 0)
	})
}

func TestEpochBlockSentFromNonLeader(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	nonLeaderMessage := false

	l.Intercept(func(entry zapcore.Entry) error {
		if entry.Message == "Got block from a block proposer that is not the leader of the round" {
			nonLeaderMessage = true
		}
		return nil
	})

	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()
	wal := newTestWAL(t)
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[1],
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

	md := e.Metadata()
	b, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)

	block := b.(Block)

	notLeader := nodes[3]
	vote, err := newTestVote(block, notLeader)
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, notLeader)
	require.NoError(t, err)
	require.True(t, nonLeaderMessage)
	records, err := wal.WriteAheadLog.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 0)
}

func TestEpochBlockTooHighRound(t *testing.T) {
	l := testutil.MakeLogger(t, 1)

	var rejectedBlock bool

	l.Intercept(func(entry zapcore.Entry) error {
		if entry.Message == "Received a block message for a too high round" {
			rejectedBlock = true
		}
		return nil
	})

	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()

	wal := newTestWAL(t)

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodes[1],
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

	t.Run("block from higher round is rejected", func(t *testing.T) {
		defer func() {
			rejectedBlock = false
		}()

		md := e.Metadata()
		md.Round = math.MaxUint64 - 3

		b, ok := bb.BuildBlock(context.Background(), md)
		require.True(t, ok)

		block := b.(Block)

		vote, err := newTestVote(block, nodes[0])
		require.NoError(t, err)
		err = e.HandleMessage(&Message{
			BlockMessage: &BlockMessage{
				Vote:  *vote,
				Block: block,
			},
		}, nodes[0])
		require.NoError(t, err)
		require.True(t, rejectedBlock)

		wal.assertWALSize(0)
	})

	t.Run("block is accepted", func(t *testing.T) {
		defer func() {
			rejectedBlock = false
		}()

		md := e.Metadata()
		b, ok := bb.BuildBlock(context.Background(), md)
		require.True(t, ok)

		block := b.(Block)

		vote, err := newTestVote(block, nodes[0])
		require.NoError(t, err)
		err = e.HandleMessage(&Message{
			BlockMessage: &BlockMessage{
				Vote:  *vote,
				Block: block,
			},
		}, nodes[0])
		require.NoError(t, err)
		require.False(t, rejectedBlock)

		wal.assertWALSize(1)
	})
}

// TestMetadataProposedRound ensures the metadata only builds off blocks
// with finalizations or notarizations
func TestMetadataProposedRound(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	storage := newInMemStorage()
	wal := newTestWAL(t)
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
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

	// assert the proposed block was written to the wal
	wal.assertWALSize(1)
	require.Zero(t, e.Metadata().Round)
	require.Zero(t, e.Metadata().Seq)
}

type AnyBlock interface {
	// BlockHeader encodes a succinct and collision-free representation of a block.
	BlockHeader() BlockHeader
}

func newTestVote(block AnyBlock, id NodeID) (*Vote, error) {
	vote := ToBeSignedVote{
		BlockHeader: block.BlockHeader(),
	}
	sig, err := vote.Sign(&testSigner{})
	if err != nil {
		return nil, err
	}

	return &Vote{
		Signature: Signature{
			Signer: id,
			Value:  sig,
		},
		Vote: vote,
	}, nil
}

func injectTestVote(t *testing.T, e *Epoch, block VerifiedBlock, id NodeID) {
	vote, err := newTestVote(block, id)
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		VoteMessage: vote,
	}, id)
	require.NoError(t, err)
}

func newTestFinalizeVote(t *testing.T, block VerifiedBlock, id NodeID) *FinalizeVote {
	f := ToBeSignedFinalization{BlockHeader: block.BlockHeader()}
	sig, err := f.Sign(&testSigner{})
	require.NoError(t, err)
	return &FinalizeVote{
		Signature: Signature{
			Signer: id,
			Value:  sig,
		},
		Finalization: ToBeSignedFinalization{
			BlockHeader: block.BlockHeader(),
		},
	}
}

func injectTestFinalization(t *testing.T, e *Epoch, finalization *Finalization, from NodeID) {
	err := e.HandleMessage(&Message{
		Finalization: finalization,
	}, from)
	require.NoError(t, err)
}

func injectTestFinalizeVote(t *testing.T, e *Epoch, block VerifiedBlock, id NodeID) {
	err := e.HandleMessage(&Message{
		FinalizeVote: newTestFinalizeVote(t, block, id),
	}, id)
	require.NoError(t, err)
}

func injectTestNotarization(t *testing.T, e *Epoch, notarization Notarization, id NodeID) {
	err := e.HandleMessage(&Message{
		Notarization: &notarization,
	}, id)
	require.NoError(t, err)
}

type testQCDeserializer struct {
	t *testing.T
}

func (t *testQCDeserializer) DeserializeQuorumCertificate(bytes []byte) (QuorumCertificate, error) {
	var qc []Signature
	rest, err := asn1.Unmarshal(bytes, &qc)
	require.NoError(t.t, err)
	require.Empty(t.t, rest)
	return testQC(qc), err
}

type testSignatureAggregator struct {
	err error
}

func (t *testSignatureAggregator) Aggregate(signatures []Signature) (QuorumCertificate, error) {
	return testQC(signatures), t.err
}

type testQC []Signature

func (t testQC) Signers() []NodeID {
	res := make([]NodeID, 0, len(t))
	for _, sig := range t {
		res = append(res, sig.Signer)
	}
	return res
}

func (t testQC) Verify(msg []byte) error {
	return nil
}

func (t testQC) Bytes() []byte {
	bytes, err := asn1.Marshal(t)
	if err != nil {
		panic(err)
	}
	return bytes
}

type testSigner struct {
}

func (t *testSigner) Sign([]byte) ([]byte, error) {
	return []byte{1, 2, 3}, nil
}

type testVerifier struct {
}

func (t *testVerifier) VerifyBlock(VerifiedBlock) error {
	return nil
}

func (t *testVerifier) Verify(_ []byte, _ []byte, _ NodeID) error {
	return nil
}

type noopComm []NodeID

func (n noopComm) ListNodes() []NodeID {
	return n
}

func (n noopComm) SendMessage(*Message, NodeID) {

}

func (n noopComm) Broadcast(msg *Message) {

}

// ListnerComm is a comm that listens for incoming messages
// and sends them to the [in] channel
type listnerComm struct {
	noopComm
	in chan *Message
}

func NewListenerComm(nodeIDs []NodeID) *listnerComm {
	return &listnerComm{
		noopComm: noopComm(nodeIDs),
		in:       make(chan *Message, 1),
	}
}

func (b *listnerComm) SendMessage(msg *Message, id NodeID) {
	b.in <- msg
}

type testBlockBuilder struct {
	out                chan *testBlock
	in                 chan *testBlock
	blockShouldBeBuilt chan struct{}
}

// BuildBlock builds a new testblock and sends it to the BlockBuilder channel
func (t *testBlockBuilder) BuildBlock(_ context.Context, metadata ProtocolMetadata) (VerifiedBlock, bool) {
	if len(t.in) > 0 {
		block := <-t.in
		return block, true
	}

	tb := newTestBlock(metadata)

	select {
	case t.out <- tb:
	default:
	}

	return tb, true
}

func (t *testBlockBuilder) IncomingBlock(ctx context.Context) {
	select {
	case <-t.blockShouldBeBuilt:
	case <-ctx.Done():
	}
}

type testBlock struct {
	data              []byte
	metadata          ProtocolMetadata
	digest            [32]byte
	onVerify          func()
	verificationDelay chan struct{}
}

func (tb *testBlock) Verify(context.Context) (VerifiedBlock, error) {
	defer func() {
		if tb.onVerify != nil {
			tb.onVerify()
		}
	}()
	if tb.verificationDelay == nil {
		return tb, nil
	}

	<-tb.verificationDelay

	return tb, nil
}

func newTestBlock(metadata ProtocolMetadata) *testBlock {
	tb := testBlock{
		metadata: metadata,
		data:     make([]byte, 32),
	}

	_, err := rand.Read(tb.data)
	if err != nil {
		panic(err)
	}

	tb.computeDigest()

	return &tb
}

func (tb *testBlock) computeDigest() {
	var bb bytes.Buffer
	tbBytes, err := tb.Bytes()
	if err != nil {
		panic(fmt.Sprintf("failed to serialize test block: %v", err))
	}

	bb.Write(tbBytes)
	tb.digest = sha256.Sum256(bb.Bytes())
}

func (t *testBlock) BlockHeader() BlockHeader {
	return BlockHeader{
		ProtocolMetadata: t.metadata,
		Digest:           t.digest,
	}
}

func (t *testBlock) Bytes() ([]byte, error) {
	bh := BlockHeader{
		ProtocolMetadata: t.metadata,
	}

	mdBuff := bh.Bytes()

	buff := make([]byte, len(t.data)+len(mdBuff)+4)
	binary.BigEndian.PutUint32(buff, uint32(len(t.data)))
	copy(buff[4:], t.data)
	copy(buff[4+len(t.data):], mdBuff)
	return buff, nil
}

type InMemStorage struct {
	data map[uint64]struct {
		VerifiedBlock
		Finalization
	}

	lock   sync.Mutex
	signal sync.Cond
}

func newInMemStorage() *InMemStorage {
	s := &InMemStorage{
		data: make(map[uint64]struct {
			VerifiedBlock
			Finalization
		}),
	}

	s.signal = *sync.NewCond(&s.lock)

	return s
}

func (mem *InMemStorage) Clone() *InMemStorage {
	clone := newInMemStorage()
	mem.lock.Lock()
	height := mem.Height()
	mem.lock.Unlock()
	for seq := uint64(0); seq < height; seq++ {
		mem.lock.Lock()
		block, finalization, ok := mem.Retrieve(seq)
		if !ok {
			panic(fmt.Sprintf("failed retrieving block %d", seq))
		}
		mem.lock.Unlock()
		clone.Index(block, finalization)
	}
	return clone
}

func (mem *InMemStorage) waitForBlockCommit(seq uint64) VerifiedBlock {
	mem.lock.Lock()
	defer mem.lock.Unlock()

	for {
		if data, exists := mem.data[seq]; exists {
			return data.VerifiedBlock
		}

		mem.signal.Wait()
	}
}

func (mem *InMemStorage) ensureNoBlockCommit(t *testing.T, seq uint64) {
	require.Never(t, func() bool {
		mem.lock.Lock()
		defer mem.lock.Unlock()

		_, exists := mem.data[seq]
		return exists
	}, time.Second, time.Millisecond*100, "block %d has been committed but shouldn't have been", seq)
}

func (mem *InMemStorage) Height() uint64 {
	return uint64(len(mem.data))
}

func (mem *InMemStorage) Retrieve(seq uint64) (VerifiedBlock, Finalization, bool) {
	item, ok := mem.data[seq]
	if !ok {
		return nil, Finalization{}, false
	}
	return item.VerifiedBlock, item.Finalization, true
}

func (mem *InMemStorage) Index(block VerifiedBlock, certificate Finalization) {
	mem.lock.Lock()
	defer mem.lock.Unlock()

	seq := block.BlockHeader().Seq

	_, ok := mem.data[seq]
	if ok {
		panic(fmt.Sprintf("block with seq %d already indexed!", seq))
	}
	mem.data[seq] = struct {
		VerifiedBlock
		Finalization
	}{block,
		certificate,
	}

	mem.signal.Signal()
}

type blockDeserializer struct {
}

func (b *blockDeserializer) DeserializeBlock(buff []byte) (VerifiedBlock, error) {
	blockLen := binary.BigEndian.Uint32(buff[:4])
	bh := BlockHeader{}
	if err := bh.FromBytes(buff[4+blockLen:]); err != nil {
		return nil, err
	}

	tb := testBlock{
		data:     buff[4 : 4+blockLen],
		metadata: bh.ProtocolMetadata,
	}

	tb.computeDigest()

	return &tb, nil
}

func TestBlockDeserializer(t *testing.T) {
	var blockDeserializer blockDeserializer

	tb := newTestBlock(ProtocolMetadata{Seq: 1, Round: 2, Epoch: 3})
	tbBytes, err := tb.Bytes()
	require.NoError(t, err)
	tb2, err := blockDeserializer.DeserializeBlock(tbBytes)
	require.NoError(t, err)
	require.Equal(t, tb, tb2)
}

func TestQuorum(t *testing.T) {
	for _, testCase := range []struct {
		n int
		f int
		q int
	}{
		{
			n: 1, f: 0,
			q: 1,
		},
		{
			n: 2, f: 0,
			q: 2,
		},
		{
			n: 3, f: 0,
			q: 2,
		},
		{
			n: 4, f: 1,
			q: 3,
		},
		{
			n: 5, f: 1,
			q: 4,
		},
		{
			n: 6, f: 1,
			q: 4,
		},
		{
			n: 7, f: 2,
			q: 5,
		},
		{
			n: 8, f: 2,
			q: 6,
		},
		{
			n: 9, f: 2,
			q: 6,
		},
		{
			n: 10, f: 3,
			q: 7,
		},
		{
			n: 11, f: 3,
			q: 8,
		},
		{
			n: 12, f: 3,
			q: 8,
		},
	} {
		t.Run(fmt.Sprintf("%d", testCase.n), func(t *testing.T) {
			require.Equal(t, testCase.q, Quorum(testCase.n))
		})
	}
}

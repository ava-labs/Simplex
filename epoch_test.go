// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	rand2 "math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	. "github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/record"
	"github.com/ava-labs/simplex/testutil"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

var (
	emptyBlacklist = Blacklist{
		NodeCount:      4,
		SuspectedNodes: SuspectedNodes{},
		Updates:        []BlacklistUpdate{},
	}
)

func TestEpochHandleNotarizationFutureRound(t *testing.T) {
	bb := &testutil.TestBlockBuilder{}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	// Create the two blocks ahead of time
	blocks := createBlocks(t, nodes, 2)
	firstBlock := blocks[0].VerifiedBlock.(*testutil.TestBlock)
	secondBlock := blocks[1].VerifiedBlock.(*testutil.TestBlock)
	bb.Out = make(chan *testutil.TestBlock, 1)
	bb.In = make(chan *testutil.TestBlock, 1)

	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)
	quorum := Quorum(len(nodes))

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	// Load the first block into the block builder, so it will not create its own block but use the pre-built one.
	// Drain the out channel before loading it
	//for len(bb.out) > 0 {
	//	<-bb.out
	//}
	bb.In <- firstBlock
	bb.Out <- firstBlock

	// Create a notarization for round 1 which is a future round because we haven't gone through round 0 yet.
	notarization, err := testutil.NewNotarization(conf.Logger, conf.SignatureAggregator, secondBlock, nodes)
	require.NoError(t, err)

	// Give the node the notarization message before receiving the first block
	e.HandleMessage(&Message{
		Notarization: &notarization,
	}, nodes[1])

	// Run through round 0
	notarizeAndFinalizeRound(t, e, bb)

	// Emulate round 1 by sending the block
	vote, err := testutil.NewTestVote(secondBlock, nodes[1])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: secondBlock,
		},
	}, nodes[1])
	require.NoError(t, err)

	// The node should store the notarization of the second block once it gets the block.
	wal.AssertNotarization(1)

	for i := 1; i < quorum; i++ {
		testutil.InjectTestFinalizeVote(t, e, secondBlock, nodes[i])
	}

	blockCommitted := storage.WaitForBlockCommit(1)
	require.Equal(t, secondBlock, blockCommitted)
}

// TestEpochIndexFinalization ensures that we properly index past finalizations when
// there have been empty rounds
func TestEpochIndexFinalization(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, _, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)
	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())
	firstBlock, _ := advanceRoundFromNotarization(t, e, bb)
	advanceRoundFromFinalization(t, e, bb)

	require.Equal(t, uint64(2), e.Metadata().Round)
	require.Equal(t, uint64(2), e.Metadata().Seq)
	require.Equal(t, uint64(0), e.Storage.NumBlocks())

	advanceRoundFromEmpty(t, e)
	require.Equal(t, uint64(3), e.Metadata().Round)
	require.Equal(t, uint64(2), e.Metadata().Seq)
	require.Equal(t, uint64(0), e.Storage.NumBlocks())

	advanceRoundFromFinalization(t, e, bb)
	require.Equal(t, uint64(4), e.Metadata().Round)
	require.Equal(t, uint64(3), e.Metadata().Seq)
	require.Equal(t, uint64(0), e.Storage.NumBlocks())

	// at this point we are waiting on finalization of seq 0.
	// when we receive that finalization, we should commit the rest of the finalizations for seqs
	// 1 & 2

	finalization, _ := testutil.NewFinalizationRecord(t, conf.Logger, conf.SignatureAggregator, firstBlock, e.Comm.Nodes())
	testutil.InjectTestFinalization(t, e, &finalization, nodes[1])

	storage.WaitForBlockCommit(2)
}

func TestEpochConsecutiveProposalsDoNotGetVerified(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{
			name: "valid block",
		},
		{
			name: "invalid block",
			err:  fmt.Errorf("invalid block"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
			nodes := []NodeID{{1}, {2}, {3}, {4}}

			conf, _, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[1], testutil.NewNoopComm(nodes), bb)

			e, err := NewEpoch(conf)
			require.NoError(t, err)

			require.NoError(t, e.Start())

			leader := nodes[0]

			md := e.Metadata()
			_, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
			require.True(t, ok)
			require.Equal(t, md.Round, md.Seq)

			onlyVerifyOnce := make(chan struct{})
			block := <-bb.Out
			block.OnVerify = func() {
				close(onlyVerifyOnce)
			}
			block.VerificationError = test.err

			vote, err := testutil.NewTestVote(block, leader)
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
		})
	}
}

// TestEpochIncreasesRoundAfterFinalization ensures that the epochs round is incremented
// if we receive a finalization for the current round(even if it is not the next seq to commit)
func TestEpochIncreasesRoundAfterFinalization(t *testing.T) {
	l := testutil.MakeLogger(t, 1)

	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}, {5}, {6}}

	conf, _, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[2], testutil.NewNoopComm(nodes), bb)
	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	block, _ := advanceRoundFromNotarization(t, e, bb)
	advanceRoundFromFinalization(t, e, bb)
	require.Equal(t, uint64(2), e.Metadata().Round)
	require.Equal(t, uint64(0), storage.NumBlocks())

	// create the finalized block
	finalization, _ := testutil.NewFinalizationRecord(t, l, conf.SignatureAggregator, block, nodes)
	testutil.InjectTestFinalization(t, e, &finalization, nodes[1])

	storage.WaitForBlockCommit(1)
	require.Equal(t, uint64(2), e.Metadata().Round)
	require.Equal(t, uint64(2), storage.NumBlocks())

	// we are the leader, ensure we can continue & propose a block
	notarizeAndFinalizeRound(t, e, bb)
}

func TestEpochNotarizeTwiceThenFinalize(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}

	nodes := []NodeID{{1}, {2}, {3}, {4}}

	recordedMessages := make(chan *Message, 100)
	comm := &recordingComm{Communication: testutil.NewNoopComm(nodes), BroadcastMessages: recordedMessages}

	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], comm, bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	// Round 0
	block0 := <-bb.Out

	testutil.InjectTestVote(t, e, block0, nodes[1])
	testutil.InjectTestVote(t, e, block0, nodes[2])
	wal.AssertNotarization(0)

	// Round 1
	emptyNote := testutil.NewEmptyNotarization(nodes, 1)
	err = e.HandleMessage(&Message{
		EmptyNotarization: emptyNote,
	}, nodes[1])
	require.NoError(t, err)
	emptyRecord := wal.AssertNotarization(1)
	require.Equal(t, record.EmptyNotarizationRecordType, emptyRecord)

	// Round 2
	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
	require.True(t, ok)
	block1 := <-bb.Out

	vote, err := testutil.NewTestVote(block1, nodes[2])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block1,
		},
	}, nodes[1])
	require.NoError(t, err)

	testutil.InjectTestVote(t, e, block1, nodes[3])
	wal.AssertNotarization(2)

	// Round 3
	md = e.Metadata()
	_, ok = bb.BuildBlock(context.Background(), md, emptyBlacklist)
	require.True(t, ok)
	block2 := <-bb.Out

	vote, err = testutil.NewTestVote(block2, nodes[3])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block2,
		},
	}, nodes[3])
	require.NoError(t, err)

	testutil.InjectTestVote(t, e, block2, nodes[2])
	wal.AssertNotarization(3)
	require.Equal(t, uint64(0), storage.NumBlocks())

	// drain the recorded messages
	for len(recordedMessages) > 0 {
		<-recordedMessages
	}

	blocks := make(map[uint64]*testutil.TestBlock)
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
						testutil.InjectTestFinalizeVote(t, e, block, nodes[1])
						testutil.InjectTestFinalizeVote(t, e, block, nodes[2])
					}
				}
			}
		}
	}()

	testutil.InjectTestFinalizeVote(t, e, block2, nodes[1])
	testutil.InjectTestFinalizeVote(t, e, block2, nodes[2])

	storage.WaitForBlockCommit(0)
	storage.WaitForBlockCommit(1)
	storage.WaitForBlockCommit(2)

	close(finish)
	wg.Wait()
}

func TestEpochFinalizeThenNotarize(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	quorum := Quorum(len(nodes))

	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)
	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	t.Run("commit without notarization, only with finalization", func(t *testing.T) {
		for round := 0; round < 100; round++ {
			advanceRoundFromFinalization(t, e, bb)
			storage.WaitForBlockCommit(uint64(round))
		}
	})

	t.Run("notarization after commit without notarizations", func(t *testing.T) {
		// leader is the proposer of the new block for the given round
		leader := LeaderForRound(nodes, uint64(100))
		// only create blocks if we are not the node running the epoch
		if !leader.Equals(e.ID) {
			md := e.Metadata()
			_, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
			require.True(t, ok)
		}

		block := <-bb.Out

		vote, err := testutil.NewTestVote(block, nodes[0])
		require.NoError(t, err)
		err = e.HandleMessage(&Message{
			BlockMessage: &BlockMessage{
				Vote:  *vote,
				Block: block,
			},
		}, nodes[1])
		require.NoError(t, err)

		for i := 1; i < quorum; i++ {
			testutil.InjectTestVote(t, e, block, nodes[i])
		}

		wal.AssertNotarization(100)
	})

}

func TestEpochSimpleFlow(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, _, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	rounds := uint64(100)
	for round := uint64(0); round < rounds; round++ {
		notarizeAndFinalizeRound(t, e, bb)
	}
}

func TestEpochStartedTwice(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, _, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())
	require.ErrorIs(t, e.Start(), ErrAlreadyStarted)
}


func advanceRoundFromEmpty(t *testing.T, e *Epoch) {
	leader := LeaderForRound(e.Comm.Nodes(), e.Metadata().Round)
	require.False(t, e.ID.Equals(leader), "epoch cannot be the leader for the empty round")

	emptyNote := testutil.NewEmptyNotarization(e.Comm.Nodes(), e.Metadata().Round)
	err := e.HandleMessage(&Message{
		EmptyNotarization: emptyNote,
	}, leader)

	require.NoError(t, err)

	emptyRecord := e.WAL.(*testutil.TestWAL).AssertNotarization(emptyNote.Vote.Round)
	require.Equal(t, record.EmptyNotarizationRecordType, emptyRecord)
}

func advanceRoundFromNotarization(t *testing.T, e *Epoch, bb *testutil.TestBlockBuilder) (VerifiedBlock, *Notarization) {
	return advanceRound(t, e, bb, true, false)
}

func advanceRoundFromFinalization(t *testing.T, e *Epoch, bb *testutil.TestBlockBuilder) VerifiedBlock {
	block, _ := advanceRound(t, e, bb, false, true)
	return block
}

func notarizeAndFinalizeRound(t *testing.T, e *Epoch, bb *testutil.TestBlockBuilder) (VerifiedBlock, *Notarization) {
	return advanceRound(t, e, bb, true, true)
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
	rounds := 10
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, rounds)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, _, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

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
		storage.WaitForBlockCommit(uint64(i))
	}
}

func createCallbacks(t *testing.T, rounds int, protocolMetadata ProtocolMetadata, nodes []NodeID, e *Epoch, bb *testutil.TestBlockBuilder) []func() {
	blocks := make([]VerifiedBlock, 0, rounds)

	callbacks := make([]func(), 0, rounds*4+len(blocks))

	for i := 0; i < rounds; i++ {
		block := testutil.NewTestBlock(protocolMetadata, emptyBlacklist)
		blocks = append(blocks, block)

		protocolMetadata.Seq++
		protocolMetadata.Round++
		protocolMetadata.Prev = block.BlockHeader().Digest

		leader := LeaderForRound(nodes, uint64(i))

		if !leader.Equals(e.ID) {
			vote, err := testutil.NewTestVote(block, leader)
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
			bb.Out <- block
		}

		for j := 1; j <= 2; j++ {
			node := nodes[j]
			vote, err := testutil.NewTestVote(block, node)
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
			vote := testutil.NewTestFinalizeVote(t, block, node)
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
	var tooFarMsg, alreadyReceivedMsg bool

	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[1], testutil.NewNoopComm(nodes), bb)

	l := conf.Logger.(*testutil.TestLogger)

	l.Intercept(func(entry zapcore.Entry) error {
		if entry.Message == "Got block of a future round" {
			tooFarMsg = true
		}

		if entry.Message == "Already received a proposal from this node for the round" {
			alreadyReceivedMsg = true
		}

		return nil
	})

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	md := e.Metadata()
	md.Round = 2

	b, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
	require.True(t, ok)

	block := b.(Block)

	vote, err := testutil.NewTestVote(block, nodes[2])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[2])
	require.NoError(t, err)

	wal.AssertWALSize(0)
	require.True(t, tooFarMsg)

	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[2])
	require.NoError(t, err)

	wal.AssertWALSize(0)
	require.True(t, alreadyReceivedMsg)

}

func TestEpochQCSignedByNonExistentNodes(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(6)

	defer wg.Wait()

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
		"Finalization quorum certificate contains an unknown signer": func() {
			wg.Done()
			close(unknownFinalizationChan)
		},
		"Notarization is signed by the same node more than once": func() {
			wg.Done()
			close(doubleNotarizationChan)
		},
		"Empty notarization is signed by the same node more than once": func() {
			wg.Done()
			close(doubleEmptyNotarizationChan)
		},
		"Finalization is signed by the same node more than once": func() {
			wg.Done()
			close(doubleFinalizationChan)
		},
	}

	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)
	l := conf.Logger.(*testutil.TestLogger)
	l.Intercept(func(entry zapcore.Entry) error {
		for key, f := range callbacks {
			if strings.Contains(entry.Message, key) {
				f()
			}
		}
		return nil
	})

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	block := <-bb.Out

	wal.AssertWALSize(1)

	t.Run("notarization with unknown signer isn't taken into account", func(t *testing.T) {
		notarization, err := testutil.NewNotarization(conf.Logger, conf.SignatureAggregator, block, []NodeID{{2}, {3}, {5}})
		require.NoError(t, err)

		err = e.HandleMessage(&Message{
			Notarization: &notarization,
		}, nodes[1])
		require.NoError(t, err)

		time.Sleep(time.Second)

		wal.AssertWALSize(1)
	})

	t.Run("notarization with double signer isn't taken into account", func(t *testing.T) {
		notarization, err := testutil.NewNotarization(conf.Logger, conf.SignatureAggregator, block, []NodeID{{2}, {3}})
		require.NoError(t, err)

		tqc := notarization.QC.(testutil.TestQC)
		tqc = append(tqc, Signature{Signer: nodes[2], Value: []byte{0}})
		notarization.QC = tqc

		err = e.HandleMessage(&Message{
			Notarization: &notarization,
		}, nodes[1])
		require.NoError(t, err)

		wal.AssertWALSize(1)
	})

	t.Run("empty notarization with unknown signer isn't taken into account", func(t *testing.T) {
		var qc testutil.TestQC
		for i, n := range []NodeID{{2}, {3}, {5}} {
			qc = append(qc, Signature{Signer: n, Value: []byte{byte(i)}})
		}

		err = e.HandleMessage(&Message{
			EmptyNotarization: &EmptyNotarization{
				Vote: ToBeSignedEmptyVote{EmptyVoteMetadata: EmptyVoteMetadata{
					Round: 0,
					Epoch: 0,
				}},
				QC: qc,
			},
		}, nodes[1])
		require.NoError(t, err)

		wal.AssertWALSize(1)
	})

	t.Run("empty notarization with double signer isn't taken into account", func(t *testing.T) {
		var qc testutil.TestQC
		for i, n := range []NodeID{{2}, {3}, {2}} {
			qc = append(qc, Signature{Signer: n, Value: []byte{byte(i)}})
		}

		err = e.HandleMessage(&Message{
			EmptyNotarization: &EmptyNotarization{
				Vote: ToBeSignedEmptyVote{EmptyVoteMetadata: EmptyVoteMetadata{
					Round: 0,
					Epoch: 0,
				}},
				QC: qc,
			},
		}, nodes[1])
		require.NoError(t, err)

		wal.AssertWALSize(1)
	})

	t.Run("finalization with unknown signer isn't taken into account", func(t *testing.T) {
		finalization, _ := testutil.NewFinalizationRecord(t, conf.Logger, conf.SignatureAggregator, block, []NodeID{{2}, {3}, {5}})

		err = e.HandleMessage(&Message{
			Finalization: &finalization,
		}, nodes[1])
		require.NoError(t, err)

		storage.EnsureNoBlockCommit(t, 0)
	})

	t.Run("finalization with double signer isn't taken into account", func(t *testing.T) {
		finalization, _ := testutil.NewFinalizationRecord(t, conf.Logger, conf.SignatureAggregator, block, []NodeID{{2}, {3}, {3}})

		err = e.HandleMessage(&Message{
			Finalization: &finalization,
		}, nodes[1])
		require.NoError(t, err)

		storage.EnsureNoBlockCommit(t, 0)
	})
}

func TestEpochBlockSentFromNonLeader(t *testing.T) {
	nonLeaderMessage := false

	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[1], testutil.NewNoopComm(nodes), bb)
	l := conf.Logger.(*testutil.TestLogger)
	l.Intercept(func(entry zapcore.Entry) error {
		if entry.Message == "Got block from a block proposer that is not the leader of the round" {
			nonLeaderMessage = true
		}
		return nil
	})

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	md := e.Metadata()
	b, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
	require.True(t, ok)

	block := b.(Block)

	notLeader := nodes[3]
	vote, err := testutil.NewTestVote(block, notLeader)
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
	var rejectedBlock bool

	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[1], testutil.NewNoopComm(nodes), bb)

	l := conf.Logger.(*testutil.TestLogger)
	l.Intercept(func(entry zapcore.Entry) error {
		if entry.Message == "Received a block message for a too high round" {
			rejectedBlock = true
		}
		return nil
	})

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	t.Run("block from higher round is rejected", func(t *testing.T) {
		defer func() {
			rejectedBlock = false
		}()

		md := e.Metadata()
		md.Round = math.MaxUint64 - 3

		b, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
		require.True(t, ok)

		block := b.(Block)

		vote, err := testutil.NewTestVote(block, nodes[0])
		require.NoError(t, err)
		err = e.HandleMessage(&Message{
			BlockMessage: &BlockMessage{
				Vote:  *vote,
				Block: block,
			},
		}, nodes[0])
		require.NoError(t, err)
		require.True(t, rejectedBlock)

		wal.AssertWALSize(0)
	})

	t.Run("block is accepted", func(t *testing.T) {
		defer func() {
			rejectedBlock = false
		}()

		md := e.Metadata()
		b, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
		require.True(t, ok)

		block := b.(Block)

		vote, err := testutil.NewTestVote(block, nodes[0])
		require.NoError(t, err)
		err = e.HandleMessage(&Message{
			BlockMessage: &BlockMessage{
				Vote:  *vote,
				Block: block,
			},
		}, nodes[0])
		require.NoError(t, err)
		require.False(t, rejectedBlock)

		wal.AssertWALSize(1)
	})
}

// TestMetadataProposedRound ensures the metadata only builds off blocks
// with finalizations or notarizations
func TestMetadataProposedRound(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	// assert the proposed block was written to the wal
	wal.AssertWALSize(1)
	require.Zero(t, e.Metadata().Round)
	require.Zero(t, e.Metadata().Seq)
}

func TestEpochVotesForEquivocatedVotes(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	recordedMessages := make(chan *Message, 100)
	comm := &recordingComm{Communication: testutil.NewNoopComm(nodes), BroadcastMessages: recordedMessages}
	conf, _, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[3], comm, bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md, emptyBlacklist)
	require.True(t, ok)

	block := <-bb.Out

	// the leader and this node are sending the votes for the same block
	leader := nodes[0]
	vote, err := testutil.NewTestVote(block, leader)
	require.NoError(t, err)

	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, leader)
	require.NoError(t, err)

	// node 1 sends a vote for a different block
	equivocatedBlock := testutil.NewTestBlock(block.Metadata, emptyBlacklist)
	equivocatedBlock.Data = []byte{1, 2, 3}
	equivocatedBlock.ComputeDigest()
	testutil.InjectTestVote(t, e, equivocatedBlock, nodes[1])
	eqbh := equivocatedBlock.BlockHeader()

	// We should not have sent a notarization yet, since we have not received enough votes for the block we received from the leader
	require.Never(t, func() bool {
		select {
		case msg := <-recordedMessages:
			if msg.Notarization != nil {
				fmt.Println(msg.Notarization.Vote.BlockHeader.Equals(&eqbh))
				return true
			}
		default:
			return false
		}
		return false
	}, time.Millisecond*500, time.Millisecond*100)

	// node 2 sends a vote for the same block as the leader
	testutil.InjectTestVote(t, e, block, nodes[2])

	// Wait for the notarization to be sent
	timeout := time.After(time.Minute)
	var notarization *Notarization
	for notarization == nil {
		select {
		case msg := <-recordedMessages:
			if msg.Notarization != nil {
				notarization = msg.Notarization
			}
		case <-timeout:
			require.Fail(t, "timed out waiting for notarization")
		}
	}

	for _, signer := range notarization.QC.(testutil.TestQC).Signers() {
		require.NotEqual(t, nodes[1], signer, "Node 1 should not be in the notarization QC")
	}
}

// ListnerComm is a comm that listens for incoming messages
// and sends them to the [in] channel
type listenerComm struct {
	testutil.NoopComm
	in chan *Message
}

func NewListenerComm(nodeIDs []NodeID) *listenerComm {
	return &listenerComm{
		NoopComm: testutil.NewNoopComm(nodeIDs),
		in:       make(chan *Message, 1),
	}
}

func (b *listenerComm) Send(msg *Message, id NodeID) {
	b.in <- msg
}


// garbageCollectSuspectedNodes progresses [e] to a new round. If [notarize] is set, the round will progress due to a notarization.
// If [finalize] is set, the round will advance and the block will be indexed to storage.
func advanceRoundWithMD(t *testing.T, e *simplex.Epoch, bb *testutil.TestBlockBuilder, notarize bool, finalize bool, md simplex.ProtocolMetadata) (simplex.VerifiedBlock, *simplex.Notarization) {
	require.True(t, notarize || finalize, "must either notarize or finalize a round to advance")
	nextSeqToCommit := e.Storage.NumBlocks()
	nodes := e.Comm.Nodes()
	quorum := simplex.Quorum(len(nodes))
	// leader is the proposer of the new block for the given round
	leader := simplex.LeaderForRound(nodes, md.Round)
	// only create blocks if we are not the node running the epoch
	isEpochNode := leader.Equals(e.ID)
	if !isEpochNode {
		_, ok := bb.BuildBlock(context.Background(), md, simplex.Blacklist{
			NodeCount: uint16(len(e.EpochConfig.Comm.Nodes())),
		})
		require.True(t, ok)
	}

	block := <-bb.Out

	if !isEpochNode {
		// send node a message from the leader
		vote, err := testutil.NewTestVote(block, leader)
		require.NoError(t, err)
		err = e.HandleMessage(&simplex.Message{
			BlockMessage: &simplex.BlockMessage{
				Vote:  *vote,
				Block: block,
			},
		}, leader)
		require.NoError(t, err)
	}

	var notarization *simplex.Notarization
	if notarize {
		// start at one since our node has already voted
		n, err := testutil.NewNotarization(e.Logger, e.SignatureAggregator, block, nodes[0:quorum])
		testutil.InjectTestNotarization(t, e, n, nodes[1])

		e.WAL.(*testutil.TestWAL).AssertNotarization(block.Metadata.Round)
		require.NoError(t, err)
		notarization = &n
	}

	if finalize {
		for i := 0; i <= quorum; i++ {
			if nodes[i].Equals(e.ID) {
				continue
			}
			testutil.InjectTestFinalizeVote(t, e, block, nodes[i])
		}

		if nextSeqToCommit != block.Metadata.Seq {
			testutil.WaitToEnterRound(t, e, block.Metadata.Round+1)
			return block, notarization
		}

		blockFromStorage := e.Storage.(*testutil.InMemStorage).WaitForBlockCommit(block.Metadata.Seq)
		require.Equal(t, block, blockFromStorage)
	}

	return block, notarization
}


// garbageCollectSuspectedNodes progresses [e] to a new round. If [notarize] is set, the round will progress due to a notarization.
// If [finalize] is set, the round will advance and the block will be indexed to storage.
func advanceRound(t *testing.T, e *simplex.Epoch, bb *testutil.TestBlockBuilder, notarize bool, finalize bool) (simplex.VerifiedBlock, *simplex.Notarization) {
	require.True(t, notarize || finalize, "must either notarize or finalize a round to advance")
	nextSeqToCommit := e.Storage.NumBlocks()
	nodes := e.Comm.Nodes()
	quorum := simplex.Quorum(len(nodes))
	// leader is the proposer of the new block for the given round
	leader := simplex.LeaderForRound(nodes, e.Metadata().Round)
	// only create blocks if we are not the node running the epoch
	isEpochNode := leader.Equals(e.ID)
	if !isEpochNode {
		md := e.Metadata()
		_, ok := bb.BuildBlock(context.Background(), md, simplex.Blacklist{
			NodeCount: uint16(len(e.EpochConfig.Comm.Nodes())),
		})
		require.True(t, ok)
	}

	block := <-bb.Out

	if !isEpochNode {
		// send node a message from the leader
		vote, err := testutil.NewTestVote(block, leader)
		require.NoError(t, err)
		err = e.HandleMessage(&simplex.Message{
			BlockMessage: &simplex.BlockMessage{
				Vote:  *vote,
				Block: block,
			},
		}, leader)
		require.NoError(t, err)
	}

	var notarization *simplex.Notarization
	if notarize {
		// start at one since our node has already voted
		n, err := testutil.NewNotarization(e.Logger, e.SignatureAggregator, block, nodes[0:quorum])
		testutil.InjectTestNotarization(t, e, n, nodes[1])

		e.WAL.(*testutil.TestWAL).AssertNotarization(block.Metadata.Round)
		require.NoError(t, err)
		notarization = &n
	}

	if finalize {
		for i := 0; i <= quorum; i++ {
			if nodes[i].Equals(e.ID) {
				continue
			}
			testutil.InjectTestFinalizeVote(t, e, block, nodes[i])
		}

		if nextSeqToCommit != block.Metadata.Seq {
			testutil.WaitToEnterRound(t, e, block.Metadata.Round+1)
			return block, notarization
		}

		blockFromStorage := e.Storage.(*testutil.InMemStorage).WaitForBlockCommit(block.Metadata.Seq)
		require.Equal(t, block, blockFromStorage)
	}

	return block, notarization
}

func TestBlockDeserializer(t *testing.T) {
	var blockDeserializer testutil.BlockDeserializer

	ctx := context.Background()
	tb := testutil.NewTestBlock(ProtocolMetadata{Seq: 1, Round: 2, Epoch: 3}, emptyBlacklist)
	tbBytes, err := tb.Bytes()
	require.NoError(t, err)
	tb2, err := blockDeserializer.DeserializeBlock(ctx, tbBytes)
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

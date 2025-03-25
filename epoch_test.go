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
	. "simplex"
	"simplex/testutil"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestEpochHandleNotarizationFutureRound(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	bb := &testutil.TestBlockBuilder{}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	// Create the two blocks ahead of time
	blocks := createBlocks(t, nodes, bb, 2)
	firstBlock := blocks[0].VerifiedBlock.(*testutil.TestBlock)
	secondBlock := blocks[1].VerifiedBlock.(*testutil.TestBlock)
	bb.Out = make(chan *testutil.TestBlock, 1)
	bb.In = make(chan *testutil.TestBlock, 1)

	quorum := Quorum(len(nodes))
	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

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
	notarization, err := newNotarization(l, conf.SignatureAggregator, secondBlock, nodes)
	require.NoError(t, err)

	// Give the node the notarization message before receiving the first block
	e.HandleMessage(&Message{
		Notarization: &notarization,
	}, nodes[1])

	// Run through round 0
	notarizeAndFinalizeRound(t, e, bb)

	// Emulate round 1 by sending the block
	vote, err := newTestVote(secondBlock, nodes[1], e.Signer)
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
		injectTestFinalization(t, e, secondBlock, nodes[i])
	}

	blockCommitted := storage.WaitForBlockCommit(1)
	require.Equal(t, secondBlock, blockCommitted)
}

func TestEpochConsecutiveProposalsDoNotGetVerified(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, _, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	leader := nodes[0]

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	require.Equal(t, md.Round, md.Seq)

	onlyVerifyOnce := make(chan struct{})
	block := <-bb.Out
	block.OnVerify = func() {
		close(onlyVerifyOnce)
	}

	vote, err := newTestVote(block, leader, e.Signer)
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

func TestEpochNotarizeTwiceThenFinalize(t *testing.T) {
	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}

	recordedMessages := make(chan *Message, 100)
	comm := &testutil.RecordingComm{Communication: testutil.NewNoopComm(nodes), BroadcastMessages: recordedMessages}

	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], comm, bb)
	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	// Round 0
	block0 := <-bb.Out

	injectTestVote(t, e, block0, nodes[1])
	injectTestVote(t, e, block0, nodes[2])
	wal.AssertNotarization(0)

	// Round 1
	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	block1 := <-bb.Out

	vote, err := newTestVote(block1, nodes[1], e.Signer)
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block1,
		},
	}, nodes[1])
	require.NoError(t, err)

	injectTestVote(t, e, block1, nodes[2])

	wal.AssertNotarization(1)

	// Round 2
	md = e.Metadata()
	_, ok = bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	block2 := <-bb.Out

	vote, err = newTestVote(block2, nodes[2], e.Signer)
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block2,
		},
	}, nodes[2])
	require.NoError(t, err)

	injectTestVote(t, e, block2, nodes[1])

	wal.AssertNotarization(2)

	// drain the recorded messages
	for len(recordedMessages) > 0 {
		<-recordedMessages
	}

	blocks := []*testutil.TestBlock{block0, block1}

	var wg sync.WaitGroup
	wg.Add(1)

	finish := make(chan struct{})
	// Once the node sends a finalization message, send it finalization messages as a response
	go func() {
		defer wg.Done()
		for {
			select {
			case <-finish:
				return
			case msg := <-recordedMessages:
				if msg.Finalization != nil {
					index := msg.Finalization.Finalization.Round
					if index > 1 {
						continue
					}
					injectTestFinalization(t, e, blocks[int(index)], nodes[1])
					injectTestFinalization(t, e, blocks[int(index)], nodes[2])
				}
			}
		}
	}()

	injectTestFinalization(t, e, block2, nodes[1])
	injectTestFinalization(t, e, block2, nodes[2])

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
			_, ok := bb.BuildBlock(context.Background(), md)
			require.True(t, ok)
		}

		block := <-bb.Out

		vote, err := newTestVote(block, nodes[0], e.Signer)
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

// advanceRound progresses [e] to a new round. If [notarize] is set, the round will progress due to a notarization.
// If [finalize] is set, the round will advance and the block will be indexed to storage.
func advanceRound(t *testing.T, e *Epoch, bb *testutil.TestBlockBuilder, notarize bool, finalize bool) (VerifiedBlock, *Notarization) {
	require.True(t, notarize || finalize, "must either notarize or finalize a round to advance")
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

	block := <-bb.Out

	if !isEpochNode {
		// send node a message from the leader
		vote, err := newTestVote(block, leader, e.Signer)
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

		e.WAL.(*testutil.TestWAL).AssertNotarization(block.BlockHeader().Round)
		require.NoError(t, err)
		notarization = &n
	}

	if finalize {
		for i := 1; i <= quorum; i++ {
			injectTestFinalization(t, e, block, nodes[i])
		}
		blockFromStorage := e.Storage.(*testutil.InMemStorage).WaitForBlockCommit(block.BlockHeader().Seq)
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
	rounds := 10

	bb := &testutil.TestBlockBuilder{In: make(chan *testutil.TestBlock, rounds)}

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
		block := testutil.NewTestBlock(protocolMetadata)
		blocks = append(blocks, block)

		protocolMetadata.Seq++
		protocolMetadata.Round++
		protocolMetadata.Prev = block.BlockHeader().Digest

		leader := LeaderForRound(nodes, uint64(i))

		if !leader.Equals(e.ID) {
			vote, err := newTestVote(block, leader, e.Signer)
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
			bb.In <- block
		}

		for j := 1; j <= 2; j++ {
			node := nodes[j]
			vote, err := newTestVote(block, node, e.Signer)
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
			finalization := newTestFinalization(t, block, node, e.Signer)
			msg := Message{
				Finalization: finalization,
			}
			callbacks = append(callbacks, func() {
				t.Log("Injecting finalization for round", msg.Finalization.Finalization.Round, msg.Finalization.Finalization.Digest)
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

	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	md := e.Metadata()
	md.Round = 2

	b, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)

	block := b.(Block)

	vote, err := newTestVote(block, nodes[2], e.Signer)
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
		"Finalization certificate signed twice by the same node": func() {
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

	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, wal, storage := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	block := <-bb.Out

	wal.AssertWALSize(1)

	t.Run("notarization with unknown signer isn't taken into account", func(t *testing.T) {
		notarization, err := newNotarization(l, e.SignatureAggregator, block, []NodeID{{2}, {3}, {5}})
		require.NoError(t, err)

		err = e.HandleMessage(&Message{
			Notarization: &notarization,
		}, nodes[1])
		require.NoError(t, err)

		time.Sleep(time.Second)
		rawWAL, err := wal.WriteAheadLog.ReadAll()
		require.NoError(t, err)
		fmt.Println(">>>", len(rawWAL))

		wal.AssertWALSize(1)
	})

	t.Run("notarization with double signer isn't taken into account", func(t *testing.T) {
		notarization, err := newNotarization(l, e.SignatureAggregator, block, []NodeID{{2}, {3}, {2}})
		require.NoError(t, err)

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
				Vote: ToBeSignedEmptyVote{ProtocolMetadata: ProtocolMetadata{
					Round: 0,
					Seq:   0,
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
				Vote: ToBeSignedEmptyVote{ProtocolMetadata: ProtocolMetadata{
					Round: 0,
					Seq:   0,
				}},
				QC: qc,
			},
		}, nodes[1])
		require.NoError(t, err)

		wal.AssertWALSize(1)
	})

	t.Run("finalization certificate with unknown signer isn't taken into account", func(t *testing.T) {
		fCert, _ := newFinalizationRecord(t, l, e.SignatureAggregator, block, []NodeID{{2}, {3}, {5}})

		err = e.HandleMessage(&Message{
			FinalizationCertificate: &fCert,
		}, nodes[1])
		require.NoError(t, err)

		storage.EnsureNoBlockCommit(t, 0)
	})

	t.Run("finalization certificate with double signer isn't taken into account", func(t *testing.T) {
		fCert, _ := newFinalizationRecord(t, l, e.SignatureAggregator, block, []NodeID{{2}, {3}, {3}})

		err = e.HandleMessage(&Message{
			FinalizationCertificate: &fCert,
		}, nodes[1])
		require.NoError(t, err)

		storage.EnsureNoBlockCommit(t, 0)
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

	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	md := e.Metadata()
	b, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)

	block := b.(Block)

	notLeader := nodes[3]
	vote, err := newTestVote(block, notLeader, e.Signer)
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

	bb := &testutil.TestBlockBuilder{Out: make(chan *testutil.TestBlock, 1)}

	nodes := []NodeID{{1}, {2}, {3}, {4}}
	conf, wal, _ := testutil.DefaultTestNodeEpochConfig(t, nodes[0], testutil.NewNoopComm(nodes), bb)

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

		vote, err := newTestVote(block, nodes[0], e.Signer)
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
		b, ok := bb.BuildBlock(context.Background(), md)
		require.True(t, ok)

		block := b.(Block)

		vote, err := newTestVote(block, nodes[0], e.Signer)
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

type AnyBlock interface {
	// BlockHeader encodes a succinct and collision-free representation of a block.
	BlockHeader() BlockHeader
}

func newTestVote(block AnyBlock, id NodeID, signer Signer) (*Vote, error) {
	vote := ToBeSignedVote{
		BlockHeader: block.BlockHeader(),
	}
	sig, err := vote.Sign(signer)
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
	vote, err := newTestVote(block, id, e.Signer)
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		VoteMessage: vote,
	}, id)
	require.NoError(t, err)
}

func newTestFinalization(t *testing.T, block VerifiedBlock, id NodeID, signer Signer) *Finalization {
	f := ToBeSignedFinalization{BlockHeader: block.BlockHeader()}
	sig, err := f.Sign(signer)
	require.NoError(t, err)
	return &Finalization{
		Signature: Signature{
			Signer: id,
			Value:  sig,
		},
		Finalization: ToBeSignedFinalization{
			BlockHeader: block.BlockHeader(),
		},
	}
}

func injectTestFinalization(t *testing.T, e *Epoch, block VerifiedBlock, id NodeID) {
	err := e.HandleMessage(&Message{
		Finalization: newTestFinalization(t, block, id, e.Signer),
	}, id)
	require.NoError(t, err)
}

func injectTestNotarization(t *testing.T, e *Epoch, notarization Notarization, id NodeID) {
	err := e.HandleMessage(&Message{
		Notarization: &notarization,
	}, id)
	require.NoError(t, err)
}

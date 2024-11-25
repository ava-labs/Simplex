// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"time"
)

type phase uint8

const (
	undefined phase = iota
	finalized
	proposed
	notarized
	shutdown
)

type Epoch struct {
	// Config
	Logger       Logger
	ID           NodeID
	Comm         Communication
	WAL          WriteAheadLog
	BlockBuilder BlockBuilder
	Round        uint64
	Seq          uint64
	StartTime    time.Time
	// Runtime
	finishCtx  context.Context
	finishFn   context.CancelFunc
	phase      phase
	nodes      []NodeID
	round, seq uint64
	now        time.Time
}

// AdvanceTime hints the engine that the given amount of time has passed.
func (e *Epoch) AdvanceTime(t time.Duration) {

}

// HandleMessage notifies the engine about a reception of a message.
func (e *Epoch) HandleMessage(msg *Message) {

}

func (e *Epoch) Start() {
	e.finishCtx, e.finishFn = context.WithCancel(context.Background())
	e.phase = finalized // TODO: restore phase and state from the WAL
	e.nodes = e.Comm.ListNodes()
	e.round = e.Round
	e.seq = e.Seq
	e.now = e.StartTime

	e.doPhase()
}

func (e *Epoch) Stop() {
	e.finishFn()
	e.phase = shutdown
}

func (e *Epoch) doPhase() {
	switch e.phase {
	case finalized:
		e.doFinalized()
	case proposed:
		e.doProposed()
	case notarized:
		e.doNotarized()
	case shutdown:
		return
	case undefined:
		panic("programming error: phase is undefined")
	}
}

func (e *Epoch) proposeBlock() {
	block, ok := e.BlockBuilder.BuildBlock(e.finishCtx)
	if !ok {
		return
	}

	e.Comm.Broadcast(&Message{
		BlockMessage: BlockMessage{
			Block: block,
		},
	})

	e.WAL.Append(&Record{
		Payload: block.Bytes(),
	})
}

func (e *Epoch) doFinalized() {
	leaderForCurrentRound := leaderForRound(e.nodes, e.round)
	if e.ID.Equals(leaderForCurrentRound) {
		e.proposeBlock()
		return
	}
}

func (e *Epoch) doProposed() {

}

func (e *Epoch) doNotarized() {

}

func leaderForRound(nodes []NodeID, r uint64) NodeID {
	n := len(nodes)
	return nodes[r%uint64(n)]
}

/*func blockRecord(block Block) Record {
	r := Record{
		Type:
	}
}*/

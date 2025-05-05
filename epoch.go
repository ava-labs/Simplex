// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime/debug"
	"simplex/record"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

var ErrAlreadyStarted = errors.New("epoch already started")

const (
	DefaultMaxRoundWindow   = 10
	DefaultMaxPendingBlocks = 20

	DefaultMaxProposalWaitTime       = 5 * time.Second
	DefaultReplicationRequestTimeout = 5 * time.Second
)

type EmptyVoteSet struct {
	timedOut          bool
	votes             map[string]*EmptyVote
	emptyNotarization *EmptyNotarization
}

type Round struct {
	num           uint64
	block         VerifiedBlock
	votes         map[string]*Vote // NodeID --> vote
	notarization  *Notarization
	finalizations map[string]*Finalization // NodeID --> vote
	fCert         *FinalizationCertificate
}

func NewRound(block VerifiedBlock) *Round {
	return &Round{
		num:           block.BlockHeader().Round,
		block:         block,
		votes:         make(map[string]*Vote),
		finalizations: make(map[string]*Finalization),
	}
}

type EpochConfig struct {
	MaxProposalWait     time.Duration
	QCDeserializer      QCDeserializer
	Logger              Logger
	ID                  NodeID
	Signer              Signer
	Verifier            SignatureVerifier
	BlockDeserializer   BlockDeserializer
	SignatureAggregator SignatureAggregator
	Comm                Communication
	Storage             Storage
	WAL                 WriteAheadLog
	BlockBuilder        BlockBuilder
	Epoch               uint64
	StartTime           time.Time
	ReplicationEnabled  bool
}

type Epoch struct {
	EpochConfig
	// Runtime
	oneTimeVerifier                *oneTimeVerifier
	sched                          *scheduler
	lock                           sync.Mutex
	lastBlock                      *VerifiedFinalizedBlock // latest block & fcert commited
	canReceiveMessages             atomic.Bool
	finishCtx                      context.Context
	finishFn                       context.CancelFunc
	nodes                          NodeIDs
	eligibleNodeIDs                map[string]struct{}
	quorumSize                     int
	rounds                         map[uint64]*Round
	emptyVotes                     map[uint64]*EmptyVoteSet
	futureMessages                 messagesFromNode
	round                          uint64 // The current round we notarize
	maxRoundWindow                 uint64
	maxPendingBlocks               int
	monitor                        *Monitor
	haltedError                    error
	cancelWaitForBlockNotarization context.CancelFunc

	replicationState *ReplicationState
}

func NewEpoch(conf EpochConfig) (*Epoch, error) {
	e := &Epoch{
		EpochConfig: conf,
	}
	return e, e.init()
}

// AdvanceTime hints the engine that the given amount of time has passed.
func (e *Epoch) AdvanceTime(t time.Time) {
	e.monitor.AdvanceTime(t)
	e.replicationState.AdvanceTime(t)
}

// HandleMessage notifies the engine about a reception of a message.
func (e *Epoch) HandleMessage(msg *Message, from NodeID) error {
	e.lock.Lock()
	defer e.lock.Unlock()

	// Guard against receiving messages before we are ready to handle them.
	if !e.canReceiveMessages.Load() {
		e.Logger.Warn("Cannot receive a message")
		return nil
	}

	if e.haltedError != nil {
		return e.haltedError
	}

	if from.Equals(e.ID) {
		e.Logger.Warn("Received message from self")
		return nil
	}

	// Guard against receiving messages from unknown nodes
	_, known := e.eligibleNodeIDs[string(from)]
	if !known {
		e.Logger.Warn("Received message from an unknown node", zap.Stringer("nodeID", from))
		return nil
	}

	switch {
	case msg.BlockMessage != nil:
		return e.handleBlockMessage(msg.BlockMessage, from)
	case msg.VoteMessage != nil:
		return e.handleVoteMessage(msg.VoteMessage, from)
	case msg.EmptyVoteMessage != nil:
		return e.handleEmptyVoteMessage(msg.EmptyVoteMessage, from)
	case msg.Notarization != nil:
		return e.handleNotarizationMessage(msg.Notarization, from)
	case msg.EmptyNotarization != nil:
		return e.handleEmptyNotarizationMessage(msg.EmptyNotarization)
	case msg.Finalization != nil:
		return e.handleFinalizationMessage(msg.Finalization, from)
	case msg.FinalizationCertificate != nil:
		return e.handleFinalizationCertificateMessage(msg.FinalizationCertificate, from)
	case msg.ReplicationResponse != nil:
		return e.handleReplicationResponse(msg.ReplicationResponse, from)
	case msg.ReplicationRequest != nil:
		return e.handleReplicationRequest(msg.ReplicationRequest, from)
	default:
		e.Logger.Warn("Invalid message type", zap.Stringer("from", from))
		return nil
	}
}

func (e *Epoch) init() error {
	e.oneTimeVerifier = newOneTimeVerifier(e.Logger)
	e.sched = NewScheduler(e.Logger)
	e.monitor = NewMonitor(e.StartTime, e.Logger)
	e.cancelWaitForBlockNotarization = func() {}
	e.finishCtx, e.finishFn = context.WithCancel(context.Background())
	e.nodes = e.Comm.ListNodes()
	e.quorumSize = Quorum(len(e.nodes))
	e.rounds = make(map[uint64]*Round)
	e.maxRoundWindow = DefaultMaxRoundWindow
	e.emptyVotes = make(map[uint64]*EmptyVoteSet)
	e.maxPendingBlocks = DefaultMaxPendingBlocks
	e.eligibleNodeIDs = make(map[string]struct{}, len(e.nodes))
	e.futureMessages = make(messagesFromNode, len(e.nodes))
	e.replicationState = NewReplicationState(e.Logger, e.Comm, e.ID, e.maxRoundWindow, e.ReplicationEnabled, e.StartTime)

	for _, node := range e.nodes {
		e.futureMessages[string(node)] = make(map[uint64]*messagesForRound)
	}
	for _, node := range e.nodes {
		e.eligibleNodeIDs[string(node)] = struct{}{}
	}
	err := e.loadLastBlock()
	if err != nil {
		return err
	}

	e.Logger.Info("Starting Simplex Epoch", zap.String("ID", e.ID.String()), zap.Stringer("nodes", e.nodes))

	return e.setMetadataFromStorage()
}

func (e *Epoch) Start() error {
	if e.canReceiveMessages.Load() {
		return ErrAlreadyStarted
	}
	// Only init receiving messages once you have initialized the data structures required for it.
	defer func() {
		e.canReceiveMessages.Store(true)
	}()
	return e.restoreFromWal()
}

func (e *Epoch) restoreBlockRecord(r []byte) error {
	block, err := BlockFromRecord(e.BlockDeserializer, r)
	if err != nil {
		return err
	}
	e.rounds[block.BlockHeader().Round] = NewRound(block)
	e.Logger.Info("Block Proposal Recovered From WAL", zap.Uint64("Round", block.BlockHeader().Round))
	return nil
}

func (e *Epoch) restoreNotarizationRecord(r []byte) error {
	notarization, err := NotarizationFromRecord(r, e.QCDeserializer)
	if err != nil {
		return err
	}
	round, exists := e.rounds[notarization.Vote.Round]
	if !exists {
		return fmt.Errorf("could not find round %d, its proposal was probably not persisted earlier", notarization.Vote.Round)
	}
	round.notarization = &notarization
	e.Logger.Info("Notarization Recovered From WAL", zap.Uint64("Round", notarization.Vote.Round))
	return nil
}

func (e *Epoch) restoreEmptyNotarizationRecord(r []byte) error {
	emptyNotarization, err := EmptyNotarizationFromRecord(r, e.QCDeserializer)
	if err != nil {
		return err
	}

	emptyVotes := e.getOrCreateEmptyVoteSetForRound(emptyNotarization.Vote.Round)
	emptyVotes.emptyNotarization = &emptyNotarization
	return nil
}

func (e *Epoch) restoreEmptyVoteRecord(r []byte) error {
	vote, err := ParseEmptyVoteRecord(r)
	if err != nil {
		return err
	}

	emptyVotes := e.getOrCreateEmptyVoteSetForRound(vote.Round)
	emptyVotes.timedOut = true

	signature, err := vote.Sign(e.Signer)
	if err != nil {
		return err
	}

	emptyVote := &EmptyVote{
		Signature: Signature{
			Signer: e.ID,
			Value:  signature,
		},
		Vote: vote,
	}

	emptyVotes.votes[string(e.ID)] = emptyVote

	return nil
}

func (e *Epoch) restoreFinalizationRecord(r []byte) error {
	fCert, err := FinalizationCertificateFromRecord(r, e.QCDeserializer)
	if err != nil {
		return err
	}
	round, ok := e.rounds[fCert.Finalization.Round]
	if !ok {
		return fmt.Errorf("round not found for finalization certificate")
	}
	e.Logger.Info("Finalization Certificate Recovered From WAL", zap.Uint64("Round", fCert.Finalization.Round))
	round.fCert = &fCert
	return nil
}

// resumeFromWal resumes the epoch from the records of the write ahead log.
func (e *Epoch) resumeFromWal(records [][]byte) error {
	if len(records) == 0 {
		return e.startRound()
	}

	lastRecord := records[len(records)-1]
	recordType := binary.BigEndian.Uint16(lastRecord)

	// set the round from the last before syncing from records
	err := e.setMetadataFromRecords(records)
	if err != nil {
		return err
	}

	switch recordType {
	case record.BlockRecordType:
		block, err := BlockFromRecord(e.BlockDeserializer, lastRecord)
		if err != nil {
			return err
		}
		if e.ID.Equals(LeaderForRound(e.nodes, block.BlockHeader().Round)) {
			vote, err := e.voteOnBlock(block)
			if err != nil {
				return err
			}
			proposal := &Message{
				VerifiedBlockMessage: &VerifiedBlockMessage{
					VerifiedBlock: block,
					Vote:          vote,
				},
			}
			// broadcast only if we are the leader
			e.Comm.Broadcast(proposal)
			return e.handleVoteMessage(&vote, e.ID)
		}
		// no need to do anything, just return and handle vote messages for this block
		return nil
	case record.NotarizationRecordType:
		notarization, err := NotarizationFromRecord(lastRecord, e.QCDeserializer)
		if err != nil {
			return err
		}
		lastMessage := Message{Notarization: &notarization}
		e.Comm.Broadcast(&lastMessage)
		return e.doNotarized(notarization.Vote.Round)
	case record.EmptyVoteRecordType:
		ev, err := ParseEmptyVoteRecord(lastRecord)
		if err != nil {
			return err
		}
		round, exists := e.emptyVotes[ev.Round]
		if !exists {
			return fmt.Errorf("round %d not found for empty vote", ev.Round)
		}
		emptyVote, exists := round.votes[string(e.ID)]
		if !exists {
			return fmt.Errorf("could not find my own vote for round %d", ev.Round)
		}
		lastMessage := Message{EmptyVoteMessage: emptyVote}
		e.Comm.Broadcast(&lastMessage)
		return nil
	case record.EmptyNotarizationRecordType:
		emptyNotarization, err := EmptyNotarizationFromRecord(lastRecord, e.QCDeserializer)
		if err != nil {
			return err
		}
		lastMessage := Message{EmptyNotarization: &emptyNotarization}
		e.Comm.Broadcast(&lastMessage)
		return e.startRound()
	case record.FinalizationRecordType:
		fCert, err := FinalizationCertificateFromRecord(lastRecord, e.QCDeserializer)
		if err != nil {
			return err
		}

		finalizationCertificate := &Message{FinalizationCertificate: &fCert}
		e.Comm.Broadcast(finalizationCertificate)

		e.Logger.Debug("Broadcast finalization certificate",
			zap.Uint64("round", fCert.Finalization.Round),
			zap.Stringer("digest", fCert.Finalization.BlockHeader.Digest))

		return e.startRound()
	default:
		return fmt.Errorf("unknown record type (%d)", recordType)
	}
}

func (e *Epoch) setMetadataFromStorage() error {
	// load from storage if no notarization records
	if e.lastBlock == nil {
		return nil
	}

	e.round = e.lastBlock.VerifiedBlock.BlockHeader().Round + 1
	e.Epoch = e.lastBlock.VerifiedBlock.BlockHeader().Epoch
	return nil
}

func (e *Epoch) setMetadataFromRecords(records [][]byte) error {
	// iterate through records to find the last notarization or empty block record
	for i := len(records) - 1; i >= 0; i-- {
		recordType := binary.BigEndian.Uint16(records[i])
		if recordType == record.NotarizationRecordType {
			notarization, err := NotarizationFromRecord(records[i], e.QCDeserializer)
			if err != nil {
				return err
			}
			if notarization.Vote.Round >= e.round {
				e.round = notarization.Vote.Round + 1
				e.Epoch = notarization.Vote.BlockHeader.Epoch
			}
			return nil
		}
		if recordType == record.EmptyNotarizationRecordType {
			emptyNotarization, err := EmptyNotarizationFromRecord(records[i], e.QCDeserializer)
			if err != nil {
				return err
			}
			if emptyNotarization.Vote.Round >= e.round {
				e.round = emptyNotarization.Vote.Round + 1
				e.Epoch = emptyNotarization.Vote.Epoch
			}
			return nil
		}
	}

	return nil
}

// restoreFromWal initializes an epoch from the write ahead log.
func (e *Epoch) restoreFromWal() error {
	records, err := e.WAL.ReadAll()
	if err != nil {
		return err
	}

	for _, r := range records {
		if len(r) < 2 {
			return fmt.Errorf("malformed record")
		}
		recordType := binary.BigEndian.Uint16(r)
		switch recordType {
		case record.BlockRecordType:
			err = e.restoreBlockRecord(r)
		case record.NotarizationRecordType:
			err = e.restoreNotarizationRecord(r)
		case record.FinalizationRecordType:
			err = e.restoreFinalizationRecord(r)
		case record.EmptyNotarizationRecordType:
			err = e.restoreEmptyNotarizationRecord(r)
		case record.EmptyVoteRecordType:
			err = e.restoreEmptyVoteRecord(r)
		default:
			e.Logger.Error("undefined record type", zap.Uint16("type", recordType))
			return fmt.Errorf("undefined record type: %d", recordType)
		}
		if err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	return e.resumeFromWal(records)
}

// loadLastBlock initializes the epoch with the lastBlock retrieved from storage.
func (e *Epoch) loadLastBlock() error {
	lastIndex, err := RetrieveLastIndexFromStorage(e.Storage)
	if err != nil {
		return err
	}

	e.lastBlock = lastIndex
	return nil
}

func (e *Epoch) Stop() {
	e.finishFn()
}

func (e *Epoch) handleFinalizationCertificateMessage(message *FinalizationCertificate, from NodeID) error {
	e.Logger.Verbo("Received finalization certificate message",
		zap.Stringer("from", from), zap.Uint64("round", message.Finalization.Round), zap.Uint64("seq", message.Finalization.Seq))

	nextSeqToCommit := e.Storage.Height()
	// Ignore finalization certificates for sequences we have already committed
	if nextSeqToCommit > message.Finalization.Seq {
		return nil
	}

	valid := IsFinalizationCertificateValid(e.eligibleNodeIDs, message, e.quorumSize, e.Logger)
	if !valid {
		e.Logger.Debug("Received an invalid finalization certificate",
			zap.Int("round", int(message.Finalization.Round)),
			zap.Stringer("NodeID", from))
		return nil
	}

	round, exists := e.rounds[message.Finalization.Round]
	if !exists {
		e.handleFinalizationCertificateForPendingOrFutureRound(message, message.Finalization.Round, nextSeqToCommit)
		return nil
	}

	if round.fCert != nil {
		e.Logger.Debug("Received finalization for an already finalized round", zap.Uint64("round", message.Finalization.Round))
		return nil
	}

	round.fCert = message

	return e.persistFinalizationCertificate(*message)
}

func (e *Epoch) handleFinalizationCertificateForPendingOrFutureRound(message *FinalizationCertificate, round uint64, nextSeqToCommit uint64) {
	if round == e.round {
		// delay collecting future finalization certificate if we are verifying the proposal for that round
		// and the fCert is for the current round
		for _, msgs := range e.futureMessages {
			msgForRound, exists := msgs[round]
			if exists && msgForRound.proposal != nil {
				msgForRound.finalizationCertificate = message
				return
			}
		}
	}

	// TODO: delay requesting future fCerts and blocks, since blocks could be in transit
	e.Logger.Debug("Received finalization certificate for a future round", zap.Uint64("round", round))
	e.replicationState.replicateBlocks(message, nextSeqToCommit)
}

func (e *Epoch) handleFinalizationMessage(message *Finalization, from NodeID) error {
	finalization := message.Finalization

	e.Logger.Verbo("Received finalization message",
		zap.Stringer("from", from), zap.Uint64("round", finalization.Round))

	// Only process a point to point finalizations.
	// This is needed to prevent a malicious node from sending us a finalization of a different node for a future round.
	// Since we only verify the finalization when it's due time, this will effectively block us from saving the real finalization
	// from the real node for a future round.
	if !from.Equals(message.Signature.Signer) {
		e.Logger.Debug("Received a finalization signed by a different party than sent it", zap.Stringer("signer", message.Signature.Signer), zap.Stringer("sender", from))
		return nil
	}

	// Have we already finalized this round?
	round, exists := e.rounds[finalization.Round]

	// If we have not received the proposal yet, we won't have a Round object in e.rounds,
	// yet we may receive the corresponding finalization.
	// This may happen if we're asynchronously verifying the proposal at the moment.
	if !exists && e.round == finalization.Round {
		e.Logger.Debug("Received a finalization for the current round",
			zap.Uint64("round", finalization.Round), zap.Stringer("from", from))
		e.storeFutureFinalization(message, from, finalization.Round)
		return nil
	}

	// This finalization may correspond to a proposal from a future round, or to the proposal of the current round
	// which we are still verifying.
	if e.isWithinMaxRoundWindow(finalization.Round) {
		e.Logger.Debug("Got finalization for a future round", zap.Uint64("round", finalization.Round), zap.Uint64("my round", e.round))
		e.storeFutureFinalization(message, from, finalization.Round)
		return nil
	}

	// Finalization for a future round that is too far in the future
	if !exists {
		e.Logger.Debug("Received finalization for an unknown round", zap.Uint64("ourRound", e.round), zap.Uint64("round", finalization.Round))
		return nil
	}

	if round.fCert != nil {
		e.Logger.Debug("Received finalization for an already finalized round", zap.Uint64("round", finalization.Round))
		return nil
	}

	if !e.isFinalizationValid(message.Signature.Value, finalization, from) {
		return nil
	}

	round.finalizations[string(from)] = message
	e.deleteFutureFinalization(from, finalization.Round)

	return e.maybeCollectFinalizationCertificate(round)
}

func (e *Epoch) storeFutureFinalization(message *Finalization, from NodeID, round uint64) {
	msgsForRound, exists := e.futureMessages[string(from)][round]
	if !exists {
		msgsForRound = &messagesForRound{}
		e.futureMessages[string(from)][round] = msgsForRound
	}
	msgsForRound.finalization = message
}

func (e *Epoch) storeFutureNotarization(message *Notarization, from NodeID, round uint64) {
	msgsForRound, exists := e.futureMessages[string(from)][round]
	if !exists {
		msgsForRound = &messagesForRound{}
		e.futureMessages[string(from)][round] = msgsForRound
	}
	msgsForRound.notarization = message
}

func (e *Epoch) handleEmptyVoteMessage(message *EmptyVote, from NodeID) error {
	vote := message.Vote

	e.Logger.Verbo("Received empty vote message",
		zap.Stringer("from", from), zap.Uint64("round", vote.Round))

	// Only process point to point empty votes.
	// A node will never need to forward to us someone else's vote.
	if !from.Equals(message.Signature.Signer) {
		e.Logger.Debug("Received an empty vote signed by a different party than sent it",
			zap.Stringer("signer", message.Signature.Signer), zap.Stringer("sender", from))
		return nil
	}

	if e.round > vote.Round {
		e.Logger.Debug("Got vote from a past round",
			zap.Uint64("round", vote.Round), zap.Uint64("my round", e.round), zap.Stringer("from", from))
		return nil
	}

	// TODO: This empty vote may correspond to a future round, so... let future me implement it!
	if e.round < vote.Round { //TODO: only handle it if it's within the max round window (&& vote.Round-e.round < e.maxRoundWindow)
		e.Logger.Debug("Got empty vote from a future round",
			zap.Uint64("round", vote.Round), zap.Uint64("my round", e.round), zap.Stringer("from", from))
		//TODO: e.storeFutureEmptyVote(message, from, vote.Round)
		return nil
	}

	// Else, this is an empty vote for current round

	e.Logger.Debug("Received an empty vote for the current round",
		zap.Uint64("round", vote.Round), zap.Stringer("from", from))

	signature := message.Signature

	if err := vote.Verify(signature.Value, e.Verifier, signature.Signer); err != nil {
		e.Logger.Debug("ToBeSignedEmptyVote verification failed", zap.Stringer("NodeID", signature.Signer), zap.Error(err))
		return nil
	}

	round := vote.Round

	emptyVotes := e.getOrCreateEmptyVoteSetForRound(round)

	emptyVotes.votes[string(from)] = message

	return e.maybeAssembleEmptyNotarization()
}

func (e *Epoch) getOrCreateEmptyVoteSetForRound(round uint64) *EmptyVoteSet {
	emptyVotes, exists := e.emptyVotes[round]
	if !exists {
		emptyVotes = &EmptyVoteSet{votes: make(map[string]*EmptyVote)}
		e.emptyVotes[round] = emptyVotes
	}
	return emptyVotes
}

func (e *Epoch) handleVoteMessage(message *Vote, from NodeID) error {
	vote := message.Vote

	e.Logger.Verbo("Received vote message",
		zap.Stringer("from", from), zap.Uint64("round", vote.Round), zap.Stringer("digest", vote.Digest))

	// Only process point to point votes.
	// This is needed to prevent a malicious node from sending us a vote of a different node for a future round.
	// Since we only verify the vote when it's due time, this will effectively block us from saving the real vote
	// from the real node for a future round.
	if !from.Equals(message.Signature.Signer) {
		e.Logger.Debug("Received a vote signed by a different party than sent it",
			zap.Stringer("signer", message.Signature.Signer), zap.Stringer("sender", from),
			zap.Stringer("digest", vote.Digest))
		return nil
	}

	if !e.isVoteRoundValid(vote.Round) {
		return nil
	}

	// If we have not received the proposal yet, we won't have a Round object in e.rounds,
	// yet we may receive the corresponding vote.
	// This may happen if we're asynchronously verifying the proposal at the moment.
	if _, exists := e.rounds[vote.Round]; !exists && e.round == vote.Round {
		e.Logger.Debug("Received a vote for the current round",
			zap.Uint64("round", vote.Round), zap.Stringer("from", from))
		e.storeFutureVote(message, from, vote.Round)
		return nil
	}

	// This vote may correspond to a proposal from a future round, or to the proposal of the current round
	// which we are still verifying.
	if e.isWithinMaxRoundWindow(vote.Round) {
		e.Logger.Debug("Got vote from a future round",
			zap.Uint64("round", vote.Round), zap.Uint64("my round", e.round), zap.Stringer("from", from))
		e.storeFutureVote(message, from, vote.Round)
		return nil
	}

	round, exists := e.rounds[vote.Round]
	if !exists {
		e.Logger.Debug("Received a vote for a non existent round",
			zap.Uint64("round", vote.Round), zap.Uint64("our round", e.round))
		return nil
	}

	if round.notarization != nil {
		e.Logger.Debug("Round already notarized", zap.Uint64("round", vote.Round))
		return nil
	}

	// Only verify the vote if we haven't verified it in the past.
	signature := message.Signature
	if _, exists := round.votes[string(signature.Signer)]; !exists {
		if err := vote.Verify(signature.Value, e.Verifier, signature.Signer); err != nil {
			e.Logger.Debug("ToBeSignedVote verification failed", zap.Stringer("NodeID", signature.Signer), zap.Error(err))
			return nil
		}
	}

	e.rounds[vote.Round].votes[string(signature.Signer)] = message
	e.deleteFutureVote(from, vote.Round)

	return e.maybeCollectNotarization()
}

func (e *Epoch) haveWeAlreadyTimedOutOnThisRound(round uint64) bool {
	emptyVotes, exists := e.emptyVotes[round]
	return exists && emptyVotes.timedOut
}

func (e *Epoch) storeFutureVote(message *Vote, from NodeID, round uint64) {
	msgsForRound, exists := e.futureMessages[string(from)][round]
	if !exists {
		msgsForRound = &messagesForRound{}
		e.futureMessages[string(from)][round] = msgsForRound
	}
	msgsForRound.vote = message
}

func (e *Epoch) deleteFutureVote(from NodeID, round uint64) {
	msgsForRound, exists := e.futureMessages[string(from)][round]
	if !exists {
		return
	}
	msgsForRound.vote = nil
}

func (e *Epoch) deleteFutureProposal(from NodeID, round uint64) {
	msgsForRound, exists := e.futureMessages[string(from)][round]
	if !exists {
		return
	}
	msgsForRound.proposal = nil
}

func (e *Epoch) deleteFutureFinalization(from NodeID, round uint64) {
	msgsForRound, exists := e.futureMessages[string(from)][round]
	if !exists {
		return
	}
	msgsForRound.finalization = nil
}

func (e *Epoch) deleteFutureNotarization(from NodeID, round uint64) {
	msgsForRound, exists := e.futureMessages[string(from)][round]
	if !exists {
		return
	}
	msgsForRound.notarization = nil
}

func (e *Epoch) isFinalizationValid(signature []byte, finalization ToBeSignedFinalization, from NodeID) bool {
	if err := finalization.Verify(signature, e.Verifier, from); err != nil {
		e.Logger.Debug("Received a finalization with an invalid signature", zap.Uint64("round", finalization.Round), zap.Error(err))
		return false
	}
	return true
}

func (e *Epoch) isVoteRoundValid(round uint64) bool {
	// Ignore votes for previous rounds
	if round < e.round {
		return false
	}

	// Ignore votes for rounds too far ahead
	if e.isRoundTooFarAhead(round) {
		e.Logger.Debug("Received a vote for a too advanced round",
			zap.Uint64("round", round), zap.Uint64("my round", e.round))
		return false
	}

	return true
}

func (e *Epoch) maybeCollectFinalizationCertificate(round *Round) error {
	finalizationCount := len(round.finalizations)

	if finalizationCount < e.quorumSize {
		e.Logger.Verbo("Counting finalizations", zap.Uint64("round", e.round), zap.Int("votes", finalizationCount))
		return nil
	}

	return e.assembleFinalizationCertificate(round)
}

func (e *Epoch) assembleFinalizationCertificate(round *Round) error {
	// Divide finalizations into sets that agree on the same metadata
	finalizationsByMD := make(map[string][]*Finalization)

	for _, vote := range round.finalizations {
		key := string(vote.Finalization.Bytes())
		finalizationsByMD[key] = append(finalizationsByMD[key], vote)
	}

	var finalizations []*Finalization

	for _, finalizationsWithTheSameDigest := range finalizationsByMD {
		if len(finalizationsWithTheSameDigest) >= e.quorumSize {
			finalizations = finalizationsWithTheSameDigest
			break
		}
	}

	if len(finalizations) == 0 {
		e.Logger.Debug("Could not find enough finalizations for the same metadata")
		return nil
	}

	fCert, err := NewFinalizationCertificate(e.Logger, e.SignatureAggregator, finalizations)
	if err != nil {
		return err
	}

	round.fCert = &fCert
	return e.persistFinalizationCertificate(fCert)
}

func (e *Epoch) progressRoundsDueToCommit(round uint64) {
	e.Logger.Debug("Progressing rounds due to commit", zap.Uint64("round", round), zap.Uint64("current round", e.round))
	for e.round < round {
		e.increaseRound()
	}
}

func (e *Epoch) persistFinalizationCertificate(fCert FinalizationCertificate) error {
	e.Logger.Debug("Received enough finalizations to finalize a block", zap.Uint64("round", fCert.Finalization.Round))
	// Check to see if we should commit this finalization to the storage as part of a block commit,
	// or otherwise write it to the WAL in order to commit it later.
	startRound := e.round
	nextSeqToCommit := e.Storage.Height()
	if fCert.Finalization.Seq == nextSeqToCommit {
		e.indexFinalizationCertificates(fCert.Finalization.Round)
	} else {
		recordBytes := NewQuorumRecord(fCert.QC.Bytes(), fCert.Finalization.Bytes(), record.FinalizationRecordType)
		if err := e.WAL.Append(recordBytes); err != nil {
			e.Logger.Error("Failed to append finalization certificate record to WAL", zap.Error(err))
			return err
		}

		e.Logger.Debug("Persisted finalization certificate to WAL",
			zap.Uint64("round", fCert.Finalization.Round),
			zap.Uint64("height", nextSeqToCommit),
			zap.Int("size", len(recordBytes)),
			zap.Stringer("digest", fCert.Finalization.BlockHeader.Digest))

		// we receive a finalization certificate for a future round
		e.Logger.Debug("Received a finalization certificate for a future sequence", zap.Uint64("seq", fCert.Finalization.Seq), zap.Uint64("nextSeqToCommit", nextSeqToCommit))
		e.replicationState.replicateBlocks(&fCert, nextSeqToCommit)

		if err := e.rebroadcastPastFinalizations(); err != nil {
			return err
		}

		if e.round == fCert.Finalization.Round {
			round, ok := e.rounds[e.round]
			// This code path can be hit after incrementing the round from a notarization.
			// this check ensure we do not double increment.
			if ok && round.notarization == nil {
				e.increaseRound()
			}
		}
	}

	finalizationCertificate := &Message{FinalizationCertificate: &fCert}
	e.Comm.Broadcast(finalizationCertificate)

	e.Logger.Debug("Broadcast finalization certificate",
		zap.Uint64("round", fCert.Finalization.Round),
		zap.Stringer("digest", fCert.Finalization.BlockHeader.Digest))

	// If we have progressed to a new round while we committed blocks,
	// start the new round.
	if startRound < e.round {
		return e.startRound()
	}

	return nil
}

func (e *Epoch) rebroadcastPastFinalizations() error {
	r := e.round

	for {
		if r == 0 {
			return nil
		}
		r--
		round, exists := e.rounds[r]
		if !exists {
			return nil
		}

		// Already collected a finalization certificate
		if round.fCert != nil {
			continue
		}

		// Has notarized this round?
		if round.notarization == nil {
			continue
		}

		var finalizationMessage *Message
		// Try to re-use finalization we created if possible, else create it.
		if finalization, exists := round.finalizations[string(e.ID)]; exists {
			finalizationMessage = &Message{Finalization: finalization}
		} else {
			_, msg, err := e.constructFinalizationMessage(round.notarization.Vote.BlockHeader)
			if err != nil {
				return err
			}
			finalizationMessage = msg
		}
		e.Logger.Debug("Rebroadcasting finalization", zap.Uint64("round", r))
		e.Comm.Broadcast(finalizationMessage)
	}
}

func (e *Epoch) indexFinalizationCertificates(startRound uint64) {
	r := startRound
	round, exists := e.rounds[r]
	if !exists {
		e.Logger.Debug("Round not found", zap.Uint64("round", r))
		return
	}
	if round.fCert.Finalization.Seq != e.Storage.Height() {
		e.Logger.Debug("Finalization certificate does not correspond to the next sequence to commit",
			zap.Uint64("seq", round.fCert.Finalization.Seq), zap.Uint64("height", e.Storage.Height()))
		return
	}

	for exists && round.fCert != nil {
		fCert := *round.fCert
		block := round.block
		e.indexFinalizationCertificate(block, fCert)

		e.deleteRounds(round.num)
		// Clean up the future messages - Remove all messages we may have stored for the round
		// the finalization is about.
		for _, messagesFromNode := range e.futureMessages {
			delete(messagesFromNode, fCert.Finalization.Round)
		}

		// Check if we can commit the next round
		r++
		round, exists = e.rounds[r]
	}
}

func (e *Epoch) indexFinalizationCertificate(block VerifiedBlock, fCert FinalizationCertificate) {
	e.Storage.Index(block, fCert)
	e.Logger.Info("Committed block",
		zap.Uint64("round", fCert.Finalization.Round),
		zap.Uint64("sequence", fCert.Finalization.Seq),
		zap.Stringer("digest", fCert.Finalization.BlockHeader.Digest))
	e.lastBlock = &VerifiedFinalizedBlock{
		VerifiedBlock: block,
		FCert:         fCert,
	}

	// We have commited because we have collected a finalization certificate.
	// However, we may have not witnessed a notarization.
	// Regardless of that, we can safely progress to the round succeeding the finalization.
	e.progressRoundsDueToCommit(fCert.Finalization.Round + 1)
}

func (e *Epoch) maybeAssembleEmptyNotarization() error {
	emptyVotes, exists := e.emptyVotes[e.round]

	// This should never happen, but done for sanity
	if !exists {
		return fmt.Errorf("could not find empty vote set for round %d", e.round)
	}

	// Check if we found a quorum of votes for the same metadata
	quorumSize := e.quorumSize
	popularEmptyVote, signatures, found := findMostPopularEmptyVote(emptyVotes.votes, quorumSize)
	if !found {
		e.Logger.Debug("Could not find empty vote with a quorum or more votes", zap.Uint64("round", e.round))
		return nil
	}

	qc, err := e.SignatureAggregator.Aggregate(signatures)
	if err != nil {
		e.Logger.Error("Could not aggregate empty votes signatures", zap.Error(err), zap.Uint64("round", e.round))
		return nil
	}

	emptyNotarization := &EmptyNotarization{QC: qc, Vote: popularEmptyVote}
	// write to the empty vote set
	emptyVotes.emptyNotarization = emptyNotarization

	// Persist the empty notarization and also broadcast it to everyone
	return e.persistEmptyNotarization(emptyNotarization, true)
}

func findMostPopularEmptyVote(votes map[string]*EmptyVote, quorumSize int) (ToBeSignedEmptyVote, []Signature, bool) {
	votesByBytes := make(map[string][]*EmptyVote)
	for _, vote := range votes {
		key := string(vote.Vote.Bytes())
		votesByBytes[key] = append(votesByBytes[key], vote)
	}

	var popularEmptyVotes []*EmptyVote

	for _, votes := range votesByBytes {
		if len(votes) >= quorumSize {
			popularEmptyVotes = votes
			break
		}
	}

	if len(popularEmptyVotes) == 0 {
		return ToBeSignedEmptyVote{}, nil, false
	}

	sigs := make([]Signature, 0, len(popularEmptyVotes))
	for _, vote := range popularEmptyVotes {
		sigs = append(sigs, vote.Signature)
	}

	return popularEmptyVotes[0].Vote, sigs, true
}

func (e *Epoch) persistEmptyNotarization(emptyNotarization *EmptyNotarization, shouldBroadcast bool) error {
	emptyNotarizationRecord := NewEmptyNotarizationRecord(emptyNotarization)
	if err := e.WAL.Append(emptyNotarizationRecord); err != nil {
		e.Logger.Error("Failed to append empty block record to WAL", zap.Error(err))
		return err
	}

	e.Logger.Debug("Persisted empty block to WAL",
		zap.Int("size", len(emptyNotarizationRecord)),
		zap.Uint64("round", emptyNotarization.Vote.Round))

	if shouldBroadcast {
		notarizationMessage := &Message{EmptyNotarization: emptyNotarization}
		e.Comm.Broadcast(notarizationMessage)
		e.Logger.Debug("Broadcast empty notarization",
			zap.Uint64("round", emptyNotarization.Vote.Round))
	}

	e.increaseRound()

	return errors.Join(e.startRound(), e.maybeLoadFutureMessages())
}

func (e *Epoch) maybeCollectNotarization() error {
	votesForCurrentRound := e.rounds[e.round].votes
	voteCount := len(votesForCurrentRound)

	if voteCount < e.quorumSize {
		from := make([]NodeID, 0, voteCount)
		for _, vote := range votesForCurrentRound {
			from = append(from, vote.Signature.Signer)
		}
		e.Logger.Verbo("Counting votes", zap.Uint64("round", e.round),
			zap.Int("votes", voteCount), zap.String("from", fmt.Sprintf("%s", from)))

		// As a last resort, check if we have received a notarization message for this round
		// by attempting to load it from the future messages.
		return e.maybeLoadFutureMessages()
	}

	// TODO: store votes before receiving the block
	block := e.rounds[e.round].block
	expectedDigest := block.BlockHeader().Digest

	// Ensure we have enough votes for the same digest
	var voteCountForOurDigest int
	for _, vote := range votesForCurrentRound {
		if bytes.Equal(expectedDigest[:], vote.Vote.Digest[:]) {
			voteCountForOurDigest++
		}
	}

	if voteCountForOurDigest < e.quorumSize {
		e.Logger.Warn("Counting votes for the digest we received from the leader",
			zap.Uint64("round", e.round),
			zap.Int("voteForOurDigests", voteCountForOurDigest),
			zap.Int("total votes", voteCount))

		// As a last resort, check if we have received a notarization message for this round
		// by attempting to load it from the future messages.
		return e.maybeLoadFutureMessages()
	}

	notarization, err := NewNotarization(e.Logger, e.SignatureAggregator, votesForCurrentRound, block.BlockHeader())
	if err != nil {
		return err
	}

	return e.persistAndBroadcastNotarization(notarization)
}

func (e *Epoch) writeNotarizationToWal(notarization Notarization) error {
	record := NewQuorumRecord(notarization.QC.Bytes(), notarization.Vote.Bytes(), record.NotarizationRecordType)

	if err := e.WAL.Append(record); err != nil {
		e.Logger.Error("Failed to append notarization record to WAL", zap.Error(err))
		return err
	}

	e.Logger.Debug("Persisted notarization to WAL",
		zap.Int("size", len(record)),
		zap.Uint64("round", notarization.Vote.Round),
		zap.Stringer("digest", notarization.Vote.BlockHeader.Digest))

	return nil
}

func (e *Epoch) persistNotarization(notarization Notarization) error {
	if err := e.writeNotarizationToWal(notarization); err != nil {
		return nil
	}

	err := e.storeNotarization(notarization)
	if err != nil {
		return err
	}

	e.increaseRound()

	return nil
}

func (e *Epoch) persistAndBroadcastNotarization(notarization Notarization) error {
	err := e.persistNotarization(notarization)
	if err != nil {
		return err
	}

	notarizationMessage := &Message{Notarization: &notarization}
	e.Comm.Broadcast(notarizationMessage)

	e.Logger.Debug("Broadcast notarization",
		zap.Uint64("round", notarization.Vote.Round),
		zap.Stringer("digest", notarization.Vote.BlockHeader.Digest))

	return errors.Join(e.doNotarized(notarization.Vote.Round), e.maybeLoadFutureMessages())
}

func (e *Epoch) handleEmptyNotarizationMessage(emptyNotarization *EmptyNotarization) error {
	vote := emptyNotarization.Vote

	e.Logger.Verbo("Received empty notarization message", zap.Uint64("round", vote.Round))

	// Ignore votes for previous rounds
	if !e.isVoteRoundValid(vote.Round) {
		return nil
	}

	// Check if we have collected a notarization or a finalization for this round.
	// If we did, then we don't need to process an empty notarization,
	// because we have a way to progress to the successive round, regardless if
	// it's our current round or not.
	round, exists := e.rounds[vote.Round]
	if exists && (round.notarization != nil || round.fCert != nil) {
		return nil
	}

	// Otherwise, this round is not notarized or finalized yet, so verify the empty notarization and store it.

	if !e.verifyEmptyNotarization(emptyNotarization) {
		return nil
	}

	emptyVotes := e.getOrCreateEmptyVoteSetForRound(vote.Round)
	emptyVotes.emptyNotarization = emptyNotarization
	if e.round != vote.Round {
		e.Logger.Debug("Received empty notarization for a future round",
			zap.Uint64("round", vote.Round), zap.Uint64("our round", e.round))
		return nil
	}

	// The empty notarization is for this round, so store it but don't broadcast it, as we've received it via a broadcast.
	return e.persistEmptyNotarization(emptyNotarization, false)
}

func (e *Epoch) verifyEmptyNotarization(emptyNotarization *EmptyNotarization) bool {
	// Check empty notarization was signed by only eligible nodes
	for _, signer := range emptyNotarization.QC.Signers() {
		if _, exists := e.eligibleNodeIDs[string(signer)]; !exists {
			e.Logger.Warn("Empty notarization quorum certificate contains an unknown signer", zap.Stringer("signer", signer))
			return false
		}
	}

	// Ensure no node signed the empty notarization twice
	doubleSigner, signedTwice := hasSomeNodeSignedTwice(emptyNotarization.QC.Signers(), e.Logger)
	if signedTwice {
		e.Logger.Warn("A node has signed the empty notarization twice", zap.Stringer("signer", doubleSigner))
		return false
	}

	// Check enough signers signed the empty notarization
	if e.quorumSize > len(emptyNotarization.QC.Signers()) {
		e.Logger.Warn("Empty notarization signed by insufficient nodes",
			zap.Int("count", len(emptyNotarization.QC.Signers())),
			zap.Int("Quorum", e.quorumSize))
		return false
	}

	if err := emptyNotarization.Verify(); err != nil {
		e.Logger.Debug("Empty Notarization is invalid", zap.Error(err))
		return false
	}
	return true
}

func (e *Epoch) handleNotarizationMessage(message *Notarization, from NodeID) error {
	vote := message.Vote

	e.Logger.Verbo("Received notarization message",
		zap.Stringer("from", from), zap.Uint64("round", vote.Round))

	if !e.isVoteRoundValid(vote.Round) {
		return nil
	}

	if !e.verifyNotarization(message, from) {
		return nil
	}

	// Can we handle this notarization right away or should we handle it later?
	round, exists := e.rounds[vote.Round]
	// If we have already notarized the round, no need to continue
	if exists && round.notarization != nil {
		e.Logger.Debug("Received a notarization for an already notarized round")
		return nil
	}
	// If this notarization is for a round we are currently processing its proposal,
	// or for a future round, then store it for later use.
	if !exists || e.round < vote.Round {
		e.Logger.Debug("Received a notarization for a future round", zap.Uint64("round", vote.Round))
		e.storeFutureNotarization(message, from, vote.Round)
		return nil
	}

	// We are about to persist the notarization, so delete it in case it came from the future messages.
	e.deleteFutureNotarization(from, vote.Round)

	// Else, this is a notarization for the current round, and we have stored the proposal for this round.
	// Note that we don't need to check if we have timed out on this round,
	// because if we had collected an empty notarization for this round, we would have progressed to the next round.
	return e.persistAndBroadcastNotarization(*message)
}

func (e *Epoch) verifyNotarization(message *Notarization, from NodeID) bool {
	// Ensure no node signed the notarization twice
	doubleSigner, signedTwice := hasSomeNodeSignedTwice(message.QC.Signers(), e.Logger)
	if signedTwice {
		e.Logger.Warn("A node has signed the notarization twice", zap.Stringer("signer", doubleSigner))
		return false
	}

	// Check enough signers signed the notarization
	if e.quorumSize > len(message.QC.Signers()) {
		e.Logger.Warn("Notarization certificate signed by insufficient nodes",
			zap.Int("count", len(message.QC.Signers())),
			zap.Int("Quorum", e.quorumSize))
		return false
	}

	// Check notarization was signed by only eligible nodes
	for _, signer := range message.QC.Signers() {
		if _, exists := e.eligibleNodeIDs[string(signer)]; !exists {
			e.Logger.Warn("Notarization quorum certificate contains an unknown signer", zap.Stringer("signer", signer))
			return false
		}
	}

	if err := message.Verify(); err != nil {
		e.Logger.Debug("Notarization quorum certificate is invalid",
			zap.Stringer("NodeID", from), zap.Error(err))
		return false
	}
	return true
}

func (e *Epoch) handleBlockMessage(message *BlockMessage, from NodeID) error {
	block := message.Block
	if block == nil {
		e.Logger.Debug("Got empty block in a BlockMessage")
		return nil
	}

	e.Logger.Verbo("Received block message",
		zap.Stringer("from", from), zap.Uint64("round", block.BlockHeader().Round))

	pendingBlocks := e.sched.Size()
	if pendingBlocks > e.maxPendingBlocks {
		e.Logger.Warn("Too many blocks being verified to ingest another one", zap.Int("pendingBlocks", pendingBlocks))
		return nil
	}

	vote := message.Vote
	from = vote.Signature.Signer

	md := block.BlockHeader()

	e.Logger.Debug("Handling block message", zap.Stringer("digest", md.Digest), zap.Uint64("round", md.Round))

	// Don't bother processing blocks from the past
	if e.round > md.Round {
		return nil
	}

	// The block is for a too high round, we shouldn't handle it as
	// we have only so much memory.
	if e.isRoundTooFarAhead(md.Round) {
		e.Logger.Debug("Received a block message for a too high round",
			zap.Uint64("round", md.Round), zap.Uint64("our round", e.round))
		return nil
	}

	// Check that the node is a leader for the round corresponding to the block.
	if !LeaderForRound(e.nodes, md.Round).Equals(from) {
		// The block is associated with a round in which the sender is not the leader,
		// it should not be sending us any block at all.
		e.Logger.Debug("Got block from a block proposer that is not the leader of the round", zap.Stringer("NodeID", from), zap.Uint64("round", md.Round))
		return nil
	}

	// Check if we have verified this message in the past:
	alreadyVerified := e.wasBlockAlreadyVerified(from, md)

	if !alreadyVerified {
		// Ensure the block was voted on by its block producer:

		// 1) Verify block digest corresponds to the digest voted on
		if !bytes.Equal(vote.Vote.Digest[:], md.Digest[:]) {
			e.Logger.Debug("ToBeSignedVote digest mismatches block digest", zap.Stringer("voteDigest", vote.Vote.Digest),
				zap.Stringer("blockDigest", md.Digest))
			return nil
		}
		// 2) Verify the vote is properly signed
		if err := vote.Vote.Verify(vote.Signature.Value, e.Verifier, vote.Signature.Signer); err != nil {
			e.Logger.Debug("ToBeSignedVote verification failed", zap.Stringer("NodeID", vote.Signature.Signer), zap.Error(err))
			return nil
		}
	}

	// If this is a message from a more advanced round,
	// only store it if it is up to `maxRoundWindow` ahead.
	// TODO: test this
	if e.isWithinMaxRoundWindow(md.Round) {
		e.Logger.Debug("Got block of a future round", zap.Uint64("round", md.Round), zap.Uint64("my round", e.round))
		msgsForRound, exists := e.futureMessages[string(from)][md.Round]
		if !exists {
			msgsForRound = &messagesForRound{}
			e.futureMessages[string(from)][md.Round] = msgsForRound
		}

		// Has this node already sent us a proposal?
		// If so, it cannot send it again.
		if msgsForRound.proposal != nil {
			e.Logger.Debug("Already received a proposal from this node for the round",
				zap.Stringer("NodeID", from), zap.Uint64("round", md.Round))
			return nil
		}

		msgsForRound.proposal = message
		return nil
	}

	if !e.verifyProposalIsPartOfOurChain(block) {
		e.Logger.Debug("Got invalid block in a BlockMessage")
		return nil
	}

	// save in future messages while we are verifying the block
	msgForRound, exists := e.futureMessages[string(from)][md.Round]
	if !exists {
		msgsForRound := &messagesForRound{}
		msgsForRound.proposal = message
		e.futureMessages[string(from)][md.Round] = msgsForRound
	} else {
		msgForRound.proposal = message
	}

	// Create a task that will verify the block in the future, after its predecessors have also been verified.
	task := e.createBlockVerificationTask(e.oneTimeVerifier.Wrap(block), from, vote)

	// isBlockReadyToBeScheduled checks if the block is known to us either from some previous round,
	// or from storage. If so, then we have verified it in the past, since only verified blocks are saved in memory.
	canBeImmediatelyVerified := e.isBlockReadyToBeScheduled(md.Seq, md.Prev)

	// Schedule the block to be verified once its direct predecessor have been verified,
	// or if it can be verified immediately.
	//e.Logger.Debug("Scheduling block verification", zap.Uint64("round", md.Round))
	e.sched.Schedule(task, md.Prev, canBeImmediatelyVerified)

	return nil
}

// processFinalizedBlocks processes a block that has a finalization certificate.
// if the block has already been verified, it will index the finalization certificate,
// otherwise it will verify the block first.
func (e *Epoch) processFinalizedBlock(block Block, fCert FinalizationCertificate) error {
	round, exists := e.rounds[fCert.Finalization.Round]
	// dont create a block verification task if the block is already in the rounds map
	if exists {
		roundDigest := round.block.BlockHeader().Digest
		seqDigest := fCert.Finalization.BlockHeader.Digest
		if !bytes.Equal(roundDigest[:], seqDigest[:]) {
			e.Logger.Warn("Received finalized block that is different from the one we have in the rounds map",
				zap.Stringer("roundDigest", roundDigest), zap.Stringer("seqDigest", seqDigest))

			delete(e.rounds, round.num)
			return e.processFinalizedBlock(block, fCert)
		}
		round.fCert = &fCert
		e.indexFinalizationCertificates(round.num)
		return e.processReplicationState()
	}

	pendingBlocks := e.sched.Size()
	if pendingBlocks > e.maxPendingBlocks {
		e.Logger.Warn("Too many blocks being verified to ingest another one", zap.Int("pendingBlocks", pendingBlocks))
		return nil
	}
	md := block.BlockHeader()

	// Create a task that will verify the block in the future, after its predecessors have also been verified.
	task := e.createFinalizedBlockVerificationTask(e.oneTimeVerifier.Wrap(block), fCert)

	// isBlockReadyToBeScheduled checks if the block is known to us either from some previous round,
	// or from storage. If so, then we have verified it in the past, since only verified blocks are saved in memory.
	canBeImmediatelyVerified := e.isBlockReadyToBeScheduled(md.Seq, md.Prev)

	// Schedule the block to be verified once its direct predecessor have been verified,
	// or if it can be verified immediately.
	if e.ID.Equals(NodeID{4}) {
		debug.PrintStack()
	}
	e.Logger.Debug("Scheduling block verification", zap.Uint64("round", md.Round))
	e.sched.Schedule(task, md.Prev, canBeImmediatelyVerified)

	return nil
}

// processNotarizedBlock processes a block that has a notarization.
// if the block has already been verified, it will persist the notarization,
// otherwise it will verify the block first.
func (e *Epoch) processNotarizedBlock(block Block, notarization *Notarization) error {
	md := block.BlockHeader()
	round, exists := e.rounds[md.Round]

	// dont create a block verification task if the block is already in the rounds map
	if exists {
		// We could have a block in the rounds map, as well as an empty notarization.
		// its important to not create a conflicting notarization for that round.
		emptyVote, exists := e.emptyVotes[md.Round]

		if exists && emptyVote.emptyNotarization != nil {
			e.Logger.Debug("Received notarized block for a round that has an empty notarization",
				zap.Uint64("round", md.Round))
			return nil
		}

		if round.notarization != nil || round.fCert != nil {
			e.Logger.Debug("Round already notarized", zap.Uint64("round", md.Round))
			return nil
		}

		roundDigest := round.block.BlockHeader().Digest
		notarizedDigest := notarization.Vote.BlockHeader.Digest
		if !bytes.Equal(roundDigest[:], notarizedDigest[:]) {
			e.Logger.Warn("Received notarized block that is different from the one we have in the rounds map",
				zap.Stringer("roundDigest", roundDigest), zap.Stringer("notarizedDigest", notarizedDigest))
			// by deleting the round, and recursively calling processNotarizedBlock
			// we will verify this new block and store the notarization.
			delete(e.rounds, md.Round)
			return e.processNotarizedBlock(block, notarization)
		}

		if err := e.persistNotarization(*notarization); err != nil {
			e.Logger.Warn("Failed to persist notarization", zap.Error(err))
			e.haltedError = err
			return nil
		}

		return e.processReplicationState()
	}

	pendingBlocks := e.sched.Size()
	if pendingBlocks > e.maxPendingBlocks {
		e.Logger.Warn("Too many blocks being verified to ingest another one", zap.Int("pendingBlocks", pendingBlocks))
		return nil
	}

	if e.ID.Equals(NodeID{4}) {
		debug.PrintStack()
	}

	// Create a task that will verify the block in the future, after its predecessors have also been verified.
	task := e.createNotarizedBlockVerificationTask(e.oneTimeVerifier.Wrap(block), *notarization)

	// isBlockReadyToBeScheduled checks if the block is known to us either from some previous round,
	// or from storage. If so, then we have verified it in the past, since only verified blocks are saved in memory.
	canBeImmediatelyVerified := e.isBlockReadyToBeScheduled(md.Seq, md.Prev)

	// Schedule the block to be verified once its direct predecessor have been verified,
	// or if it can be verified immediately.
	e.Logger.Debug("Scheduling block verification", zap.Uint64("round", md.Round))
	e.sched.Schedule(task, md.Prev, canBeImmediatelyVerified)

	return nil
}

func (e *Epoch) createBlockVerificationTask(block Block, from NodeID, vote Vote) func() Digest {
	return func() Digest {
		md := block.BlockHeader()

/*		//e.Logger.Debug("Block verification started", zap.Uint64("round", md.Round))
		//start := time.Now()
		defer func() {
			//elapsed := time.Since(start)
			//e.Logger.Debug("Block verification ended", zap.Uint64("round", md.Round), zap.Duration("elapsed", elapsed))
		}()*/

		verifiedBlock, err := block.Verify(context.Background())
		if err != nil {
			e.Logger.Debug("Failed verifying block", zap.Error(err))
			return md.Digest
		}

		e.lock.Lock()
		defer e.lock.Unlock()

		record := BlockRecord(md, verifiedBlock.Bytes())
		if err := e.WAL.Append(record); err != nil {
			e.haltedError = err
			e.Logger.Error("Failed to append block record to WAL", zap.Error(err))
			return md.Digest
		}

		e.Logger.Debug("Persisted block to WAL",
			zap.Uint64("round", md.Round),
			zap.Stringer("digest", md.Digest))

		e.deleteFutureProposal(from, md.Round)

		if !e.storeProposal(verifiedBlock) {
			e.Logger.Warn("Unable to store proposed block for the round", zap.Stringer("NodeID", from), zap.Uint64("round", md.Round))
			return md.Digest
			// TODO: timeout
		}

		// We might have received votes and finalizations from future rounds before we received this block.
		// So load the messages into our round data structure now that we have created it.
		err = e.maybeLoadFutureMessages()
		if err != nil {
			e.haltedError = err
			return md.Digest
		}

		// Check if we have timed out on this round.
		// Although we store the proposal for this round,
		// we refuse to vote for it because we have timed out.
		// We store the proposal only in order to be able to finalize it
		// in case we cannot assemble an empty notarization but eventually
		// this proposal is either notarized or finalized.
		if e.haveWeAlreadyTimedOutOnThisRound(md.Round) {
			e.Logger.Debug("Refusing to vote on block because already timed out in this round", zap.Uint64("round", md.Round), zap.Stringer("NodeID", from))
			return md.Digest
		}

		// Once we have stored the proposal, we have a Round object for the round.
		// We store the vote to prevent verifying its signature again.
		round, exists := e.rounds[md.Round]
		if !exists {
			// This shouldn't happen, but in case it does, return an error
			e.Logger.Error("programming error: round not found", zap.Uint64("round", md.Round))
			return md.Digest
		}
		round.votes[string(vote.Signature.Signer)] = &vote

		if err := e.doProposed(verifiedBlock); err != nil {
			e.Logger.Warn("Failed voting on block", zap.Error(err))
		}

		return md.Digest
	}
}

func (e *Epoch) createFinalizedBlockVerificationTask(block Block, fCert FinalizationCertificate) func() Digest {
	return func() Digest {
		md := block.BlockHeader()

		//e.Logger.Debug("Block verification started", zap.Uint64("round", md.Round))
		//start := time.Now()
		defer func() {
			//elapsed := time.Since(start)
			//e.Logger.Debug("Block verification ended", zap.Uint64("round", md.Round), zap.Duration("elapsed", elapsed))
		}()

		e.Logger.Debug("Handling finalized block message", zap.Uint64("seq", md.Seq), zap.Uint64("round", md.Round))

		verifiedBlock, err := block.Verify(context.Background())
		if err != nil {
			e.Logger.Debug("Failed verifying block", zap.Error(err))
			return md.Digest
		}

		e.lock.Lock()
		defer e.lock.Unlock()

		// we started verifying the block when it was the next sequence to commit, however its
		// possible we received a fCert for this block in the meantime. This check ensures we commit
		// the block only if it is still the next sequence to commit.
		if e.Storage.Height() != md.Seq {
			e.Logger.Debug("Received finalized block that is not the next sequence to commit",
				zap.Uint64("seq", md.Seq), zap.Uint64("height", e.Storage.Height()))
			return md.Digest
		}

		e.indexFinalizationCertificate(verifiedBlock, fCert)
		err = e.processReplicationState()

		if err != nil {
			e.haltedError = err
			e.Logger.Error("Failed to process replication state", zap.Error(err))
			return md.Digest
		}

		return md.Digest
	}
}

func (e *Epoch) createNotarizedBlockVerificationTask(block Block, notarization Notarization) func() Digest {
	return func() Digest {
		md := block.BlockHeader()

/*		e.Logger.Debug("Block verification started", zap.Uint64("round", md.Round))
		start := time.Now()
		defer func() {
			elapsed := time.Since(start)
			e.Logger.Debug("Block verification ended", zap.Uint64("round", md.Round), zap.Duration("elapsed", elapsed))
		}()*/

		verifiedBlock, err := block.Verify(context.Background())
		if err != nil {
			e.Logger.Debug("Failed verifying block", zap.Error(err))
			return md.Digest
		}

		e.lock.Lock()
		defer e.lock.Unlock()

		// we started verifying the block when we didn't have a notarization, however its
		// possible we received a notarization or empty notarization for this block in the meantime.
		round, ok := e.rounds[md.Round]
		emptyVote, emptyOk := e.emptyVotes[md.Round]
		if (ok && round.notarization != nil) || (emptyOk && emptyVote.emptyNotarization != nil) {
			e.Logger.Debug("Verifying notarized block that already has a notarization for the round",
				zap.Uint64("round", md.Round))
			return md.Digest
		}

		// store the block in rounds
		if !e.storeProposal(verifiedBlock) {
			e.Logger.Warn("Unable to store proposed block for the round", zap.Uint64("round", md.Round))
			return md.Digest
			// TODO: timeout
		}

		round, ok = e.rounds[block.BlockHeader().Round]
		if !ok {
			e.Logger.Warn("Unable to get proposed block for the round", zap.Uint64("round", md.Round))
			return md.Digest
		}
		round.notarization = &notarization

		if err := e.persistNotarization(notarization); err != nil {
			e.haltedError = err
		}

		err = e.processReplicationState()
		if err != nil {
			e.haltedError = err
			e.Logger.Error("Failed to process replication state", zap.Error(err))
			return md.Digest
		}

		return md.Digest
	}
}

func (e *Epoch) isBlockReadyToBeScheduled(seq uint64, prev Digest) bool {
	if seq > 0 {
		// A block can be scheduled if its predecessor either exists in storage,
		// or there exists a round object for it.
		// Since we only create a round object after we verify the block,
		// it means we have verified this block in the past.
		_, ok := e.locateBlock(seq-1, prev[:])
		return ok
	}
	// The first block is always ready to be scheduled
	return true
}

func (e *Epoch) wasBlockAlreadyVerified(from NodeID, md BlockHeader) bool {
	var alreadyVerified bool
	msgsForRound, exists := e.futureMessages[string(from)][md.Round]
	if exists && msgsForRound.proposal != nil {
		bh := msgsForRound.proposal.Block.BlockHeader()
		alreadyVerified = bh.Equals(&md)
	}
	return alreadyVerified
}

func (e *Epoch) verifyProposalIsPartOfOurChain(block Block) bool {
	bh := block.BlockHeader()

	if bh.Version != 0 {
		e.Logger.Debug("Got block message with wrong version number, expected 0", zap.Uint8("version", bh.Version))
		return false
	}

	if e.Epoch != bh.Epoch {
		e.Logger.Debug("Got block message but the epoch mismatches our epoch",
			zap.Uint64("our epoch", e.Epoch), zap.Uint64("block epoch", bh.Epoch))
	}

	var expectedSeq uint64
	var expectedPrevDigest Digest

	// Else, either it's not the first block, or we haven't committed the first block, and it is the first block.
	// If it's the latter we have nothing else to do.
	// If it's the former, we need to find the parent of the block and ensure it is correct.
	if bh.Seq > 0 {
		// TODO: we should cache this data, we don't need the block, just the hash and sequence.
		_, found := e.locateBlock(bh.Seq-1, bh.Prev[:])
		if !found {
			e.Logger.Debug("Could not find parent block with given digest",
				zap.Uint64("blockSeq", bh.Seq-1),
				zap.Stringer("digest", bh.Prev))
			// We could not find the parent block, so no way to verify this proposal.
			return false
		}

		// TODO: we need to take into account dummy blocks!
		expectedSeq = bh.Seq
		expectedPrevDigest = bh.Prev
	}

	if bh.Seq != expectedSeq {
		e.Logger.Debug("Received block with an incorrect sequence",
			zap.Uint64("round", bh.Round),
			zap.Uint64("seq", bh.Seq),
			zap.Uint64("expected seq", expectedSeq))
	}

	digest := block.BlockHeader().Digest

	expectedBH := BlockHeader{
		Digest: digest,
		ProtocolMetadata: ProtocolMetadata{
			Round:   e.round,
			Seq:     expectedSeq,
			Epoch:   e.Epoch,
			Prev:    expectedPrevDigest,
			Version: 0,
		},
	}
	return expectedBH.Equals(&bh)
}

// locateBlock locates a block:
// 1) In memory
// 2) Else, on storage.
// Compares to the given digest, and if it's the same, returns it.
// Otherwise, returns false.
func (e *Epoch) locateBlock(seq uint64, digest []byte) (VerifiedBlock, bool) {
	// TODO index rounds by digest too to make it quicker
	// TODO: optimize this by building an index from digest to round
	for _, round := range e.rounds {
		dig := round.block.BlockHeader().Digest
		if bytes.Equal(dig[:], digest) {
			return round.block, true
		}
	}

	height := e.Storage.Height()
	// Not in memory, and no block resides in storage.
	if height == 0 {
		return nil, false
	}

	// If the given block has a sequence that is higher than the last block we committed to storage,
	// we don't have the block in our storage.
	maxSeq := height - 1
	if maxSeq < seq {
		return nil, false
	}

	block, _, ok := e.Storage.Retrieve(seq)
	if !ok {
		return nil, false
	}

	dig := block.BlockHeader().Digest
	if bytes.Equal(dig[:], digest) {
		return block, true
	}

	return nil, false
}

func (e *Epoch) buildBlock() {
	metadata := e.metadata()

	task := e.createBlockBuildingTask(metadata)

	e.Logger.Debug("Scheduling block building", zap.Uint64("round", metadata.Round))
	e.sched.Schedule(task, metadata.Prev, true)
}

func (e *Epoch) createBlockBuildingTask(metadata ProtocolMetadata) func() Digest {
	return func() Digest {
		block, ok := e.BlockBuilder.BuildBlock(e.finishCtx, metadata)
		if !ok {
			e.Logger.Warn("Failed building block")
			return Digest{}
		}

		e.lock.Lock()
		defer e.lock.Unlock()

		e.proposeBlock(block)

		return block.BlockHeader().Digest
	}
}

func (e *Epoch) proposeBlock(block VerifiedBlock) error {
	md := block.BlockHeader()

	// Write record to WAL before broadcasting it, so that
	// if we crash during broadcasting, we know what we sent.

	rawBlock := block.Bytes()
	record := BlockRecord(block.BlockHeader(), rawBlock)
	if err := e.WAL.Append(record); err != nil {
		e.Logger.Error("Failed appending block to WAL", zap.Error(err))
		return err
	}
	e.Logger.Debug("Wrote block to WAL",
		zap.Uint64("round", md.Round),
		zap.Int("size", len(rawBlock)),
		zap.Stringer("digest", md.Digest))

	vote, err := e.voteOnBlock(block)
	if err != nil {
		return err
	}

	proposal := &Message{
		VerifiedBlockMessage: &VerifiedBlockMessage{
			VerifiedBlock: block,
			Vote:          vote,
		},
	}

	if !e.storeProposal(block) {
		return errors.New("failed to store block proposed by me")
	}

	e.Comm.Broadcast(proposal)
	e.Logger.Debug("Proposal broadcast",
		zap.Uint64("round", md.Round),
		zap.Int("size", len(rawBlock)),
		zap.Stringer("digest", md.Digest))

	// We might have received votes and finalizations from future rounds before we received this block.
	// So load the messages into our round data structure now that we have created it.
	return errors.Join(e.handleVoteMessage(&vote, e.ID), e.maybeLoadFutureMessages())
}

// Metadata returns the metadata of the next expected block of the epoch.
func (e *Epoch) Metadata() ProtocolMetadata {
	e.lock.Lock()
	defer e.lock.Unlock()

	return e.metadata()
}

func (e *Epoch) metadata() ProtocolMetadata {
	var prev Digest
	seq := e.Storage.Height()

	highestRound := e.getHighestRound()
	if highestRound != nil {
		// Build on top of the latest block
		currMed := highestRound.block.BlockHeader()
		prev = currMed.Digest
		seq = currMed.Seq + 1
	}

	if e.lastBlock != nil {
		currMed := e.lastBlock.VerifiedBlock.BlockHeader()
		if currMed.Seq+1 >= seq {
			prev = currMed.Digest
			seq = currMed.Seq + 1
		}
	}

	md := ProtocolMetadata{
		Round:   e.round,
		Seq:     seq,
		Epoch:   e.Epoch,
		Prev:    prev,
		Version: 0,
	}
	return md
}

func (e *Epoch) triggerProposalWaitTimeExpired(round uint64) {
	leader := LeaderForRound(e.nodes, round)
	e.Logger.Info("Timed out on block agreement", zap.Uint64("round", round), zap.Stringer("leader", leader))
	// TODO: Actually start the empty block agreement

	md := e.metadata()
	md.Seq-- // e.metadata() returns metadata fit for a new block proposal, but we need the sequence of the previous block proposal.

	emptyVote := ToBeSignedEmptyVote{ProtocolMetadata: md}
	rawSig, err := emptyVote.Sign(e.Signer)
	if err != nil {
		e.Logger.Error("Failed signing message", zap.Error(err))
		return
	}

	emptyVoteRecord := NewEmptyVoteRecord(emptyVote)
	if err := e.WAL.Append(emptyVoteRecord); err != nil {
		e.Logger.Error("Failed appending empty vote", zap.Error(err))
		return
	}
	e.Logger.Debug("Persisted empty vote to WAL",
		zap.Uint64("round", round),
		zap.Int("size", len(emptyVoteRecord)))

	emptyVotes := e.getOrCreateEmptyVoteSetForRound(round)
	emptyVotes.timedOut = true

	signedEV := EmptyVote{Vote: emptyVote, Signature: Signature{Signer: e.ID, Value: rawSig}}

	// Add our own empty vote to the set
	emptyVotes.votes[string(e.ID)] = &signedEV

	e.Comm.Broadcast(&Message{EmptyVoteMessage: &signedEV})

	if err := e.maybeAssembleEmptyNotarization(); err != nil {
		e.Logger.Error("Failed assembling empty notarization", zap.Error(err))
		e.haltedError = err
	}
}

func (e *Epoch) monitorProgress(round uint64) {
	e.Logger.Debug("Monitoring progress", zap.Uint64("round", round))
	ctx, cancelContext := context.WithCancel(context.Background())

	noop := func() {}

	// If we have a task pending to be executed, remove it from execution because we're about to schedule
	// a task for a higher round.
	e.monitor.CancelTask()
	e.monitor.CancelFutureTask()

	// Since we're about to execute a task, abort the previous execution to make room for the new execution.
	e.cancelWaitForBlockNotarization()

	proposalWaitTimeExpired := func() {
		e.lock.Lock()
		defer e.lock.Unlock()
		e.triggerProposalWaitTimeExpired(round)
	}

	var cancelled atomic.Bool

	blockShouldBeBuiltNotification := func() {
		// This invocation blocks until the block builder tells us it's time to build a new block.
		e.BlockBuilder.IncomingBlock(ctx)
		// While we waited, a block might have been notarized.
		// If so, then don't start monitoring for it being notarized.
		if cancelled.Load() {
			return
		}

		e.Logger.Info("It is time to build a block", zap.Uint64("round", round))

		// Once it's time to build a new block, wait a grace period of 'e.maxProposalWait' time,
		// and if the monitor isn't cancelled by then, invoke proposalWaitTimeExpired() above.
		e.monitor.FutureTask(e.EpochConfig.MaxProposalWait, proposalWaitTimeExpired)
	}

	// Registers a wait operation that:
	// (1) Waits for the block builder to tell us it thinks it's time to build a new block.
	// (2) Registers a monitor which, if not cancelled earlier, notifies the Epoch about a timeout for this round.
	scheduled := e.monitor.RunTask(blockShouldBeBuiltNotification)
	if !scheduled {
		// If we fail monitoring for a block to be proposed by the leader, this is not an irrecoverable error,
		// because it might be that other nodes have succeeded.
		e.Logger.Warn("Failed monitoring leader progress")
		cancelContext()
		return
	}

	// If we notarize a block for this round we should cancel the monitor,
	// so first stop it and then cancel the context.
	e.cancelWaitForBlockNotarization = func() {
		e.monitor.CancelFutureTask()
		cancelled.Store(true)
		cancelContext()
		e.cancelWaitForBlockNotarization = noop
	}
}

func (e *Epoch) startRound() error {
	leaderForCurrentRound := LeaderForRound(e.nodes, e.round)

	if e.ID.Equals(leaderForCurrentRound) {
		e.buildBlock()
		return nil
	}

	// We're not the leader, make sure if a block is not notarized within a timely manner,
	// we will agree on an empty block.
	e.monitorProgress(e.round)

	// If we're not the leader, check if we have received a proposal earlier for this round
	msgsForRound, exists := e.futureMessages[string(leaderForCurrentRound)][e.round]
	if !exists || msgsForRound.proposal == nil {
		return nil
	}

	return e.handleBlockMessage(msgsForRound.proposal, leaderForCurrentRound)
}

func (e *Epoch) doProposed(block VerifiedBlock) error {
	vote, err := e.voteOnBlock(block)
	if err != nil {
		return err
	}

	md := block.BlockHeader()

	// We do not write the vote to the WAL as we have written the block itself to the WAL
	// and we can always restore the block and sign it again if needed.
	voteMsg := &Message{
		VoteMessage: &vote,
	}

	e.Logger.Debug("Broadcasting vote",
		zap.Uint64("round", md.Round),
		zap.Stringer("digest", md.Digest))

	e.Comm.Broadcast(voteMsg)
	// Send yourself a vote message
	return e.handleVoteMessage(&vote, e.ID)
}

func (e *Epoch) voteOnBlock(block VerifiedBlock) (Vote, error) {
	vote := ToBeSignedVote{BlockHeader: block.BlockHeader()}
	sig, err := vote.Sign(e.Signer)
	if err != nil {
		return Vote{}, fmt.Errorf("failed signing vote %w", err)
	}

	sv := Vote{
		Signature: Signature{
			Signer: e.ID,
			Value:  sig,
		},
		Vote: vote,
	}
	return sv, nil
}

// deletesRounds deletes all the rounds before [round] in the rounds map.
func (e *Epoch) deleteRounds(round uint64) {
	for i, r := range e.rounds {
		if r.num+e.maxRoundWindow < round {
			delete(e.rounds, i)
		}
	}
}

func (e *Epoch) deleteEmptyVoteForPreviousRound() {
	if e.round == 0 {
		return
	}
	delete(e.emptyVotes, e.round-1)
}

func (e *Epoch) increaseRound() {
	// In case we're waiting for a block to be notarized, cancel the wait because
	// we advanced to the next round.
	e.cancelWaitForBlockNotarization()

	e.deleteEmptyVoteForPreviousRound()

	leader := LeaderForRound(e.nodes, e.round)
	e.Logger.Info("Moving to a new round",
		zap.Uint64("old round", e.round),
		zap.Uint64("new round", e.round+1),
		zap.Stringer("leader", leader))
	e.round++
	if e.ID.Equals(NodeID{4}) {
		debug.PrintStack()
	}
}

func (e *Epoch) doNotarized(r uint64) error {
	if e.haveWeAlreadyTimedOutOnThisRound(r) {
		e.Logger.Info("We have already timed out on this round, will not finalize it", zap.Uint64("round", r))
		return e.startRound()
	}

	round := e.rounds[r]
	block := round.block

	md := block.BlockHeader()

	finalization, finalizationMsg, err := e.constructFinalizationMessage(md)
	if err != nil {
		return err
	}
	e.Comm.Broadcast(finalizationMsg)

	err1 := e.startRound()
	err2 := e.handleFinalizationMessage(&finalization, e.ID)

	return errors.Join(err1, err2)
}

func (e *Epoch) constructFinalizationMessage(md BlockHeader) (Finalization, *Message, error) {
	f := ToBeSignedFinalization{BlockHeader: md}
	signature, err := f.Sign(e.Signer)
	if err != nil {
		return Finalization{}, nil, fmt.Errorf("failed signing vote %w", err)
	}

	finalization := Finalization{
		Signature: Signature{
			Signer: e.ID,
			Value:  signature,
		},
		Finalization: ToBeSignedFinalization{
			BlockHeader: md,
		},
	}

	finalizationMsg := &Message{
		Finalization: &finalization,
	}
	return finalization, finalizationMsg, nil
}

// stores a notarization in the epoch's memory.
func (e *Epoch) storeNotarization(notarization Notarization) error {
	round := notarization.Vote.Round
	r, exists := e.rounds[round]
	if !exists {
		return fmt.Errorf("attempted to store notarization of a non existent round %d", round)
	}

	r.notarization = &notarization
	return nil
}

func (e *Epoch) maybeLoadFutureMessages() error {
	for {
		round := e.round
		height := e.Storage.Height()

		for from, messagesFromNode := range e.futureMessages {
			if msgs, exists := messagesFromNode[round]; exists {
				if msgs.proposal != nil {
					if err := e.handleBlockMessage(msgs.proposal, NodeID(from)); err != nil {
						return err
					}
				}
				if msgs.finalizationCertificate != nil {
					if err := e.handleFinalizationCertificateMessage(msgs.finalizationCertificate, NodeID(from)); err != nil {
						return err
					}
				}
				if msgs.vote != nil {
					if err := e.handleVoteMessage(msgs.vote, NodeID(from)); err != nil {
						return err
					}
				}
				if msgs.notarization != nil {
					if err := e.handleNotarizationMessage(msgs.notarization, NodeID(from)); err != nil {
						return err
					}
				}
				if msgs.finalization != nil {
					if err := e.handleFinalizationMessage(msgs.finalization, NodeID(from)); err != nil {
						return err
					}
				}
				if e.futureMessagesForRoundEmpty(msgs) {
					e.Logger.Debug("Deleting future messages",
						zap.Stringer("from", NodeID(from)), zap.Uint64("round", round))
					delete(messagesFromNode, round)
				}
			} else {
				//e.Logger.Debug("No future messages received for this round",
				//	zap.Stringer("from", NodeID(from)), zap.Uint64("round", round))
			}
		}

		emptyVotes, exists := e.emptyVotes[round]
		if exists {
			if emptyVotes.emptyNotarization != nil {
				if err := e.handleEmptyNotarizationMessage(emptyVotes.emptyNotarization); err != nil {
					return err
				}
			} else {
				for from, vote := range emptyVotes.votes {
					if err := e.handleEmptyVoteMessage(vote, NodeID(from)); err != nil {
						return err
					}
				}
			}
		}

		if e.round == round && height == e.Storage.Height() {
			return nil
		}
		e.Logger.Debug("Round or height was increased while processing future messages", zap.Uint64("epoch round", e.round), zap.Uint64("Round", round), zap.Uint64("height", height), zap.Uint64("storage height", e.Storage.Height()))
	}
}

func (e *Epoch) futureMessagesForRoundEmpty(msgs *messagesForRound) bool {
	return msgs.proposal == nil && msgs.vote == nil && msgs.finalization == nil &&
		msgs.notarization == nil && msgs.finalizationCertificate != nil
}

// storeProposal stores a block in the epochs memory(NOT storage).
// it creates a new round with the block and stores it in the rounds map.
func (e *Epoch) storeProposal(block VerifiedBlock) bool {
	md := block.BlockHeader()

	// Have we already received a block from that node?
	// If so, it cannot change its mind and send us a different block.
	if _, exists := e.rounds[md.Round]; exists {
		// We have already received a block for this round in the past, refuse receiving an alternative block.
		// We do this because we may have already voted for a different block.
		// Refuse processing the block to not be coerced into voting for a different block.
		e.Logger.Warn("Already received block for round", zap.Uint64("round", md.Round))
		return false
	}

	round := NewRound(block)
	e.rounds[md.Round] = round

	return true
}

// HandleRequest processes a request and returns a response. It also sends a response to the sender.
func (e *Epoch) handleReplicationRequest(req *ReplicationRequest, from NodeID) error {
	e.Logger.Debug("Received replication request", zap.Stringer("from", from), zap.Int("num seqs", len(req.Seqs)), zap.Uint64("latest round", req.LatestRound))
	if !e.ReplicationEnabled {
		return nil
	}
	response := &VerifiedReplicationResponse{}

	latestRound := e.getLatestVerifiedQuorumRound()

	if latestRound != nil && latestRound.GetRound() > req.LatestRound {
		response.LatestRound = latestRound
	}

	seqs := req.Seqs
	slices.Sort(seqs)
	data := make([]VerifiedQuorumRound, len(seqs))
	for i, seq := range seqs {
		quorumRound := e.locateQuorumRecord(seq)
		if quorumRound == nil {
			// since we are sorted, we can break early
			data = data[:i]
			break
		}

		data[i] = *quorumRound
	}

	response.Data = data
	msg := &Message{VerifiedReplicationResponse: response}
	e.Comm.SendMessage(msg, from)
	return nil
}

// locateQuorumRecord locates a block with a notarization or finalization certificate in the epochs memory or storage.
func (e *Epoch) locateQuorumRecord(seq uint64) *VerifiedQuorumRound {
	for _, round := range e.rounds {
		blockSeq := round.block.BlockHeader().Seq
		if blockSeq == seq {
			if round.fCert == nil && round.notarization == nil {
				break
			}
			return &VerifiedQuorumRound{
				VerifiedBlock: round.block,
				Notarization:  round.notarization,
				FCert:         round.fCert,
			}
		}
	}

	block, fCert, exists := e.Storage.Retrieve(seq)
	if exists {
		return &VerifiedQuorumRound{
			VerifiedBlock: block,
			FCert:         &fCert,
		}
	}

	return nil
}

func (e *Epoch) handleReplicationResponse(resp *ReplicationResponse, from NodeID) error {
	if !e.ReplicationEnabled {
		return nil
	}

	e.Logger.Debug("Received replication response", zap.Stringer("from", from), zap.Int("num seqs", len(resp.Data)), zap.Stringer("latest round", resp.LatestRound))
	nextSeqToCommit := e.Storage.Height()

	for _, data := range resp.Data {
		if err := data.IsWellFormed(); err != nil {
			e.Logger.Debug("Malformed Quorum Round Received", zap.Error(err))
			continue
		}

		if nextSeqToCommit > data.GetSequence() {
			e.Logger.Debug("Received quorum round for a seq that is too far behind", zap.Uint64("seq", data.GetSequence()))
			continue
		}

		if data.GetSequence() > nextSeqToCommit+e.maxRoundWindow {
			e.Logger.Debug("Received quorum round for a seq that is too far ahead", zap.Uint64("seq", data.GetSequence()))
			// we are too far behind, we should ignore this message
			continue
		}

		if err := e.verifyQuorumRound(data); err != nil {
			e.Logger.Debug("Received invalid quorum round", zap.Uint64("seq", data.GetSequence()), zap.Stringer("from", from))
			continue
		}

		e.replicationState.StoreQuorumRound(data)
	}

	if err := e.processLatestRoundReceived(resp.LatestRound); err != nil {
		e.Logger.Debug("Failed processing latest round", zap.Error(err))
		return nil
	}

	e.replicationState.receivedReplicationResponse(resp.Data, from)

	return e.processReplicationState()
}

func (e *Epoch) verifyQuorumRound(q QuorumRound) error {
	if err := q.Verify(); err != nil {
		return err
	}

	if q.FCert != nil {
		// extra check needed if we have a finalized block
		valid := IsFinalizationCertificateValid(e.eligibleNodeIDs, q.FCert, e.quorumSize, e.Logger)
		if !valid {
			return errors.New("invalid finalization certificate")
		}
	}

	return nil
}

func (e *Epoch) processEmptyNotarization(emptyNotarization *EmptyNotarization) error {
	emptyVotes := e.getOrCreateEmptyVoteSetForRound(emptyNotarization.Vote.Round)
	emptyVotes.emptyNotarization = emptyNotarization

	err := e.persistEmptyNotarization(emptyVotes.emptyNotarization, false)
	if err != nil {
		return err
	}

	return e.processReplicationState()
}

func (e *Epoch) processLatestRoundReceived(latestRound *QuorumRound) error {
	if latestRound == nil {
		return nil
	}

	// make sure the latest round is well formed
	if err := latestRound.IsWellFormed(); err != nil {
		e.Logger.Debug("Received invalid latest round", zap.Error(err))
		return err
	}

	if err := e.verifyQuorumRound(*latestRound); err != nil {
		e.Logger.Debug("Received invalid latest round", zap.Error(err))
		return err
	}

	e.replicationState.StoreQuorumRound(*latestRound)
	return nil
}

func (e *Epoch) processReplicationState() error {
	nextSeqToCommit := e.Storage.Height()

	// check if we are done replicating and should start a new round
	if e.replicationState.isReplicationComplete(nextSeqToCommit, e.round) {
		// TODO: an adversarial node can send multiple empty replication responses, causing us
		// to call start round multiple times. This is potentially bad if we are the leader, since we will
		// propose multiple blocks for the same round.
		return e.startRound()
	}

	e.replicationState.maybeCollectFutureSequences(e.Storage.Height())

	// first we check if we can commit the next sequence, it is ok to try and commit the next sequence
	// directly, since if there are any empty notarizations, `indexFinalizationCertificate` will
	// increment the round properly.
	block, fCert, exists := e.replicationState.GetFinalizedBlockForSequence(nextSeqToCommit)
	if exists {
		delete(e.replicationState.receivedQuorumRounds, block.BlockHeader().Round)
		return e.processFinalizedBlock(block, fCert)
	}

	qRound, ok := e.replicationState.receivedQuorumRounds[e.round]
	if ok && qRound.Notarization != nil {
		if qRound.FCert != nil {
			return nil
		}
		e.Logger.Debug("We cannot commit the next sequence, but we have a notarization for the current round",
			zap.Uint64("round", e.round), zap.Uint64("seq", nextSeqToCommit))
		delete(e.replicationState.receivedQuorumRounds, e.round)
		return e.processNotarizedBlock(qRound.Block, qRound.Notarization)
	}

	// the current round is an empty notarization
	if ok && qRound.EmptyNotarization != nil {
		delete(e.replicationState.receivedQuorumRounds, qRound.GetRound())
		return e.processEmptyNotarization(qRound.EmptyNotarization)
	}

	roundAdvanced, err := e.maybeAdvanceRoundFromEmptyNotarizations()
	if err != nil {
		return err
	}
	if roundAdvanced {
		return e.processReplicationState()
	}

	return nil
}

// maybeAdvanceRoundFromEmptyNotarizations advances the round if
// there is an empty notarization for the current sequence.
//
// For example, say we have the following QuorumRounds
//
//	QRound1 { round 1, seq 1 }
//	QRound2 { round 8, seq 1 }
//
// in this case we can infer there was 8-1 empty notarizations during rounds [2, 8].
func (e *Epoch) maybeAdvanceRoundFromEmptyNotarizations() (bool, error) {
	prevRound := e.round
	round := e.round
	expectedSeq := e.metadata().Seq

	nextSeqQuorum := e.replicationState.GetQuroumRoundWithSeq(expectedSeq)
	if nextSeqQuorum != nil {

		e.Logger.Debug("", zap.Uint64("nextSeqQuorumSeq", nextSeqQuorum.GetSequence()),
			zap.Uint64("nextSeqQuorumRound", nextSeqQuorum.GetRound()),
			zap.Uint64("expectedSeq", expectedSeq), zap.Uint64("previous round", prevRound))

		// num empty notarizations
		if round < nextSeqQuorum.GetRound() {
			for range nextSeqQuorum.GetRound() - round {
				e.increaseRound()
			}
			newRound := e.round
			e.Logger.Debug("Advancing round from empty notarization", zap.Uint64("nextSeqQuorumSeq", nextSeqQuorum.GetSequence()),
				zap.Uint64("nextSeqQuorumRound", nextSeqQuorum.GetRound()),
				zap.Uint64("expectedSeq", expectedSeq), zap.Uint64("previous round", prevRound), zap.Uint64("round", newRound))
			return true, nil
		}
	}

	// if there is no sequence, then maybe there is one with the same sequence but an empty notarization
	sameSeqQuorum := e.replicationState.GetQuroumRoundWithSeq(expectedSeq - 1)
	if sameSeqQuorum != nil && sameSeqQuorum.EmptyNotarization != nil {
		// num empty notarizations
		if round < nextSeqQuorum.GetRound() {
			for range sameSeqQuorum.GetRound() - round {
				e.increaseRound()
			}
			newRound := e.round
			e.Logger.Debug("Advancing round from empty notarization II", zap.Uint64("expectedSeq", expectedSeq), zap.Uint64("previous round", prevRound), zap.Uint64("round", newRound))
			return true, nil
		}
	}

	return false, nil
}

// getHighestRound returns the highest round that has either a notarization or finalization
func (e *Epoch) getHighestRound() *Round {
	var max uint64
	var found bool

	for _, round := range e.rounds {
		if round.num >= max {
			if round.notarization == nil && round.fCert == nil {
				continue
			}
			max = round.num
			found = true
		}
	}

	if found {
		return e.rounds[max]
	}

	return nil
}

func (e *Epoch) getHighestEmptyNotarization() *EmptyNotarization {
	var emptyNotarization *EmptyNotarization
	var max uint64
	for round, emptyVote := range e.emptyVotes {
		if round > max && emptyVote.emptyNotarization != nil {
			max = round
			emptyNotarization = emptyVote.emptyNotarization
		}
	}

	return emptyNotarization
}

func (e *Epoch) getLatestVerifiedQuorumRound() *VerifiedQuorumRound {
	return GetLatestVerifiedQuorumRound(
		e.getHighestRound(),
		e.getHighestEmptyNotarization(),
		e.lastBlock,
	)
}

// isRoundTooFarAhead returns true if [round] is more than `maxRoundWindow` rounds ahead of the current round.
func (e *Epoch) isRoundTooFarAhead(round uint64) bool {
	return round > e.round+e.maxRoundWindow
}

// isWithinMaxRoundWindow checks if [round] is within `maxRoundWindow` rounds ahead of the current round.
func (e *Epoch) isWithinMaxRoundWindow(round uint64) bool {
	return e.round < round && round-e.round < e.maxRoundWindow
}

func LeaderForRound(nodes []NodeID, r uint64) NodeID {
	n := len(nodes)
	return nodes[r%uint64(n)]
}

func Quorum(n int) int {
	f := (n - 1) / 3
	// Obtained from the equation:
	// Quorum * 2 = N + F + 1
	return (n+f)/2 + 1
}

// messagesFromNode maps nodeIds to the messages it sent in a given round.
type messagesFromNode map[string]map[uint64]*messagesForRound

type messagesForRound struct {
	proposal                *BlockMessage
	vote                    *Vote
	finalization            *Finalization
	finalizationCertificate *FinalizationCertificate
	notarization            *Notarization
}

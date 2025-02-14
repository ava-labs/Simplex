// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"simplex/record"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

const (
	defaultMaxRoundWindow   = 10
	DefaultMaxPendingBlocks = 20

	DefaultMaxProposalWaitTime = 5 * time.Second
)

type EmptyVoteSet struct {
	timedOut          bool
	votes             map[string]*EmptyVote
	emptyNotarization *EmptyNotarization
}

type Round struct {
	num           uint64
	block         Block
	votes         map[string]*Vote // NodeID --> vote
	notarization  *Notarization
	finalizations map[string]*Finalization // NodeID --> vote
	fCert         *FinalizationCertificate
}

func NewRound(block Block) *Round {
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
}

type Epoch struct {
	EpochConfig
	// Runtime
	sched                          *oneTimeBlockScheduler
	lock                           sync.Mutex
	lastBlock                      Block // latest block commited
	canReceiveMessages             atomic.Bool
	finishCtx                      context.Context
	finishFn                       context.CancelFunc
	nodes                          []NodeID
	eligibleNodeIDs                map[string]struct{}
	quorumSize                     int
	rounds                         map[uint64]*Round
	emptyVotes                     map[uint64]*EmptyVoteSet
	futureMessages                 messagesFromNode
	round                          uint64 // The current round we notarize
	maxRoundWindow                 uint64
	maxPendingBlocks               int
	monitor                        *Monitor
	cancelWaitForBlockNotarization context.CancelFunc
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
	//case msg.EmptyNotarization != nil:
	//	return e.handleEmptyNotarizationMessage(msg.EmptyNotarization, from)
	case msg.Finalization != nil:
		return e.handleFinalizationMessage(msg.Finalization, from)
	case msg.FinalizationCertificate != nil:
		return e.handleFinalizationCertificateMessage(msg.FinalizationCertificate, from)
	default:
		e.Logger.Warn("Invalid message type", zap.Stringer("from", from))
		return nil
	}
}

func (e *Epoch) init() error {
	e.sched = newOneTimeBlockScheduler(NewScheduler(e.Logger))
	e.monitor = NewMonitor(e.StartTime, e.Logger)
	e.cancelWaitForBlockNotarization = func() {}
	e.finishCtx, e.finishFn = context.WithCancel(context.Background())
	e.nodes = e.Comm.ListNodes()
	e.quorumSize = Quorum(len(e.nodes))
	e.rounds = make(map[uint64]*Round)
	e.emptyVotes = make(map[uint64]*EmptyVoteSet)
	e.maxRoundWindow = defaultMaxRoundWindow
	e.maxPendingBlocks = DefaultMaxPendingBlocks
	e.eligibleNodeIDs = make(map[string]struct{}, len(e.nodes))
	e.futureMessages = make(messagesFromNode, len(e.nodes))
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
	return e.setMetadataFromStorage()
}

func (e *Epoch) Start() error {
	// Only init receiving messages once you have initialized the data structures required for it.
	defer func() {
		e.canReceiveMessages.Store(true)
	}()
	return e.syncFromWal()
}

func (e *Epoch) syncBlockRecord(r []byte) error {
	block, err := BlockFromRecord(e.BlockDeserializer, r)
	if err != nil {
		return err
	}
	b := e.storeProposal(block)
	if !b {
		return fmt.Errorf("failed to store block from WAL")
	}

	e.Logger.Info("Block Proposal Recovered From WAL", zap.Uint64("Round", block.BlockHeader().Round), zap.Bool("stored", b))
	return nil
}

func (e *Epoch) syncNotarizationRecord(r []byte) error {
	notarization, err := NotarizationFromRecord(r, e.QCDeserializer)
	if err != nil {
		return err
	}
	e.Logger.Info("Notarization Recovered From WAL", zap.Uint64("Round", notarization.Vote.Round))
	return e.storeNotarization(notarization)
}

func (e *Epoch) syncEmptyNotarizationRecord(r []byte) error {
	emptyNotarization, err := EmptyNotarizationFromRecord(r, e.QCDeserializer)
	if err != nil {
		return err
	}

	emptyVotes := e.getOrCreateEmptyVoteSetForRound(emptyNotarization.Vote.Round)
	emptyVotes.emptyNotarization = &emptyNotarization
	return nil
}

func (e *Epoch) syncEmptyVoteRecord(r []byte) error {
	vote, err := ParseEmptyVoteRecord(r)
	if err != nil {
		return err
	}

	emptyVotes := e.getOrCreateEmptyVoteSetForRound(vote.Round)
	emptyVotes.timedOut = true
	return nil
}

func (e *Epoch) syncFinalizationRecord(r []byte) error {
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
				BlockMessage: &BlockMessage{
					Block: block,
					Vote:  vote,
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
		return errors.New("unknown record type")
	}
}

func (e *Epoch) setMetadataFromStorage() error {
	// load from storage if no notarization records
	block, _, err := RetrieveLastIndexFromStorage(e.Storage)
	if err != nil {
		return err
	}
	if block == nil {
		return nil
	}

	e.round = block.BlockHeader().Round + 1
	e.Epoch = block.BlockHeader().Epoch
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

// syncFromWal initializes an epoch from the write ahead log.
func (e *Epoch) syncFromWal() error {
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
			err = e.syncBlockRecord(r)
		case record.NotarizationRecordType:
			err = e.syncNotarizationRecord(r)
		case record.FinalizationRecordType:
			err = e.syncFinalizationRecord(r)
		case record.EmptyNotarizationRecordType:
			err = e.syncEmptyNotarizationRecord(r)
		case record.EmptyVoteRecordType:
			err = e.syncEmptyVoteRecord(r)
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
	block, _, err := RetrieveLastIndexFromStorage(e.Storage)
	if err != nil {
		return err
	}

	e.lastBlock = block
	return nil
}

func (e *Epoch) Stop() {
	e.finishFn()
}

func (e *Epoch) handleFinalizationCertificateMessage(message *FinalizationCertificate, from NodeID) error {

	e.Logger.Verbo("Received finalization certificate message",
		zap.Stringer("from", from), zap.Uint64("round", message.Finalization.Round))

	round, exists := e.rounds[message.Finalization.Round]
	if !exists {
		e.Logger.Debug("Received finalization certificate for a non existent round", zap.Int("round", int(message.Finalization.Round)))
		return nil
	}

	if round.fCert != nil {
		e.Logger.Debug("Received finalization for an already finalized round", zap.Uint64("round", message.Finalization.Round))
		return nil
	}

	valid, err := IsFinalizationCertificateValid(message, e.quorumSize, e.Logger)
	if err != nil {
		return err
	}
	if !valid {
		e.Logger.Debug("Received an invalid finalization certificate",
			zap.Int("round", int(message.Finalization.Round)),
			zap.Stringer("NodeID", from))
		return nil
	}
	round.fCert = message

	return e.persistFinalizationCertificate(*message)
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
	if e.round < finalization.Round && finalization.Round-e.round < e.maxRoundWindow {
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
		e.Logger.Debug("Got vote from a future round",
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
		zap.Stringer("from", from), zap.Uint64("round", vote.Round))

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

	// Check if we have timed out on this round
	if e.haveWeAlreadyTimedOutOnThisRound(vote.Round) {
		e.Logger.Debug("Received a vote but already timed out in that round", zap.Uint64("round", vote.Round), zap.Stringer("NodeID", from))
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
	if e.round < vote.Round && vote.Round-e.round < e.maxRoundWindow {
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

	if !e.isVoteValid(vote) {
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

func (e *Epoch) deleteFutureEmptyNotarization(from NodeID, round uint64) {
	msgsForRound, exists := e.futureMessages[string(from)][round]
	if !exists {
		return
	}
	msgsForRound.emptyNotarization = nil
}

func (e *Epoch) deleteFutureEmptyVote(from NodeID, round uint64) {
	msgsForRound, exists := e.futureMessages[string(from)][round]
	if !exists {
		return
	}
	msgsForRound.emptyVote = nil
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

func (e *Epoch) isFinalizationValid(signature []byte, finalization ToBeSignedFinalization, from NodeID) bool {
	if err := finalization.Verify(signature, e.Verifier, from); err != nil {
		e.Logger.Debug("Received a finalization with an invalid signature", zap.Uint64("round", finalization.Round), zap.Error(err))
		return false
	}
	return true
}

func (e *Epoch) isVoteValid(vote ToBeSignedVote) bool {
	// Ignore votes for previous rounds
	if vote.Round < e.round {
		return false
	}

	// Ignore votes for rounds too far ahead
	if vote.Round-e.round > e.maxRoundWindow {
		e.Logger.Debug("Received a vote for a too advanced round",
			zap.Uint64("round", vote.Round), zap.Uint64("my round", e.round))
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
	e.Logger.Debug("Progressing rounds due to commit", zap.Uint64("round", round))
	for e.round < round {
		e.increaseRound()
	}
}

func (e *Epoch) persistFinalizationCertificate(fCert FinalizationCertificate) error {
	// Check to see if we should commit this finalization to the storage as part of a block commit,
	// or otherwise write it to the WAL in order to commit it later.
	startRound := e.round
	nextSeqToCommit := e.Storage.Height()
	if fCert.Finalization.Seq == nextSeqToCommit {
		for {
			r := fCert.Finalization.Round
			block := e.rounds[r].block

			e.Storage.Index(block, fCert)
			e.Logger.Info("Committed block",
				zap.Uint64("round", fCert.Finalization.Round),
				zap.Uint64("sequence", fCert.Finalization.Seq),
				zap.Stringer("digest", fCert.Finalization.BlockHeader.Digest))
			e.lastBlock = block

			// We have commited because we have collected a finalization certificate.
			// However, we may have not witnessed a notarization.
			// Regardless of that, we can safely progress to the round succeeding the finalization.
			e.progressRoundsDueToCommit(fCert.Finalization.Round + 1)

			// If the round we're committing is too far in the past, don't keep it in the rounds cache.
			if fCert.Finalization.Round+e.maxRoundWindow < e.round {
				delete(e.rounds, fCert.Finalization.Round)
			}
			// Clean up the future messages - Remove all messages we may have stored for the round
			// the finalization is about.
			for _, messagesFromNode := range e.futureMessages {
				delete(messagesFromNode, fCert.Finalization.Round)
			}

			// Check if we can commit the next round
			r++
			round, exists := e.rounds[r]
			if !exists {
				break
			}
			if round.fCert == nil {
				break
			}
			fCert = *round.fCert
		}
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

	return e.persistEmptyNotarization(emptyNotarization)
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

func (e *Epoch) persistEmptyNotarization(emptyNotarization *EmptyNotarization) error {
	emptyNotarizationRecord := NewEmptyNotarizationRecord(emptyNotarization)
	if err := e.WAL.Append(emptyNotarizationRecord); err != nil {
		e.Logger.Error("Failed to append empty block record to WAL", zap.Error(err))
		return err
	}

	e.Logger.Debug("Persisted empty block to WAL",
		zap.Int("size", len(emptyNotarizationRecord)),
		zap.Uint64("round", emptyNotarization.Vote.Round))

	e.emptyVotes[emptyNotarization.Vote.Round].emptyNotarization = emptyNotarization

	notarizationMessage := &Message{EmptyNotarization: emptyNotarization}
	e.Comm.Broadcast(notarizationMessage)

	e.Logger.Debug("Broadcast empty block",
		zap.Uint64("round", emptyNotarization.Vote.Round))

	e.increaseRound()

	return errors.Join(e.startRound(), e.maybeLoadFutureMessages(e.round))
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
		return nil
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
		e.Logger.Verbo("Counting votes for the digest we received from the leader",
			zap.Uint64("round", e.round), zap.Int("votes", voteCount))
		return nil
	}

	notarization, err := NewNotarization(e.Logger, e.SignatureAggregator, votesForCurrentRound, block.BlockHeader())
	if err != nil {
		return err
	}

	return e.persistNotarization(notarization)
}

func (e *Epoch) persistNotarization(notarization Notarization) error {
	record := NewQuorumRecord(notarization.QC.Bytes(), notarization.Vote.Bytes(), record.NotarizationRecordType)

	if err := e.WAL.Append(record); err != nil {
		e.Logger.Error("Failed to append notarization record to WAL", zap.Error(err))
		return err
	}
	e.Logger.Debug("Persisted notarization to WAL",
		zap.Int("size", len(record)),
		zap.Uint64("round", notarization.Vote.Round),
		zap.Stringer("digest", notarization.Vote.BlockHeader.Digest))

	err := e.storeNotarization(notarization)
	if err != nil {
		return err
	}

	notarizationMessage := &Message{Notarization: &notarization}
	e.Comm.Broadcast(notarizationMessage)

	e.Logger.Debug("Broadcast notarization",
		zap.Uint64("round", notarization.Vote.Round),
		zap.Stringer("digest", notarization.Vote.BlockHeader.Digest))

	e.increaseRound()

	return errors.Join(e.doNotarized(notarization.Vote.Round), e.maybeLoadFutureMessages(e.round))
}

func (e *Epoch) handleNotarizationMessage(message *Notarization, from NodeID) error {
	vote := message.Vote

	e.Logger.Verbo("Received notarization message",
		zap.Stringer("from", from), zap.Uint64("round", vote.Round))

	// Ignore votes for previous rounds
	if vote.Round < e.round {
		e.Logger.Debug("Received a notarization for an earlier round", zap.Uint64("round", vote.Round))
		return nil
	}

	// Ignore votes for rounds too far ahead
	if vote.Round-e.round > e.maxRoundWindow {
		e.Logger.Debug("Received a notarization for a too advanced round",
			zap.Uint64("round", vote.Round), zap.Uint64("my round", e.round),
			zap.Stringer("NodeID", from))
		return nil
	}

	// Have we already notarized in this round?
	round, exists := e.rounds[vote.Round]
	if !exists {
		e.Logger.Debug("Received a notarization for a non existent round",
			zap.Stringer("NodeID", from))
		return nil
	}

	if round.notarization != nil {
		e.Logger.Debug("Received a notarization for an already notarized round",
			zap.Stringer("NodeID", from))
		return nil
	}

	if !e.isVoteValid(vote) {
		e.Logger.Debug("Notarization contains invalid vote",
			zap.Stringer("NodeID", from))
		return nil
	}

	if err := message.Verify(); err != nil {
		e.Logger.Debug("Notarization quorum certificate is invalid",
			zap.Stringer("NodeID", from), zap.Error(err))
		return nil
	}

	return e.persistNotarization(*message)
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
	if md.Round-e.round >= e.maxRoundWindow {
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

	// Check if we have timed out on this round
	if e.haveWeAlreadyTimedOutOnThisRound(md.Round) {
		e.Logger.Debug("Received a block but already timed out in that round", zap.Uint64("round", md.Round), zap.Stringer("NodeID", from))
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
	if e.round < md.Round && md.Round-e.round < e.maxRoundWindow {
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

	defer e.deleteFutureProposal(from, md.Round)

	// Create a task that will verify the block in the future, after its predecessors have also been verified.
	task := e.createBlockVerificationTask(block, from, vote)

	// isBlockReadyToBeScheduled checks if the block is known to us either from some previous round,
	// or from storage. If so, then we have verified it in the past, since only verified blocks are saved in memory.
	canBeImmediatelyVerified := e.isBlockReadyToBeScheduled(md.Seq, md.Prev)

	// Schedule the block to be verified once its direct predecessor have been verified,
	// or if it can be verified immediately.
	e.Logger.Debug("Scheduling block verification", zap.Uint64("round", md.Round))
	e.sched.Schedule(task, md.Prev, md.Round, canBeImmediatelyVerified)

	return nil
}

func (e *Epoch) createBlockVerificationTask(block Block, from NodeID, vote Vote) func() Digest {
	return func() Digest {
		md := block.BlockHeader()

		e.Logger.Debug("Block verification started", zap.Uint64("round", md.Round))
		start := time.Now()
		defer func() {
			elapsed := time.Since(start)
			e.Logger.Debug("Block verification ended", zap.Uint64("round", md.Round), zap.Duration("elapsed", elapsed))
		}()

		if err := block.Verify(); err != nil {
			e.Logger.Debug("Failed verifying block", zap.Error(err))
			return md.Digest
		}

		e.lock.Lock()
		defer e.lock.Unlock()

		record := BlockRecord(md, block.Bytes())
		e.WAL.Append(record)

		if !e.storeProposal(block) {
			e.Logger.Warn("Unable to store proposed block for the round", zap.Stringer("NodeID", from), zap.Uint64("round", md.Round))
			return md.Digest
			// TODO: timeout
		}

		e.deleteFutureProposal(from, md.Round)

		// Once we have stored the proposal, we have a Round object for the round.
		// We store the vote to prevent verifying its signature again.
		round, exists := e.rounds[md.Round]
		if !exists {
			// This shouldn't happen, but in case it does, return an error
			e.Logger.Error("programming error: round not found", zap.Uint64("round", md.Round))
			return md.Digest
		}
		round.votes[string(vote.Signature.Signer)] = &vote

		if err := e.doProposed(block); err != nil {
			e.Logger.Warn("Failed voting on block", zap.Error(err))
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
func (e *Epoch) locateBlock(seq uint64, digest []byte) (Block, bool) {
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
	e.sched.Schedule(task, metadata.Prev, metadata.Round, true)
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

func (e *Epoch) proposeBlock(block Block) error {
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
		BlockMessage: &BlockMessage{
			Block: block,
			Vote:  vote,
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

	return errors.Join(e.handleVoteMessage(&vote, e.ID), e.maybeLoadFutureMessages(md.Round))
}

func (e *Epoch) Metadata() ProtocolMetadata {
	e.lock.Lock()
	defer e.lock.Unlock()

	return e.metadata()
}

func (e *Epoch) metadata() ProtocolMetadata {
	var prev Digest
	seq := e.Storage.Height()
	if len(e.rounds) > 0 {
		// Build on top of the latest block
		currMed := e.getHighestRound().block.BlockHeader()
		prev = currMed.Digest
		seq = currMed.Seq + 1
	}

	if e.lastBlock != nil {
		currMed := e.lastBlock.BlockHeader()
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

	emptyVotes := e.getOrCreateEmptyVoteSetForRound(round)
	emptyVotes.timedOut = true

	signedEV := EmptyVote{Vote: emptyVote, Signature: Signature{Signer: e.ID, Value: rawSig}}

	// Add our own empty vote to the set
	emptyVotes.votes[string(e.ID)] = &signedEV

	e.Comm.Broadcast(&Message{EmptyVoteMessage: &signedEV})
}

func (e *Epoch) monitorProgress(round uint64) {
	e.Logger.Debug("Monitoring progress", zap.Uint64("round", round))
	ctx, cancelContext := context.WithCancel(context.Background())

	noop := func() {}

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
		stop := e.monitor.WaitUntil(e.EpochConfig.MaxProposalWait, proposalWaitTimeExpired)

		e.lock.Lock()
		defer e.lock.Unlock()

		// However, if the proposal is notarized before the wait time expires,
		// cancel the above wait procedure.
		e.cancelWaitForBlockNotarization = func() {
			stop()
			e.cancelWaitForBlockNotarization = noop
		}
	}

	// Registers a wait operation that:
	// (1) Waits for the block builder to tell us it thinks it's time to build a new block.
	// (2) Registers a monitor which, if not cancelled earlier, notifies the Epoch about a timeout for this round.
	e.monitor.WaitFor(blockShouldBeBuiltNotification)

	// If we notarize a block for this round we should cancel the monitor,
	// so first stop it and then cancel the context.
	e.cancelWaitForBlockNotarization = func() {
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

func (e *Epoch) doProposed(block Block) error {
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
	if err := e.handleVoteMessage(&vote, e.ID); err != nil {
		return err
	}
	return nil
}

func (e *Epoch) voteOnBlock(block Block) (Vote, error) {
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

func (e *Epoch) increaseRound() {
	// In case we're waiting for a block to be notarized, cancel the wait because
	// we advanced to the next round.
	e.cancelWaitForBlockNotarization()

	leader := LeaderForRound(e.nodes, e.round)
	e.Logger.Info("Moving to a new round",
		zap.Uint64("old round", e.round),
		zap.Uint64("new round", e.round+1),
		zap.Stringer("leader", leader))
	e.round++
}

func (e *Epoch) doNotarized(r uint64) error {
	round := e.rounds[r]
	block := round.block

	md := block.BlockHeader()

	f := ToBeSignedFinalization{BlockHeader: md}
	signature, err := f.Sign(e.Signer)
	if err != nil {
		return fmt.Errorf("failed signing vote %w", err)
	}

	sf := Finalization{
		Signature: Signature{
			Signer: e.ID,
			Value:  signature,
		},
		Finalization: ToBeSignedFinalization{
			BlockHeader: md,
		},
	}

	finalizationMsg := &Message{
		Finalization: &sf,
	}
	e.Comm.Broadcast(finalizationMsg)

	if e.haveWeAlreadyTimedOutOnThisRound(r) {
		e.Logger.Info("We have already timed out on this round, will not finalize it", zap.Uint64("round", r))
		return e.startRound()
	}

	err1 := e.startRound()
	err2 := e.handleFinalizationMessage(&sf, e.ID)

	return errors.Join(err1, err2)
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

func (e *Epoch) maybeLoadFutureMessages(round uint64) error {
	e.Logger.Debug("Loading messages received for this round in the past", zap.Uint64("round", round))

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
				if msgs.vote != nil {
					if err := e.handleVoteMessage(msgs.vote, NodeID(from)); err != nil {
						return err
					}
				}
				if msgs.finalization != nil {
					if err := e.handleFinalizationMessage(msgs.finalization, NodeID(from)); err != nil {
						return err
					}
				}

				if msgs.proposal == nil && msgs.vote == nil && msgs.finalization == nil {
					e.Logger.Debug("Deleting future messages",
						zap.Stringer("from", NodeID(from)), zap.Uint64("round", round))
					delete(messagesFromNode, round)
				}
			} else {
				e.Logger.Debug("No future messages received for this round",
					zap.Stringer("from", NodeID(from)), zap.Uint64("round", round))
			}
		}

		if e.round == round && height == e.Storage.Height() {
			return nil
		}
	}
}

// storeProposal stores a block in the epochs memory(NOT storage).
// it creates a new round with the block and stores it in the rounds map.
func (e *Epoch) storeProposal(block Block) bool {
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
	// We might have received votes and finalizations from future rounds before we received this block.
	// So load the messages into our round data structure now that we have created it.
	e.maybeLoadFutureMessages(md.Round)
	return true
}

func (e *Epoch) getHighestRound() *Round {
	var max uint64
	for _, round := range e.rounds {
		if round.num > max {
			if round.notarization == nil && round.fCert == nil {
				continue
			}
			max = round.num
		}
	}
	return e.rounds[max]
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
	emptyNotarization *EmptyNotarization
	emptyVote         *EmptyVote
	proposal          *BlockMessage
	vote              *Vote
	finalization      *Finalization
}

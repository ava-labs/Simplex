// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"time"

	"go.uber.org/zap"
)

type phase uint8

const (
	undefinedPhase phase = iota
	finalized
	proposed
	notarized
	shutdown
)

func (p phase) String() string {
	switch p {
	case finalized:
		return "finalized"
	case proposed:
		return "proposed"
	case notarized:
		return "notarized"
	case shutdown:
		return "shutdown"
	default:
		return "undefined"
	}
}

type recordType uint8

const (
	undefinedRecordType recordType = iota
	blockRecordType
	notarizationRecordType
	finalizationRecordType

	defaultMaxRoundWindow = 10
)

type Round struct {
	num           uint64
	block         Block
	votes         map[string]*SignedVoteMessage // NodeID --> vote
	notarization  *Notarization
	finalizations map[string]*SignedFinalizationMessage // NodeID --> vote
	fCert         *FinalizationCertificate
}

func NewRound(block Block) *Round {
	return &Round{
		num:           block.Metadata().Round,
		block:         block,
		votes:         make(map[string]*SignedVoteMessage),
		finalizations: make(map[string]*SignedFinalizationMessage),
	}
}

type Epoch struct {
	// Config
	Logger              Logger
	ID                  NodeID
	Signer              Signer
	Verifier            SignatureVerifier
	BlockDigester       BlockDigester
	BlockVerifier       BlockVerifier
	SignatureAggregator SignatureAggregator
	Comm                Communication
	Storage             Storage
	WAL                 WriteAheadLog
	BlockBuilder        BlockBuilder
	Round               uint64
	Seq                 uint64
	Epoch               uint64
	StartTime           time.Time
	// Runtime
	lastBlock           Block
	noBlockCommittedYet bool
	canReceiveMessages  bool
	finishCtx           context.Context
	finishFn            context.CancelFunc
	phase               phase
	nodes               []NodeID
	quorumSize          int
	eligibleNodeIDs     map[string]struct{}
	now                 time.Time
	rounds              map[uint64]*Round
	futureMessages      messagesFromNode
	round               uint64
	maxRoundWindow      uint64
}

// AdvanceTime hints the engine that the given amount of time has passed.
func (e *Epoch) AdvanceTime(t time.Duration) {

}

// HandleMessage notifies the engine about a reception of a message.
func (e *Epoch) HandleMessage(msg *Message, from NodeID) {
	// Guard against receiving messages before we are ready to handle them.
	if !e.canReceiveMessages {
		e.Logger.Warn("Cannot receive a message")
		return
	}

	// Guard against receiving messages from unknown nodes
	_, known := e.eligibleNodeIDs[string(from)]
	if !known {
		e.Logger.Warn("Received message from an unknown node", zap.Stringer("nodeID", from))
		return
	}

	if msg.BlockMessage != nil {
		e.handleBlockMessage(msg, from)
		return
	}

	if msg.VoteMessage != nil {
		e.handleVoteMessage(msg, from)
		return
	}

	if msg.Notarization != nil {
		e.handleNotarizationMessage(msg, from)
	}

	if msg.Finalization != nil {
		e.handleFinalizationMessage(msg, from)
	}

	if msg.FinalizationCertificate != nil {
		e.handleFinalizationCertificateMessage(msg, from)
	}

}

func (e *Epoch) Start() {
	// Only start receiving messages once you have initialized the data structures required for it.
	defer func() {
		e.canReceiveMessages = true
	}()

	e.finishCtx, e.finishFn = context.WithCancel(context.Background())
	e.phase = finalized // TODO: restore phase and msgStore from the WAL
	e.nodes = e.Comm.ListNodes()
	e.quorumSize = quorum(len(e.nodes))
	e.round = e.Round
	e.rounds = make(map[uint64]*Round)
	e.maxRoundWindow = defaultMaxRoundWindow
	e.eligibleNodeIDs = make(map[string]struct{}, len(e.nodes))
	e.futureMessages = make(messagesFromNode, len(e.nodes))
	for _, node := range e.nodes {
		e.futureMessages[string(node)] = make(map[uint64]*messagesForRound)
	}
	for _, node := range e.nodes {
		e.eligibleNodeIDs[string(node)] = struct{}{}
	}
	e.now = e.StartTime
	e.loadLastBlockFromStorage()
	e.doPhase()
}

func (e *Epoch) loadLastBlockFromStorage() {
	lastBlock, retrieved := e.retrieveLastBlockFromStorage()
	e.lastBlock, e.noBlockCommittedYet = lastBlock, !retrieved

	// Put the last block we committed in the rounds map.
	if retrieved {
		round := NewRound(lastBlock)
		e.rounds[round.num] = round
	}
}

func (e *Epoch) retrieveLastBlockFromStorage() (Block, bool) {
	var lastBlock Block
	var retrieved bool

	height := e.Storage.Height()
	if height > 0 {
		lastBlock, _, retrieved = e.Storage.Retrieve(height - 1)
		if !retrieved {
			e.Logger.Fatal("Failed retrieving last block from storage", zap.Uint64("seq", height-1))
		}
		return lastBlock, true
	}
	return lastBlock, false
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
	case undefinedPhase:
		panic("programming error: phase is undefined")
	}
}

func (e *Epoch) handleFinalizationCertificateMessage(message *Message, from NodeID) {
	fCert := message.FinalizationCertificate
	round, exists := e.rounds[fCert.Finalization.Round]
	if !exists {
		e.Logger.Debug("Received finalization certificate for a non existent round", zap.Int("round", int(fCert.Finalization.Round)))
		return
	}

	if round.fCert != nil {
		e.Logger.Debug("Received finalization for an already finalized round", zap.Uint64("round", fCert.Finalization.Round))
		return
	}

	if !e.isFinalizationCertificateValid(fCert) {
		e.Logger.Debug("Received an invalid finalization certificate",
			zap.Int("round", int(fCert.Finalization.Round)),
			zap.Stringer("NodeID", from))
		return
	}
	round.fCert = fCert

	e.persistFinalizationCertificate(*fCert)
}

func (e *Epoch) isFinalizationCertificateValid(fCert *FinalizationCertificate) bool {
	if fCert.AggregatedSignedVote != nil {
		if !e.isAggregateSigFinalizationCertValid(fCert) {
			return false
		}
	} else if len(fCert.SignaturesAndSigners) == 0 {
		if !e.isMultiSigFinalizationCertValid(fCert) {
			return false
		}
	} else {
		e.Logger.Debug("Received finalization without any signatures in it")
		return false
	}
	return true
}

func (e *Epoch) isAggregateSigFinalizationCertValid(fCert *FinalizationCertificate) bool {
	// Check enough signers signed the finalization certificate
	if e.quorumSize > len(fCert.AggregatedSignedVote.Signers) {
		e.Logger.Debug("Finalization certificate signed by insufficient nodes",
			zap.Int("count", len(fCert.SignaturesAndSigners)),
			zap.Int("quorum", e.quorumSize))
		return false
	}
	if e.hasSomeNodeSignedTwice(nil, fCert.AggregatedSignedVote.Signers) {
		return false
	}
	if !e.isFinalizationValid(fCert.AggregatedSignedVote.Signature, fCert.Finalization, fCert.AggregatedSignedVote.Signers...) {
		return false
	}
	return true
}

func (e *Epoch) isMultiSigFinalizationCertValid(fCert *FinalizationCertificate) bool {
	// Check enough signers signed the finalization certificate
	if e.quorumSize > len(fCert.SignaturesAndSigners) {
		e.Logger.Debug("Finalization certificate signed by insufficient nodes",
			zap.Int("count", len(fCert.SignaturesAndSigners)),
			zap.Int("quorum", e.quorumSize))
		return false
	}
	if e.hasSomeNodeSignedTwice(fCert.SignaturesAndSigners, nil) {
		return false
	}
	for _, sig := range fCert.SignaturesAndSigners {
		if e.isFinalizationValid(sig.Signature, fCert.Finalization, sig.Signer) {
			return false
		}
	}
	return true
}

func (e *Epoch) handleFinalizationMessage(message *Message, from NodeID) {
	msg := message.Finalization
	finalization := msg.Finalization

	// Only process a point to point finalization
	if !from.Equals(msg.Signer) {
		e.Logger.Debug("Received a finalization signed by a different party than sent it", zap.Stringer("signer", msg.Signer), zap.Stringer("sender", from))
		return
	}

	// Have we already finalized this round?
	round, exists := e.rounds[finalization.Round]
	if !exists {
		e.Logger.Debug("Received finalization for an unknown round", zap.Uint64("round", finalization.Round))
		return
	}

	if round.fCert != nil {
		e.Logger.Debug("Received finalization for an already finalized round", zap.Uint64("round", finalization.Round))
		return
	}

	if !e.isFinalizationValid(msg.Signature, finalization, from) {
		return
	}

	round.finalizations[string(from)] = msg

	e.maybeCollectFinalizationCertificate(round)
}

func (e *Epoch) handleVoteMessage(message *Message, from NodeID) {
	msg := message.VoteMessage
	vote := msg.Vote

	// Only process point to point votes
	if !from.Equals(msg.Signer) {
		e.Logger.Debug("Received a vote signed by a different party than sent it", zap.Stringer("signer", msg.Signer), zap.Stringer("sender", from))
		return
	}

	// TODO: what if we've received a vote for a round we didn't instantiate yet?
	round, exists := e.rounds[vote.Round]
	if !exists {
		e.Logger.Debug("Received a vote for a non existent round", zap.Uint64("round", vote.Round))
		return
	}

	if round.notarization != nil {
		e.Logger.Debug("Round already notarized", zap.Uint64("round", vote.Round))
		return
	}

	if !e.isVoteValid(msg.Signature, vote, from) {
		return
	}

	e.rounds[vote.Round].votes[string(from)] = msg

	e.maybeCollectNotarization()
}

func (e *Epoch) isFinalizationValid(signature []byte, finalization Finalization, from ...NodeID) bool {
	// First before verifying the signature, check the sequence and digest match what we think it should,
	// according to the notarized chain of blocks.

	if err := finalization.Verify(signature, e.Verifier, from...); err != nil {
		e.Logger.Debug("Received a finalization with an invalid signature", zap.Uint64("round", finalization.Round), zap.Error(err))
		return false
	}
	return true
}

func (e *Epoch) isVoteValid(signature []byte, vote Vote, from ...NodeID) bool {
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

	if err := vote.Verify(signature, e.Verifier, from...); err != nil {
		return false
	}
	return true
}

func (e *Epoch) maybeCollectFinalizationCertificate(round *Round) {
	finalizationCount := len(round.finalizations)

	if finalizationCount < e.quorumSize {
		e.Logger.Verbo("Counting finalizations", zap.Uint64("round", e.round), zap.Int("votes", finalizationCount))
		return
	}

	e.assembleFinalizationCertificate(round)
}

func (e *Epoch) assembleFinalizationCertificate(round *Round) {
	// Divide finalizations into sets that agree on the same metadata
	finalizationsByMD := make(map[string][]*SignedFinalizationMessage)

	for _, vote := range round.finalizations {
		key := string(vote.Finalization.Bytes())
		finalizationsByMD[key] = append(finalizationsByMD[key], vote)
	}

	var finalizations []*SignedFinalizationMessage

	for _, finalizationsWithTheSameDigest := range finalizationsByMD {
		if len(finalizationsWithTheSameDigest) >= e.quorumSize {
			finalizations = finalizationsWithTheSameDigest
			break
		}
	}

	if len(finalizations) == 0 {
		e.Logger.Debug("Could not find enough finalizations for the same metadata")
		return
	}

	finalization := finalizations[0]

	voteCount := len(finalizations)

	signers := make([]NodeID, 0, voteCount)
	signatures := make([][]byte, 0, voteCount)
	e.Logger.Info("Collected quorum of votes", zap.Uint64("round", e.round), zap.Int("votes", voteCount))
	for _, vote := range finalizations {
		// TODO: ensure all finalizations agree on the same metadata!
		e.Logger.Debug("Collected finalization from node", zap.Stringer("NodeID", vote.Signer))
		signatures = append(signatures, vote.Signer)
		signers = append(signers, vote.Signer)
	}

	var fCert FinalizationCertificate
	fCert.Finalization = finalization.Finalization

	if e.SignatureAggregator != nil {
		signatures = [][]byte{e.SignatureAggregator.Aggregate(signatures)}
		fCert.AggregatedSignedVote = &AggregatedSignedVote{
			Signers:   signers,
			Signature: signatures[0],
		}
	} else {
		for _, v := range finalizations {
			fCert.SignaturesAndSigners = append(fCert.SignaturesAndSigners, &SignatureSignerPair{
				Signature: v.Signature,
				Signer:    v.Signer,
			})
		}
	}

	round.fCert = &fCert

	e.persistFinalizationCertificate(fCert)
}

func (e *Epoch) persistFinalizationCertificate(fCert FinalizationCertificate) {
	signatures := make([][]byte, 0, e.quorumSize)
	signers := make([]NodeID, 0, e.quorumSize)

	if fCert.AggregatedSignedVote != nil {
		signatures = [][]byte{fCert.AggregatedSignedVote.Signature}
		for _, signer := range fCert.AggregatedSignedVote.Signers {
			signers = append(signers, signer)
		}
	} else if len(fCert.SignaturesAndSigners) > 0 {
		for _, sig := range fCert.SignaturesAndSigners {
			signers = append(signers, sig.Signer)
			signatures = append(signatures, sig.Signature)
		}
	}

	// Check to see if we should commit this finalization to the storage as part of a block commit,
	// or otherwise write it to the WAL in order to commit it later.
	nextSeqToCommit := e.Storage.Height()
	if fCert.Finalization.Seq == nextSeqToCommit {
		block := e.rounds[fCert.Finalization.Round].block
		e.Storage.Index(fCert.Finalization.Seq, block, fCert)
		e.Logger.Info("Committed block",
			zap.Uint64("round", fCert.Finalization.Round),
			zap.Uint64("sequence", fCert.Finalization.Seq),
			zap.Stringer("digest", fCert.Finalization.Metadata.Digest))
		e.noBlockCommittedYet = false
		e.lastBlock = block

		// If the round we're committing is too far in the past, don't keep it in the rounds cache.
		if fCert.Finalization.Round+e.maxRoundWindow < e.round {
			delete(e.rounds, fCert.Finalization.Round)
		}
		// Clean up the future messages - Remove all messages we may have stored for the round
		// the finalization is about.
		for _, messagesFromNode := range e.futureMessages {
			delete(messagesFromNode, fCert.Finalization.Round)
		}
	} else {
		record := quorumRecord(signatures, signers, fCert.Finalization.Bytes(), uint16(finalizationRecordType))
		e.WAL.Append(&record)

		e.Logger.Debug("Persisted finalization certificate to WAL",
			zap.Int("size", record.Length()),
			zap.Uint64("round", fCert.Finalization.Round),
			zap.Stringer("digest", fCert.Finalization.Metadata.Digest))
	}

	finalizationCertificate := &Message{FinalizationCertificate: &fCert}
	e.Comm.Broadcast(finalizationCertificate)

	e.Logger.Debug("Broadcast finalization certificate",
		zap.Uint64("round", fCert.Finalization.Round),
		zap.Stringer("digest", fCert.Finalization.Metadata.Digest))

	e.phase = finalized
	e.doPhase()
}

func (e *Epoch) maybeCollectNotarization() {
	votesForCurrentRound := e.rounds[e.round].votes
	voteCount := len(votesForCurrentRound)

	if voteCount < e.quorumSize {
		e.Logger.Verbo("Counting votes", zap.Uint64("round", e.round), zap.Int("votes", voteCount))
		return
	}

	// TODO: store votes before receiving the block

	block := e.rounds[e.round].block
	digestWeExpect := block.Metadata().Digest

	// Ensure we have enough votes for the same digest
	var voteCountForOurDigest int
	for _, vote := range votesForCurrentRound {
		if bytes.Equal(digestWeExpect, vote.Vote.Digest) {
			voteCountForOurDigest++
		}
	}

	if voteCountForOurDigest < e.quorumSize {
		e.Logger.Verbo("Counting votes for the digest we received from the leader",
			zap.Uint64("round", e.round), zap.Int("votes", voteCount))
		return
	}

	e.assembleNotarization(votesForCurrentRound, digestWeExpect)
}

func (e *Epoch) assembleNotarization(votesForCurrentRound map[string]*SignedVoteMessage, digest []byte) {
	vote := Vote{
		Metadata{
			ProtocolMetadata: ProtocolMetadata{
				Epoch: e.Epoch,
				Round: e.round,
			},
			Digest: digest,
		},
	}

	voteCount := len(votesForCurrentRound)

	signers := make([]NodeID, 0, voteCount)
	signatures := make([][]byte, 0, voteCount)
	e.Logger.Info("Collected quorum of votes", zap.Uint64("round", e.round), zap.Int("votes", voteCount))
	for _, vote := range votesForCurrentRound {
		e.Logger.Debug("Collected vote from node", zap.Stringer("NodeID", vote.Signer))
		signatures = append(signatures, vote.Signer)
		signers = append(signers, vote.Signer)
	}

	var notarization Notarization
	notarization.Vote = vote

	if e.SignatureAggregator != nil {
		signatures = [][]byte{e.SignatureAggregator.Aggregate(signatures)}
		notarization.AggregatedSignedVote = &AggregatedSignedVote{
			Signers:   signers,
			Signature: signatures[0],
		}
	} else {
		for _, v := range votesForCurrentRound {
			notarization.SignaturesAndSigners = append(notarization.SignaturesAndSigners, &SignatureSignerPair{
				Signature: v.Signature,
				Signer:    v.Signer,
			})
		}
	}

	e.storeNotarization(notarization)
	e.persistNotarization(notarization, signatures, signers, vote)
}

func (e *Epoch) persistNotarization(notarization Notarization, signatures [][]byte, signers []NodeID, vote Vote) {
	notarizationMessage := &Message{Notarization: &notarization}

	record := quorumRecord(signatures, signers, vote.Bytes(), uint16(notarizationRecordType))

	e.WAL.Append(&record)

	e.Logger.Debug("Persisted notarization to WAL",
		zap.Int("size", record.Length()),
		zap.Uint64("round", notarization.Vote.Round),
		zap.Stringer("digest", notarization.Vote.Metadata.Digest))

	e.Comm.Broadcast(notarizationMessage)

	e.Logger.Debug("Broadcast notarization",
		zap.Uint64("round", notarization.Vote.Round),
		zap.Stringer("digest", notarization.Vote.Metadata.Digest))

	e.phase = notarized
	e.rounds[notarization.Vote.Round].notarization = &notarization
	e.doPhase()
}

func (e *Epoch) handleNotarizationMessage(message *Message, from NodeID) {
	msg := message.Notarization
	vote := msg.Vote

	// Ignore votes for previous rounds
	if vote.Round < e.round {
		e.Logger.Debug("Received a notarization for an earlier round", zap.Uint64("round", vote.Round))
		return
	}

	// Ignore votes for rounds too far ahead
	if vote.Round-e.round > e.maxRoundWindow {
		e.Logger.Debug("Received a notarization for a too advanced round",
			zap.Uint64("round", vote.Round), zap.Uint64("my round", e.round),
			zap.Stringer("NodeID", from))
		return
	}

	// Have we already notarized in this round?
	round, exists := e.rounds[vote.Round]
	if !exists {
		e.Logger.Debug("Received a notarization for a non existent round",
			zap.Stringer("NodeID", from))
		return
	}

	if round.notarization != nil {
		e.Logger.Debug("Received a notarization for an already notarized round",
			zap.Stringer("NodeID", from))
		return
	}

	signatures := make([][]byte, 0, e.quorumSize)
	signers := make([]NodeID, 0, e.quorumSize)

	if msg.AggregatedSignedVote != nil {
		if !e.isVoteValid(msg.AggregatedSignedVote.Signature, vote, msg.AggregatedSignedVote.Signers...) {
			e.Logger.Debug("Notarization contains invalid vote",
				zap.String("NodeIDs", fmt.Sprintf("%s", msg.AggregatedSignedVote.Signers)),
				zap.Stringer("NodeID", from))
			return
		}
		signatures = [][]byte{msg.AggregatedSignedVote.Signature}
		signers = msg.AggregatedSignedVote.Signers
	} else if len(msg.SignaturesAndSigners) >= e.quorumSize {
		// Deduplicate the signed votes - make sure that each node signed only once.
		if e.hasSomeNodeSignedTwice(msg.SignaturesAndSigners, nil) {
			return
		}
		for _, ssp := range msg.SignaturesAndSigners {
			if !e.isVoteValid(ssp.Signature, vote, ssp.Signer) {
				e.Logger.Debug("Notarization contains invalid vote",
					zap.Stringer("NodeID", ssp.Signer),
					zap.Stringer("NodeID", from))
				return
			}
			signers = append(signers, ssp.Signer)
			signatures = append(signatures, ssp.Signature)
		}
	} else {
		e.Logger.Debug("Got message that is neither an aggregated signed vote nor contains enough votes",
			zap.Stringer("NodeID", from))
		return
	}

	e.persistNotarization(*msg, signatures, signers, vote)
}

func (e *Epoch) hasSomeNodeSignedTwice(sigSignPairs []*SignatureSignerPair, nodeIDs []NodeID) bool {
	if len(sigSignPairs) > 0 && len(nodeIDs) > 0 {
		panic(fmt.Sprintf("programming error: expected either sigSignPairs or nodeIDs to be used but not both"))
	}
	seen := make(map[string]struct{}, len(sigSignPairs))
	for _, ssp := range sigSignPairs {
		if _, alreadySeen := seen[string(ssp.Signer)]; alreadySeen {
			e.Logger.Warn("Observed a signature originating at least twice from the same node")
			return true
		}
		seen[string(ssp.Signer)] = struct{}{}
	}

	for _, nodeID := range nodeIDs {
		if _, alreadySeen := seen[string(nodeID)]; alreadySeen {
			e.Logger.Warn("Observed a signature originating at least twice from the same node")
			return true
		}
		seen[string(nodeID)] = struct{}{}
	}

	return false
}

func (e *Epoch) handleBlockMessage(message *Message, from NodeID) {
	block := message.BlockMessage.Block
	if block == nil {
		e.Logger.Debug("Got empty block in a BlockMessage")
		return
	}

	md := block.Metadata()

	// Check that the node is a leader for the round corresponding to the block.
	if !leaderForRound(e.nodes, md.Round).Equals(from) {
		// The block is associated with a round in which the sender is not the leader,
		// it should not be sending us any block at all.
		e.Logger.Debug("Got block from a block proposer that is not the leader of the round", zap.Stringer("NodeID", from), zap.Uint64("round", md.Round))
		return
	}

	// If this is a message from a more advanced round,
	// only store it if `maxRoundWindow` ahead.
	if e.round < md.Round && md.Round-e.round > e.maxRoundWindow {
		e.Logger.Debug("Got block from round too far in the future", zap.Uint64("round", md.Round), zap.Uint64("my round", e.round))
		msgsForRound, exists := e.futureMessages[string(from)][md.Round]
		if !exists {
			msgsForRound = &messagesForRound{}
			e.futureMessages[string(from)][md.Round] = msgsForRound
		}
		msgsForRound.proposal = message
		return
	}

	if !e.isMetadataValid(block) {
		e.Logger.Debug("Got invalid block in a BlockMessage")
		return
	}

	if !e.storeProposal(block, from) {
		// TODO: timeout
	}

	// If this is a block we have proposed, don't write it to the WAL
	// because we have done so right before sending it.
	// Also, don't bother verifying it.
	// Else, it's a block that we have received from the leader of this round.
	// So verify it and store it in the WAL.
	if !e.ID.Equals(from) {
		if err := e.BlockVerifier.VerifyBlock(block); err != nil {
			e.Logger.Debug("Failed verifying block", zap.Error(err))
			return
		}
		record := blockRecord(md, block.Bytes())
		e.WAL.Append(&record)
	}

	e.phase = proposed

	e.doPhase()
}

func (e *Epoch) isMetadataValid(block Block) bool {
	md := block.Metadata()

	expectedDigest := e.BlockDigester.Digest(block)

	if md.Version != 0 {
		e.Logger.Debug("Got block message with wrong version number, expected 0", zap.Uint8("version", md.Version))
	}

	if e.Epoch != md.Epoch {
		e.Logger.Debug("Got block message but the epoch mismatches our epoch",
			zap.Uint64("our epoch", e.Epoch), zap.Uint64("block epoch", md.Epoch))
	}

	if !bytes.Equal(md.Digest, expectedDigest) {
		e.Logger.Debug("Received block with an incorrect digest",
			zap.Uint64("round", md.Round),
			zap.Stringer("digest", md.Digest),
			zap.String("expected digest", fmt.Sprintf("%x", expectedDigest[:10])))
	}

	if md.Seq == 0 && e.Storage.Height() > 0 {
		// We have already committed the first block, no need to commit it again.
		return false
	}

	var expectedSeq uint64
	var expectedPrevDigest []byte

	// Else, either it's not the first block, or we haven't committed the first block, and it is the first block.
	// If it's the latter we have nothing else to do.
	// If it's the former, we need to find the parent of the block and ensure it is correct.
	if md.Seq > 0 {
		// TODO: we should cache this data, we don't need the block, just the hash and sequence.
		_, found := e.locateBlock(md.Seq-1, md.Prev)
		if !found {
			// We could not find the parent block, so no way to verify this proposal.
			return false
		}

		// TODO: we need to take into account dummy blocks!

		expectedSeq = md.Seq
		expectedPrevDigest = md.Prev
	}

	if md.Seq != expectedSeq {
		e.Logger.Debug("Received block with an incorrect sequence",
			zap.Uint64("round", md.Round),
			zap.Uint64("seq", md.Seq),
			zap.Uint64("expected seq", expectedSeq))
	}

	expectedMD := Metadata{
		Digest: expectedDigest,
		ProtocolMetadata: ProtocolMetadata{
			Round:   e.round,
			Seq:     expectedSeq,
			Epoch:   e.Epoch,
			Prev:    expectedPrevDigest,
			Version: 0,
		},
	}

	return expectedMD.Equals(&md)
}

// locateBlock locates a block:
// 1) In memory
// 2) Else, on storage.
// Compares to the given digest, and if it's the same, returns it.
// Otherwise, returns false.
func (e *Epoch) locateBlock(seq uint64, digest []byte) (Block, bool) {
	// TODO index rounds by digest too to make it quicker
	round, exists := e.rounds[seq]
	if exists {
		if bytes.Equal(round.block.Metadata().Digest, digest) {
			return round.block, true
		}
		return nil, false
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

	if bytes.Equal(block.Metadata().Digest, digest) {
		return block, true
	}

	return nil, false
}

func (e *Epoch) proposeBlock() {
	block, ok := e.BlockBuilder.BuildBlock(e.finishCtx, e.Metadata())
	if !ok {
		return
	}

	md := block.Metadata()

	// Write record to WAL before broadcasting it, so that
	// if we crash during broadcasting, we know what we sent.

	rawBlock := block.Bytes()
	record := blockRecord(block.Metadata(), rawBlock)
	e.WAL.Append(&record)
	e.Logger.Debug("Wrote block to WAL",
		zap.Uint64("round", md.Round),
		zap.Int("size", len(rawBlock)),
		zap.Stringer("digest", md.Digest))

	proposal := &Message{
		BlockMessage: &BlockMessage{
			Block: block,
		},
	}

	e.Comm.Broadcast(proposal)
	e.Logger.Debug("Proposal broadcast",
		zap.Uint64("round", md.Round),
		zap.Int("size", len(rawBlock)),
		zap.Stringer("digest", md.Digest))

	e.handleBlockMessage(proposal, e.ID)
}

func (e *Epoch) Metadata() ProtocolMetadata {
	var prev []byte
	seq := e.Storage.Height()
	if !e.noBlockCommittedYet {
		// Build on top of the latest block
		currMed := e.getHighestRound().block.Metadata()
		prev = currMed.Digest
		seq = currMed.Seq + 1
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

func (e *Epoch) doFinalized() {
	leaderForCurrentRound := leaderForRound(e.nodes, e.round)

	if e.ID.Equals(leaderForCurrentRound) {
		e.proposeBlock()
		return
	}

	// If we're not the leader, check if we have received a proposal earlier for this round
	msgsForRound, exists := e.futureMessages[string(leaderForCurrentRound)][e.round]
	if !exists || msgsForRound.proposal == nil || msgsForRound.proposal.BlockMessage == nil {
		return
	}

	e.handleBlockMessage(msgsForRound.proposal, leaderForCurrentRound)
}

func (e *Epoch) doProposed() {
	block := e.rounds[e.round].block

	vote := Vote{Metadata: block.Metadata()}
	sig, err := vote.Sign(e.Signer)
	if err != nil {
		e.Logger.Fatal("Failed signing vote", zap.Error(err))
	}

	sv := SignedVoteMessage{
		Signature: sig,
		Signer:    e.ID,
		Vote:      vote,
	}

	md := block.Metadata()

	// We do not write the vote to the WAL as we have written the block itself to the WAL
	// and we can always restore the block and sign it again if needed.
	voteMsg := &Message{
		VoteMessage: &sv,
	}

	e.Logger.Debug("Broadcasting vote",
		zap.Uint64("round", md.Round),
		zap.Stringer("digest", md.Digest))

	e.Comm.Broadcast(voteMsg)
	// Send yourself a vote message
	e.handleVoteMessage(voteMsg, e.ID)
}

func (e *Epoch) increaseRound() {
	e.Logger.Info(fmt.Sprintf("Moving to a new round (%d --> %d", e.round, e.round+1), zap.Uint64("round", e.round+1))
	e.round++
}

func (e *Epoch) doNotarized() {
	round := e.rounds[e.round]
	block := round.block

	defer e.increaseRound()

	md := block.Metadata()

	f := Finalization{Metadata: md}
	signature, err := f.Sign(e.Signer)
	if err != nil {
		e.Logger.Fatal("Failed signing vote", zap.Error(err))
	}

	sf := SignedFinalizationMessage{
		Signature: signature,
		Signer:    e.ID,
		Finalization: Finalization{
			Metadata: md,
		},
	}

	finalizationMsg := &Message{
		Finalization: &sf,
	}

	e.Comm.Broadcast(finalizationMsg)
	e.handleFinalizationMessage(finalizationMsg, e.ID)
}

func (e *Epoch) storeNotarization(notarization Notarization) {
	round := notarization.Vote.Round
	r, exists := e.rounds[round]
	if !exists {
		e.Logger.Fatal("Programming error: Attempted to store notarization of a non existent round",
			zap.Uint64("round", round))
	}

	r.notarization = &notarization
}

func (e *Epoch) maybeLoadFutureMessages(round uint64) {
	for from, messagesFromNode := range e.futureMessages {
		if msgs, exists := messagesFromNode[round]; exists {
			e.handleVoteMessage(msgs.vote, NodeID(from))
		}
	}

	for from, messagesFromNode := range e.futureMessages {
		if msgs, exists := messagesFromNode[round]; exists {
			e.handleFinalizationMessage(msgs.finalization, NodeID(from))
		}
	}
}

func (e *Epoch) storeProposal(block Block, from NodeID) bool {
	md := block.Metadata()

	// Don't bother processing blocks from the past
	if e.round > md.Round {
		return false
	}

	// Have we already received a block from that node?
	// If so, it cannot change its mind and send us a different block.
	round, exists := e.rounds[md.Round]
	if !exists {
		round = NewRound(block)
		e.rounds[md.Round] = round
		// We might have receied votes and finalizations from future rounds before we received this block.
		// So load the messages into our round data structure now that we have created it.
		defer e.maybeLoadFutureMessages(md.Round)
	} else {
		// We have already received a block for this round in the past, refuse receiving an alternative block.
		// We do this because we may have already voted for a different block.
		// Refuse processing the block to not be coerced into voting for a different block.
		e.Logger.Warn("Already received a block for the round", zap.Stringer("NodeID", from), zap.Uint64("round", md.Round))
		return false
	}
	return true
}

func (e *Epoch) getHighestRound() *Round {
	var max uint64
	for _, round := range e.rounds {
		if round.num > max {
			max = round.num
		}
	}
	return e.rounds[max]
}

func leaderForRound(nodes []NodeID, r uint64) NodeID {
	n := len(nodes)
	return nodes[r%uint64(n)]
}

func quorum(n int) int {
	// Obtained from the equation:
	// Quorum * 2 = N + F + 1
	return int(math.Ceil(float64((n + (n-1)/3 + 1) / 2.0)))
}

type messagesFromNode map[string]map[uint64]*messagesForRound

type messagesForRound struct {
	proposal     *Message
	vote         *Message
	finalization *Message
}

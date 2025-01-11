// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
)

const defaultMaxRoundWindow = 10

type Round struct {
	num          uint64
	block        Block
	votes        map[string]*Vote // NodeID --> vote
	notarization *Quorum
	finalizes    map[string]*Vote // NodeID --> vote
	finalization *Quorum
}

func NewRound(block Block) *Round {
	return &Round{
		num:       block.BlockHeader().Round,
		block:     block,
		votes:     make(map[string]*Vote),
		finalizes: make(map[string]*Vote),
	}
}

type EpochConfig struct {
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
	Round               uint64
	Seq                 uint64
	Epoch               uint64
	StartTime           time.Time
}

type Epoch struct {
	EpochConfig
	// Runtime
	lastBlock          Block // latest block commited
	canReceiveMessages bool
	finishCtx          context.Context
	finishFn           context.CancelFunc
	nodes              []NodeID
	eligibleNodeIDs    map[string]struct{}
	quorumSize         int
	rounds             map[uint64]*Round
	futureMessages     messagesFromNode
	round              uint64 // The current round we notarize
	maxRoundWindow     uint64
}

func NewEpoch(conf EpochConfig) (*Epoch, error) {
	e := &Epoch{
		EpochConfig: conf,
	}
	return e, e.init()
}

// AdvanceTime hints the engine that the given amount of time has passed.
func (e *Epoch) AdvanceTime(t time.Duration) {

}

// HandleMessage notifies the engine about a reception of a message.
func (e *Epoch) HandleMessage(msg *Message, from NodeID) error {
	// Guard against receiving messages before we are ready to handle them.
	if !e.canReceiveMessages {
		e.Logger.Warn("Cannot receive a message")
		return nil
	}

	// Guard against receiving messages from unknown nodes
	_, known := e.eligibleNodeIDs[string(from)]
	if !known {
		e.Logger.Warn("Received message from an unknown node", zap.Stringer("nodeID", from))
		return nil
	}

	switch {
	case msg.Proposal != nil:
		return e.handleBlockMessage(msg.Proposal, from)
	case msg.Vote != nil:
		return e.handleVoteMessage(msg.Vote, from)
	case msg.Notarization != nil:
		return e.handleNotarizationMessage(msg.Notarization, from)
	case msg.Finalize != nil:
		return e.handleFinalizeMessage(msg.Finalize, from)
	case msg.Finalization != nil:
		return e.handleFinalizationMessage(msg.Finalization, from)
	default:
		return fmt.Errorf("invalid message type: %v", msg)
	}
}

func (e *Epoch) init() error {
	// Only init receiving messages once you have initialized the data structures required for it.
	defer func() {
		e.canReceiveMessages = true
	}()

	e.finishCtx, e.finishFn = context.WithCancel(context.Background())
	e.nodes = e.Comm.ListNodes()
	e.quorumSize = QuorumSize(len(e.nodes))
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

	err := e.loadLastBlock()
	if err != nil {
		return err
	}

	e.loadLastRound()
	return nil
}

func (e *Epoch) Start() error {
	return e.syncFromWal()
}

// syncFromWal init an epoch from the write ahead log.
func (e *Epoch) syncFromWal() error {
	return e.startRound()
}

// loadLastBlock initializes the epoch with the lastBlock retrieved from storage.
func (e *Epoch) loadLastBlock() error {
	block, err := RetrieveLastBlockFromStorage(e.Storage)
	if err != nil {
		return err
	}

	e.lastBlock = block
	return nil
}

func (e *Epoch) loadLastRound() {
	// Put the last block we committed in the rounds map.
	if e.lastBlock != nil {
		round := NewRound(e.lastBlock)
		e.rounds[round.num] = round
	}
}

func (e *Epoch) Stop() {
	e.finishFn()
}

func (e *Epoch) handleFinalizationMessage(msg *Quorum, from NodeID) error {
	round, exists := e.rounds[msg.Header.Round]
	if !exists {
		e.Logger.Debug("Received finalization certificate for a non existent round", zap.Int("round", int(msg.Header.Round)))
		return nil
	}

	if round.finalization != nil {
		e.Logger.Debug("Received finalization for an already finalized round", zap.Uint64("round", msg.Header.Round))
		return nil
	}

	valid, err := e.isFinalizationCertificateValid(msg)
	if err != nil {
		return err
	}
	if !valid {
		e.Logger.Debug("Received an invalid finalization certificate",
			zap.Int("round", int(msg.Header.Round)),
			zap.Stringer("NodeID", from))
		return nil
	}
	round.finalization = msg

	return e.persistFinalization(*msg)
}

func (e *Epoch) isFinalizationCertificateValid(finalization *Quorum) (bool, error) {
	valid, err := e.validateFinalizationQC(finalization)
	if err != nil {
		return false, err
	}
	if !valid {
		return false, nil
	}

	e.Logger.Debug("Received finalization without any signatures in it")
	return false, nil
}

func (e *Epoch) validateFinalizationQC(finalization *Quorum) (bool, error) {
	qc, err := e.QCDeserializer.DeserializeQuorumCertificate(finalization.QC)
	if err != nil {
		e.Logger.Debug("FinalizationCertificate QC failed to be parsed",
			zap.Error(err),
		)
		return false, nil
	}

	// Check enough signers signed the finalization certificate
	if e.quorumSize > len(qc.Signers()) {
		e.Logger.Debug("ToBeSignedFinalization certificate signed by insufficient nodes",
			zap.Int("count", len(qc.Signers())),
			zap.Int("Quorum", e.quorumSize))
		return false, nil
	}

	signedTwice := e.hasSomeNodeSignedTwice(qc.Signers())
	if signedTwice {
		return false, nil
	}

	if err := finalization.Verify(e.QCDeserializer, FinalizeContext); err != nil {
		return false, nil
	}

	return true, nil
}

func (e *Epoch) handleFinalizeMessage(msg *Vote, from NodeID) error {
	finalization := msg.Header

	// Only process a point to point finalization
	if !from.Equals(msg.Signature.Signer) {
		e.Logger.Debug("Received a finalization signed by a different party than sent it", zap.Stringer("signer", msg.Signature.Signer), zap.Stringer("sender", from))
		return nil
	}

	// Have we already finalized this round?
	round, exists := e.rounds[finalization.Round]
	if !exists {
		e.Logger.Debug("Received finalization for an unknown round", zap.Uint64("round", finalization.Round))
		return nil
	}

	if round.finalization != nil {
		e.Logger.Debug("Received finalization for an already finalized round", zap.Uint64("round", finalization.Round))
		return nil
	}

	if err := msg.Verify(e.Verifier, FinalizeContext); err != nil {
		e.Logger.Debug("Received a finalization with an invalid signature",
			zap.Uint64("round", finalization.Round),
			zap.Error(err),
		)
		return nil
	}

	round.finalizes[string(from)] = msg

	return e.maybeCollectFinalizationCertificate(round)
}

func (e *Epoch) handleVoteMessage(msg *Vote, _ NodeID) error {
	vote := msg.Header

	// TODO: what if we've received a vote for a round we didn't instantiate yet?
	round, exists := e.rounds[vote.Round]
	if !exists {
		e.Logger.Debug("Received a vote for a non existent round",
			zap.Uint64("round", vote.Round),
		)
		return nil
	}

	if round.notarization != nil {
		e.Logger.Debug("Round already notarized",
			zap.Uint64("round", vote.Round),
		)
		return nil
	}

	if !e.isVoteValid(vote) {
		return nil
	}

	// Only verify the vote if we haven't verified it in the past.
	signature := msg.Signature
	if _, exists := round.votes[string(signature.Signer)]; !exists {
		if err := msg.Verify(e.Verifier, VoteContext); err != nil {
			e.Logger.Debug("ToBeSignedVote verification failed",
				zap.Stringer("NodeID", signature.Signer),
				zap.Error(err),
			)
			return nil
		}
	}

	e.rounds[vote.Round].votes[string(signature.Signer)] = msg

	return e.maybeCollectNotarization()
}

func (e *Epoch) isVoteValid(vote BlockHeader) bool {
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
	finalizationCount := len(round.finalizes)

	if finalizationCount < e.quorumSize {
		e.Logger.Verbo("Counting finalizations", zap.Uint64("round", e.round), zap.Int("votes", finalizationCount))
		return nil
	}

	return e.assembleFinalizationCertificate(round)
}

func (e *Epoch) assembleFinalizationCertificate(round *Round) error {
	// Divide finalizations into sets that agree on the same metadata
	finalizesByMD := make(map[string][]*Vote)

	for _, vote := range round.finalizes {
		key := string(vote.Header.MarshalCanoto())
		finalizesByMD[key] = append(finalizesByMD[key], vote)
	}

	var finalizes []*Vote

	for _, finalizationsWithTheSameDigest := range finalizesByMD {
		if len(finalizationsWithTheSameDigest) >= e.quorumSize {
			finalizes = finalizationsWithTheSameDigest
			break
		}
	}

	if len(finalizes) == 0 {
		e.Logger.Debug("Could not find enough finalizations for the same metadata")
		return nil
	}

	finalize := finalizes[0]

	voteCount := len(finalizes)

	signatures := make([]Signature, 0, voteCount)
	e.Logger.Info("Collected Quorum of votes", zap.Uint64("round", e.round), zap.Int("votes", voteCount))
	for _, vote := range finalizes {
		// TODO: ensure all finalizations agree on the same metadata!
		e.Logger.Debug("Collected finalization from node", zap.Stringer("NodeID", vote.Signature.Signer))
		signatures = append(signatures, vote.Signature)
	}

	qc, err := e.SignatureAggregator.Aggregate(signatures)
	if err != nil {
		return fmt.Errorf("could not aggregate signatures for finalization certificate: %w", err)
	}

	finalization := Quorum{
		Header: finalize.Header,
		QC:     qc.Bytes(),
	}
	round.finalization = &finalization

	return e.persistFinalization(finalization)
}

func (e *Epoch) persistFinalization(finalization Quorum) error {
	// Check to see if we should commit this finalization to the storage as part of a block commit,
	// or otherwise write it to the WAL in order to commit it later.
	nextSeqToCommit := e.Storage.Height()
	if finalization.Header.Seq == nextSeqToCommit {
		block := e.rounds[finalization.Header.Round].block
		e.Storage.Index(block, finalization)
		e.Logger.Info("Committed block",
			zap.Uint64("round", finalization.Header.Round),
			zap.Uint64("sequence", finalization.Header.Seq),
			zap.Stringer("digest", finalization.Header.Digest))
		e.lastBlock = block

		// If the round we're committing is too far in the past, don't keep it in the rounds cache.
		if finalization.Header.Round+e.maxRoundWindow < e.round {
			delete(e.rounds, finalization.Header.Round)
		}
		// Clean up the future messages - Remove all messages we may have stored for the round
		// the finalization is about.
		for _, messagesFromNode := range e.futureMessages {
			delete(messagesFromNode, finalization.Header.Round)
		}
	} else {
		record := Record{
			Finalization: &finalization,
		}
		recordBytes := record.MarshalCanoto()
		if err := e.WAL.Append(recordBytes); err != nil {
			e.Logger.Error("Failed to append finalization certificate record to WAL", zap.Error(err))
			return err
		}

		e.Logger.Debug("Persisted finalization certificate to WAL",
			zap.Int("size", len(recordBytes)),
			zap.Uint64("round", finalization.Header.Round),
			zap.Stringer("digest", finalization.Header.Digest))
	}

	finalizationMessage := &Message{Finalization: &finalization}
	e.Comm.Broadcast(finalizationMessage)

	e.Logger.Debug("Broadcast finalization certificate",
		zap.Uint64("round", finalization.Header.Round),
		zap.Stringer("digest", finalization.Header.Digest))

	return nil
}

func (e *Epoch) maybeCollectNotarization() error {
	votesForCurrentRound := e.rounds[e.round].votes
	voteCount := len(votesForCurrentRound)

	if voteCount < e.quorumSize {
		e.Logger.Verbo("Counting votes", zap.Uint64("round", e.round), zap.Int("votes", voteCount))
		return nil
	}

	// TODO: store votes before receiving the block

	block := e.rounds[e.round].block
	digestWeExpect := block.BlockHeader().Digest

	// Ensure we have enough votes for the same digest
	var voteCountForOurDigest int
	for _, vote := range votesForCurrentRound {
		if bytes.Equal(digestWeExpect[:], vote.Header.Digest[:]) {
			voteCountForOurDigest++
		}
	}

	if voteCountForOurDigest < e.quorumSize {
		e.Logger.Verbo("Counting votes for the digest we received from the leader",
			zap.Uint64("round", e.round), zap.Int("votes", voteCount))
		return nil
	}

	return e.assembleNotarization(votesForCurrentRound, digestWeExpect)
}

func (e *Epoch) assembleNotarization(votesForCurrentRound map[string]*Vote, digest Digest) error {
	vote := BlockHeader{
		ProtocolMetadata: ProtocolMetadata{
			Epoch: e.Epoch,
			Round: e.round,
		},
		Digest: digest,
	}

	voteCount := len(votesForCurrentRound)

	signatures := make([]Signature, 0, voteCount)
	e.Logger.Info("Collected Quorum of votes",
		zap.Uint64("round", e.round),
		zap.Int("votes", voteCount),
	)
	for _, vote := range votesForCurrentRound {
		e.Logger.Debug("Collected vote from node", zap.Stringer("NodeID", vote.Signature.Signer))
		signatures = append(signatures, vote.Signature)
	}

	qc, err := e.SignatureAggregator.Aggregate(signatures)
	if err != nil {
		return fmt.Errorf("could not aggregate signatures for notarization: %w", err)
	}

	notarization := Quorum{
		Header: vote,
		QC:     qc.Bytes(),
	}
	if err := e.storeNotarization(notarization); err != nil {
		return err
	}

	return e.persistNotarization(notarization)
}

func (e *Epoch) persistNotarization(notarization Quorum) error {
	record := Record{
		Notarization: &notarization,
	}
	recordBytes := record.MarshalCanoto()
	if err := e.WAL.Append(recordBytes); err != nil {
		e.Logger.Error("Failed to append notarization record to WAL", zap.Error(err))
		return err
	}

	e.Logger.Debug("Persisted notarization to WAL",
		zap.Int("size", len(recordBytes)),
		zap.Uint64("round", notarization.Header.Round),
		zap.Stringer("digest", notarization.Header.Digest))

	notarizationMessage := &Message{Notarization: &notarization}
	e.Comm.Broadcast(notarizationMessage)

	e.Logger.Debug("Broadcast notarization",
		zap.Uint64("round", notarization.Header.Round),
		zap.Stringer("digest", notarization.Header.Digest))

	e.rounds[notarization.Header.Round].notarization = &notarization

	return e.doNotarized()
}

func (e *Epoch) handleNotarizationMessage(msg *Quorum, from NodeID) error {
	vote := msg.Header

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

	if err := msg.Verify(e.QCDeserializer, VoteContext); err != nil {
		e.Logger.Debug("Notarization quorum certificate is invalid",
			zap.Stringer("NodeID", from), zap.Error(err))
		return nil
	}

	return e.persistNotarization(*msg)
}

func (e *Epoch) hasSomeNodeSignedTwice(nodeIDs []NodeID) bool {
	seen := make(map[string]struct{}, len(nodeIDs))

	for _, nodeID := range nodeIDs {
		if _, alreadySeen := seen[string(nodeID)]; alreadySeen {
			e.Logger.Warn("Observed a signature originating at least twice from the same node")
			return true
		}
		seen[string(nodeID)] = struct{}{}
	}

	return false
}

func (e *Epoch) handleBlockMessage(msg *Proposal, _ NodeID) error {
	block, err := e.BlockDeserializer.DeserializeBlock(msg.Block)
	if err != nil {
		e.Logger.Debug("Got invalid block in a BlockMessage",
			zap.Error(err),
		)
		return nil
	}

	vote := msg.Vote
	from := vote.Signature.Signer

	md := block.BlockHeader()

	// Ignore block messages sent by us
	if e.ID.Equals(from) {
		e.Logger.Debug("Got a BlockMessage from ourselves or created by us")
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
		if !bytes.Equal(vote.Header.Digest[:], md.Digest[:]) {
			e.Logger.Debug("ToBeSignedVote digest mismatches block digest", zap.Stringer("voteDigest", vote.Header.Digest),
				zap.Stringer("blockDigest", md.Digest))
			return nil
		}
		// 2) Verify the vote is properly signed
		if err := vote.Verify(e.Verifier, VoteContext); err != nil {
			e.Logger.Debug("ToBeSignedVote verification failed", zap.Stringer("NodeID", vote.Signature.Signer), zap.Error(err))
			return nil
		}
	}

	// If this is a message from a more advanced round,
	// only store it if it is up to `maxRoundWindow` ahead.
	// TODO: test this
	if e.round < md.Round && md.Round-e.round < e.maxRoundWindow {
		e.Logger.Debug("Got block from round too far in the future", zap.Uint64("round", md.Round), zap.Uint64("my round", e.round))
		msgsForRound, exists := e.futureMessages[string(from)][md.Round]
		if !exists {
			msgsForRound = &messagesForRound{}
			e.futureMessages[string(from)][md.Round] = msgsForRound
		}
		msgsForRound.proposal = msg
		return nil
	}

	if !e.verifyProposalIsPartOfOurChain(block) {
		e.Logger.Debug("Got invalid block in a BlockMessage")
		return nil
	}

	if !e.storeProposal(block) {
		e.Logger.Warn("Unable to store proposed block for the round", zap.Stringer("NodeID", from), zap.Uint64("round", md.Round))
		// TODO: timeout
	}

	// Once we have stored the proposal, we have a Round object for the round.
	// We store the vote to prevent verifying its signature again.
	round, exists := e.rounds[md.Round]
	if !exists {
		// This shouldn't happen, but in case it does, return an error
		return fmt.Errorf("programming error: round %d not found", md.Round)
	}
	round.votes[string(vote.Signature.Signer)] = &vote

	if err := block.Verify(); err != nil {
		e.Logger.Debug("Failed verifying block", zap.Error(err))
		return nil
	}

	record := Record{
		Block: block.Bytes(),
	}
	recordBytes := record.MarshalCanoto()
	if err := e.WAL.Append(recordBytes); err != nil {
		e.Logger.Error("Failed appending block to WAL", zap.Error(err))
		return err
	}

	return e.doProposed(block, vote)
}

func (e *Epoch) wasBlockAlreadyVerified(from NodeID, md BlockHeader) bool {
	var alreadyVerified bool
	msgsForRound, exists := e.futureMessages[string(from)][md.Round]
	if exists && msgsForRound.proposal != nil {
		block, err := e.BlockDeserializer.DeserializeBlock(msgsForRound.proposal.Block)
		if err != nil {
			panic("failed to deserialize block")
		}
		bh := block.BlockHeader()
		alreadyVerified = bh.Equals(&md)
	}
	return alreadyVerified
}

func (e *Epoch) verifyProposalIsPartOfOurChain(block Block) bool {
	bh := block.BlockHeader()

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
			Round: e.round,
			Seq:   expectedSeq,
			Epoch: e.Epoch,
			Prev:  expectedPrevDigest,
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

func (e *Epoch) proposeBlock() error {
	block, ok := e.BlockBuilder.BuildBlock(e.finishCtx, e.Metadata())
	if !ok {
		return errors.New("failed to build block")
	}

	blockHeader := block.BlockHeader()
	blockBytes := block.Bytes()

	// Write record to WAL before broadcasting it, so that
	// if we crash during broadcasting, we know what we sent.

	record := Record{
		Block: blockBytes,
	}
	recordBytes := record.MarshalCanoto()
	if err := e.WAL.Append(recordBytes); err != nil {
		e.Logger.Error("Failed appending block to WAL", zap.Error(err))
		return err
	}
	e.Logger.Debug("Wrote block to WAL",
		zap.Uint64("round", blockHeader.Round),
		zap.Int("size", len(record.Block)),
		zap.Stringer("digest", blockHeader.Digest))

	vote, err := e.voteOnBlock(block)
	if err != nil {
		return err
	}

	proposal := &Message{
		Proposal: &Proposal{
			Block: blockBytes,
			Vote:  vote,
		},
	}

	if !e.storeProposal(block) {
		return errors.New("failed to store block proposed by me")
	}

	e.Comm.Broadcast(proposal)
	e.Logger.Debug("Proposal broadcast",
		zap.Uint64("round", blockHeader.Round),
		zap.Int("size", len(record.Block)),
		zap.Stringer("digest", blockHeader.Digest))

	return e.handleVoteMessage(&vote, e.ID)
}

func (e *Epoch) Metadata() ProtocolMetadata {
	var prev Digest
	seq := e.Storage.Height()
	if e.lastBlock != nil {
		// Build on top of the latest block
		currMed := e.getHighestRound().block.BlockHeader()
		prev = currMed.Digest
		seq = currMed.Seq + 1
	}

	md := ProtocolMetadata{
		Round: e.round,
		Seq:   seq,
		Epoch: e.Epoch,
		Prev:  prev,
	}
	return md
}

func (e *Epoch) startRound() error {
	leaderForCurrentRound := LeaderForRound(e.nodes, e.round)

	if e.ID.Equals(leaderForCurrentRound) {
		return e.proposeBlock()
	}

	// If we're not the leader, check if we have received a proposal earlier for this round
	msgsForRound, exists := e.futureMessages[string(leaderForCurrentRound)][e.round]
	if !exists || msgsForRound.proposal == nil {
		return nil
	}

	return e.handleBlockMessage(msgsForRound.proposal, leaderForCurrentRound)
}

func (e *Epoch) doProposed(block Block, voteFromLeader Vote) error {
	vote, err := e.voteOnBlock(block)
	if err != nil {
		return err
	}

	md := block.BlockHeader()

	// We do not write the vote to the WAL as we have written the block itself to the WAL
	// and we can always restore the block and sign it again if needed.
	voteMsg := &Message{
		Vote: &vote,
	}

	e.Logger.Debug("Broadcasting vote",
		zap.Uint64("round", md.Round),
		zap.Stringer("digest", md.Digest))

	e.Comm.Broadcast(voteMsg)
	// Send yourself a vote message
	if err := e.handleVoteMessage(&vote, e.ID); err != nil {
		return err
	}

	return e.handleVoteMessage(&voteFromLeader, e.ID)
}

func (e *Epoch) voteOnBlock(block Block) (Vote, error) {
	vote := Vote{
		Header: block.BlockHeader(),
	}
	if err := vote.Sign(e.ID, e.Signer, VoteContext); err != nil {
		return Vote{}, fmt.Errorf("failed signing vote %w", err)
	}
	return vote, nil
}

func (e *Epoch) increaseRound() {
	e.Logger.Info(fmt.Sprintf("Moving to a new round (%d --> %d", e.round, e.round+1), zap.Uint64("round", e.round+1))
	e.round++
}

func (e *Epoch) doNotarized() error {
	round := e.rounds[e.round]
	block := round.block

	md := block.BlockHeader()

	f := Vote{
		Header: md,
	}
	if err := f.Sign(e.ID, e.Signer, FinalizeContext); err != nil {
		return fmt.Errorf("failed signing finalize %w", err)
	}

	finalizationMsg := &Message{
		Finalize: &f,
	}
	e.Comm.Broadcast(finalizationMsg)

	e.increaseRound()

	err1 := e.startRound()
	err2 := e.handleFinalizeMessage(&f, e.ID)

	return errors.Join(err1, err2)
}

func (e *Epoch) storeNotarization(notarization Quorum) error {
	round := notarization.Header.Round
	r, exists := e.rounds[round]
	if !exists {
		fmt.Errorf("attempted to store notarization of a non existent round %d", round)
	}

	r.notarization = &notarization
	return nil
}

func (e *Epoch) maybeLoadFutureMessages(round uint64) {
	for from, messagesFromNode := range e.futureMessages {
		if msgs, exists := messagesFromNode[round]; exists {
			if msgs.proposal != nil {
				e.handleBlockMessage(msgs.proposal, NodeID(from))
				msgs.proposal = nil
			}
			if msgs.vote != nil {
				e.handleVoteMessage(msgs.vote, NodeID(from))
				msgs.vote = nil
			}
			if msgs.finalize != nil {
				e.handleFinalizeMessage(msgs.finalize, NodeID(from))
				msgs.finalize = nil
			}

			if msgs.proposal == nil && msgs.vote == nil && msgs.finalize == nil {
				delete(messagesFromNode, round)
			}
		}
	}
}

func (e *Epoch) storeProposal(block Block) bool {
	md := block.BlockHeader()

	// Don't bother processing blocks from the past
	if e.round > md.Round {
		return false
	}

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
			max = round.num
		}
	}
	return e.rounds[max]
}

func LeaderForRound(nodes []NodeID, r uint64) NodeID {
	n := len(nodes)
	return nodes[r%uint64(n)]
}

func QuorumSize(n int) int {
	f := (n - 1) / 3
	// Obtained from the equation:
	// Quorum * 2 = N + F + 1
	return (n+f)/2 + 1
}

// messagesFromNode maps nodeIds to the messages it sent in a given round.
type messagesFromNode map[string]map[uint64]*messagesForRound

type messagesForRound struct {
	proposal *Proposal
	vote     *Vote
	finalize *Vote
}

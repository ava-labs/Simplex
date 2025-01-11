// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"

	"go.uber.org/zap"
)

type Logger interface {
	// Log that a fatal error has occurred. The program should likely exit soon
	// after this is called
	Fatal(msg string, fields ...zap.Field)
	// Log that an error has occurred. The program should be able to recover
	// from this error
	Error(msg string, fields ...zap.Field)
	// Log that an event has occurred that may indicate a future error or
	// vulnerability
	Warn(msg string, fields ...zap.Field)
	// Log an event that may be useful for a user to see to measure the progress
	// of the protocol
	Info(msg string, fields ...zap.Field)
	// Log an event that may be useful for understanding the order of the
	// execution of the protocol
	Trace(msg string, fields ...zap.Field)
	// Log an event that may be useful for a programmer to see when debuging the
	// execution of the protocol
	Debug(msg string, fields ...zap.Field)
	// Log extremely detailed events that can be useful for inspecting every
	// aspect of the program
	Verbo(msg string, fields ...zap.Field)
}

type BlockBuilder interface {
	// BuildBlock blocks until some transactions are available to be batched into a block,
	// in which case a block and true are returned.
	// When the given context is cancelled by the caller, returns false.
	BuildBlock(ctx context.Context, metadata ProtocolMetadata) (Block, bool)

	// IncomingBlock returns when either the given context is cancelled,
	// or when the application signals that a block should be built.
	IncomingBlock(ctx context.Context)
}

type Storage interface {
	Height() uint64
	Retrieve(seq uint64) (Block, FinalizationCertificate, bool)
	Index(block Block, certificate FinalizationCertificate)
}

type Communication interface {

	// ListNodes returns all nodes known to the application.
	ListNodes() []NodeID

	// SendMessage sends a message to the given destination node
	SendMessage(msg *Message, destination NodeID)

	// Broadcast broadcasts the given message to all nodes.
	// Does not send it to yourself.
	Broadcast(msg *Message)
}

type Signer interface {
	Sign(message []byte) ([]byte, error)
}

type SignatureVerifier interface {
	Verify(message []byte, signature []byte, signer NodeID) error
}

type WriteAheadLog interface {
	Append([]byte) error
	ReadAll() ([][]byte, error)
}

type Block interface {
	// BlockHeader encodes a succinct and collision-free representation of a block.
	BlockHeader() BlockHeader

	// Bytes returns a byte encoding of the block
	Bytes() []byte

	// Verify verifies the block by speculatively executing it on top of its ancestor.
	Verify() error
}

// BlockDeserializer deserializes blocks according to formatting
// enforced by the application.
type BlockDeserializer interface {
	// DeserializeBlock parses the given bytes and initializes a Block.
	// Returns an error upon failure.
	DeserializeBlock(bytes []byte) (Block, error)
}

// QuorumCertificate is equivalent to a collection of signatures from a quorum of nodes,
type QuorumCertificate interface {
	// Signers returns who participated in creating this QuorumCertificate.
	Signers() []NodeID
	// Verify checks whether the nodes participated in creating this QuorumCertificate,
	// signed the given message.
	Verify(msg []byte) error
	// Bytes returns a raw representation of the given QuorumCertificate.
	Bytes() []byte
}

// QCDeserializer deserializes QuorumCertificates according to formatting
type QCDeserializer interface {
	// DeserializeQuorumCertificate parses the given bytes and initializes a QuorumCertificate.
	// Returns an error upon failure.
	DeserializeQuorumCertificate(bytes []byte) (QuorumCertificate, error)
}

// SignatureAggregator aggregates signatures into a QuorumCertificate
type SignatureAggregator interface {
	// Aggregate aggregates several signatures into a QuorumCertificate
	Aggregate([]Signature) (QuorumCertificate, error)
}

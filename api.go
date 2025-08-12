// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"fmt"

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
	BuildBlock(ctx context.Context, metadata ProtocolMetadata) (VerifiedBlock, bool)

	// IncomingBlock returns when either the given context is cancelled,
	// or when the application signals that a block should be built.
	IncomingBlock(ctx context.Context)
}

var ErrBlockNotFound = fmt.Errorf("block not found")

type Storage interface {
	Height() uint64
	// Retrieve returns the block and finalization at [seq].
	// If [seq] is not found, returns an error.
	Retrieve(seq uint64) (VerifiedBlock, Finalization, error)
	Index(ctx context.Context, block VerifiedBlock, certificate Finalization) error
}

type Communication interface {

	// Nodes returns all nodes that participate in the epoch.
	Nodes() []NodeID

	// Send sends a message to the given destination node
	Send(msg *Message, destination NodeID)

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

	// Verify verifies the block by speculatively executing it on top of its ancestor.
	Verify(ctx context.Context) (VerifiedBlock, error)
}

type VerifiedBlock interface {
	// BlockHeader encodes a succinct and collision-free representation of a block.
	BlockHeader() BlockHeader

	// Bytes returns a byte encoding of the block
	Bytes() ([]byte, error)
}

// BlockDeserializer deserializes blocks according to formatting
// enforced by the application.
type BlockDeserializer interface {
	// DeserializeBlock parses the given bytes and initializes a VerifiedBlock.
	// Returns an error upon failure.
	DeserializeBlock(ctx context.Context, bytes []byte) (Block, error)
}

// Signature encodes a signature and the node that signed it, without the message it was signed on.
type Signature struct {
	// Signer is the NodeID of the creator of the signature.
	Signer NodeID
	// Value is the byte representation of the signature.
	Value []byte
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

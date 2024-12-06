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
	BuildBlock(ctx context.Context) (Block, bool)

	// IncomingBlock returns when either the given context is cancelled,
	// or when the application signals that a block should be built.
	IncomingBlock(ctx context.Context)
}

type Communication interface {

	// ListNodes returns all nodes known to the application.
	ListNodes() []NodeID

	// SendMessage sends a message to the given destination node
	SendMessage(msg *Message, destination NodeID)

	// Broadcast broadcasts the given message to all nodes
	Broadcast(msg *Message)
}

type WriteAheadLog interface {
	Append(*Record) error
	ReadAll() ([]Record, error)
}

type Block interface {
	// Metadata is the consensus specific metadata for the block
	Metadata() Metadata

	// Bytes returns a byte encoding of the block
	Bytes() []byte
}

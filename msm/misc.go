// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"time"

	"go.uber.org/zap"
)

// This file contains implementations of utility methods and structures that exists in Avalanchego,
// but are not imported here to prevent us from importing the entire Avalanchego codebase.
// Once we incorporate Simplex into Avalanchego, we can remove this file and import the relevant code from Avalanchego instead.

func safeAdd(a, b uint64) (uint64, error) {
	if a > math.MaxUint64-b {
		return 0, fmt.Errorf("overflow: %d + %d > maxuint64", a, b)
	}
	return a + b, nil
}

type nodeID [20]byte

type VMBlock interface {
	// Digest returns a succinct representation of this block.
	Digest() [32]byte

	// Height returns the height of this block in the chain.
	Height() uint64

	// Time this block was proposed at. This value should be consistent across
	// all nodes. If this block hasn't been successfully verified, any value can
	// be returned. If this block is the last accepted block, the timestamp must
	// be returned correctly. Otherwise, accepted blocks can return any value.
	Timestamp() time.Time

	// Verify that the state transition this block would make if accepted is
	// valid. If the state transition is invalid, a non-nil error should be
	// returned.
	//
	// It is guaranteed that the Parent has been successfully verified.
	//
	// If nil is returned, it is guaranteed that either Accept or Reject will be
	// called on this block, unless the VM is shut down.
	Verify(context.Context) error
}

type upgradeConfig = any

type bitmask big.Int

func (bm *bitmask) Bytes() []byte {
	return (*big.Int)(bm).Bytes()
}

func (bm *bitmask) Contains(i int) bool {
	return (*big.Int)(bm).Bit(i) == 1
}

func (bm *bitmask) Add(i int) {
	bits := (*big.Int)(bm)
	bits.SetBit(bits, i, 1)
}

func (bm *bitmask) Difference(bm2 *bitmask) {
	bits := (*big.Int)(bm)
	bits2 := (*big.Int)(bm2)
	bits.AndNot(bits, bits2)
}

func (bm *bitmask) Len() int {
	bits := (*big.Int)(bm)
	result := 0
	var zero big.Int
	for bits.Cmp(&zero) > 0 {
		lsb := bits.Bit(0)
		if lsb == 1 {
			result++
		}
		bits.Rsh(bits, 1)
	}
	return result
}

func bitmaskFromBytes(bytes []byte) bitmask {
	var bm bitmask
	(*big.Int)(&bm).SetBytes(bytes)
	return bm
}

// Logger defines the interface that is used to keep a record of all events that
// happen to the program
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
	// Log an event that may be useful for a programmer to see when debugging the
	// execution of the protocol
	Debug(msg string, fields ...zap.Field)
	// Log extremely detailed events that can be useful for inspecting every
	// aspect of the program
	Verbo(msg string, fields ...zap.Field)
}

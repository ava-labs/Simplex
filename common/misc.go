// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package common

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"
)

// This file contains implementations of utility methods and structures that exists in Avalanchego,
// but are not imported here to prevent us from importing the entire Avalanchego codebase.
// Once we incorporate Simplex into Avalanchego, we can remove this file and import the relevant code from Avalanchego instead.

var ErrOverflow = errors.New("overflow")

func SafeAdd(a, b uint64) (uint64, error) {
	if a > math.MaxUint64-b {
		return 0, fmt.Errorf("%w: %d + %d > maxuint64", ErrOverflow, a, b)
	}
	return a + b, nil
}

// NodeIdentifier is a 20 byte identifier for a node.
// It is used instead of NodeID in Avalanchego to avoid importing the entire Avalanchego codebase.
// TODO: Once we incorporate Simplex into Avalanchego, we can combine this with NodeID in common/global.go.
type NodeIdentifier [20]byte

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
	Verify(ctx context.Context, pChainHeight uint64) error
}

type Bitmask big.Int

func (bm *Bitmask) Bytes() []byte {
	return (*big.Int)(bm).Bytes()
}

func (bm *Bitmask) Clone() Bitmask {
	var newBM Bitmask
	(*big.Int)(&newBM).Set((*big.Int)(bm))
	return newBM
}

func (bm *Bitmask) Contains(i int) bool {
	return (*big.Int)(bm).Bit(i) == 1
}

func (bm *Bitmask) Add(i int) {
	bits := (*big.Int)(bm)
	bits.SetBit(bits, i, 1)
}

func (bm *Bitmask) Difference(bm2 *Bitmask) {
	bits := (*big.Int)(bm)
	bits2 := (*big.Int)(bm2)
	bits.AndNot(bits, bits2)
}

func (bm *Bitmask) Len() int {
	bmAsBigInt := (*big.Int)(bm)
	bits := new(big.Int).Set(bmAsBigInt)

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

func BitmaskFromBytes(bytes []byte) Bitmask {
	var bm Bitmask
	(*big.Int)(&bm).SetBytes(bytes)
	return bm
}

// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package testutil

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/stretchr/testify/require"
)

type InMemStorage struct {
	data map[uint64]struct {
		simplex.VerifiedBlock
		simplex.Finalization
	}

	lock   sync.Mutex
	signal sync.Cond
}

func NewInMemStorage() *InMemStorage {
	s := &InMemStorage{
		data: make(map[uint64]struct {
			simplex.VerifiedBlock
			simplex.Finalization
		}),
	}

	s.signal = *sync.NewCond(&s.lock)

	return s
}

func (mem *InMemStorage) NumBlocks() uint64 {
	mem.lock.Lock()
	defer mem.lock.Unlock()

	return uint64(len(mem.data))
}

func (mem *InMemStorage) Clone() *InMemStorage {
	clone := NewInMemStorage()
	height := mem.NumBlocks()
	for seq := uint64(0); seq < height; seq++ {
		block, finalization, err := mem.Retrieve(seq)
		if err != nil {
			panic(fmt.Sprintf("failed retrieving block %d: %v", seq, err))
		}
		clone.Index(context.Background(), block, finalization)
	}
	return clone
}

func (mem *InMemStorage) WaitForBlockCommit(seq uint64) simplex.VerifiedBlock {
	mem.lock.Lock()
	defer mem.lock.Unlock()

	for {
		if data, exists := mem.data[seq]; exists {
			return data.VerifiedBlock
		}

		mem.signal.Wait()
	}
}

func (mem *InMemStorage) EnsureNoBlockCommit(t *testing.T, seq uint64) {
	require.Never(t, func() bool {
		mem.lock.Lock()
		defer mem.lock.Unlock()

		_, exists := mem.data[seq]
		return exists
	}, time.Second, time.Millisecond*100, "block %d has been committed but shouldn't have been", seq)
}

func (mem *InMemStorage) Retrieve(seq uint64) (simplex.VerifiedBlock, simplex.Finalization, error) {
	mem.lock.Lock()
	defer mem.lock.Unlock()

	item, ok := mem.data[seq]
	if !ok {
		return nil, simplex.Finalization{}, fmt.Errorf("%w: seq %d", simplex.ErrBlockNotFound, seq)
	}
	return item.VerifiedBlock, item.Finalization, nil
}

func (mem *InMemStorage) Index(ctx context.Context, block simplex.VerifiedBlock, certificate simplex.Finalization) error {
	mem.lock.Lock()
	defer mem.lock.Unlock()

	seq := block.BlockHeader().Seq

	_, ok := mem.data[seq]
	if ok {
		panic(fmt.Sprintf("block with seq %d already indexed!", seq))
	}
	mem.data[seq] = struct {
		simplex.VerifiedBlock
		simplex.Finalization
	}{block,
		certificate,
	}

	mem.signal.Signal()
	return nil
}

func (mem *InMemStorage) Compare(other *InMemStorage) error {
	mem.lock.Lock()
	defer mem.lock.Unlock()

	other.lock.Lock()
	defer other.lock.Unlock()

	if len(mem.data) != len(other.data) {
		return fmt.Errorf("storage lengths differ: %d vs %d", len(mem.data), len(other.data))
	}

	for seq, item := range mem.data {
		otherItem, ok := other.data[seq]
		if !ok {
			return fmt.Errorf("other storage missing seq %d", seq)
		}

		// compare blocks
		blockBytes, err := item.VerifiedBlock.Bytes()
		if err != nil {
			return fmt.Errorf("failed getting bytes for seq %d: %v", seq, err)
		}

		otherBlockBytes, err := otherItem.VerifiedBlock.Bytes()
		if err != nil {
			return fmt.Errorf("failed getting bytes for seq %d: %v", seq, err)
		}

		if !bytes.Equal(blockBytes, otherBlockBytes) {
			return fmt.Errorf("blocks differ at seq %d", seq)
		}

		// compare finalizations
		if item.Finalization.Finalization.Digest != otherItem.Finalization.Finalization.Digest {
			return fmt.Errorf("finalizations differ at seq %d", seq)
		}
	}

	return nil
}

package testutil

import (
	"fmt"
	"simplex"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type InMemStorage struct {
	data map[uint64]struct {
		simplex.VerifiedBlock
		simplex.FinalizationCertificate
	}

	lock   sync.Mutex
	signal sync.Cond
}

func NewInMemStorage() *InMemStorage {
	s := &InMemStorage{
		data: make(map[uint64]struct {
			simplex.VerifiedBlock
			simplex.FinalizationCertificate
		}),
	}

	s.signal = *sync.NewCond(&s.lock)

	return s
}

func (mem *InMemStorage) Clone() *InMemStorage {
	clone := NewInMemStorage()
	mem.lock.Lock()
	height := mem.Height()
	mem.lock.Unlock()
	for seq := uint64(0); seq < height; seq++ {
		mem.lock.Lock()
		block, fCert, ok := mem.Retrieve(seq)
		if !ok {
			panic(fmt.Sprintf("failed retrieving block %d", seq))
		}
		mem.lock.Unlock()
		clone.Index(block, fCert)
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

func (mem *InMemStorage) ensureNoBlockCommit(t *testing.T, seq uint64) {
	require.Never(t, func() bool {
		mem.lock.Lock()
		defer mem.lock.Unlock()

		_, exists := mem.data[seq]
		return exists
	}, time.Second, time.Millisecond*100, "block %d has been committed but shouldn't have been", seq)
}

func (mem *InMemStorage) Height() uint64 {
	return uint64(len(mem.data))
}

func (mem *InMemStorage) Retrieve(seq uint64) (simplex.VerifiedBlock, simplex.FinalizationCertificate, bool) {
	item, ok := mem.data[seq]
	if !ok {
		return nil, simplex.FinalizationCertificate{}, false
	}
	return item.VerifiedBlock, item.FinalizationCertificate, true
}

func (mem *InMemStorage) Index(block simplex.VerifiedBlock, certificate simplex.FinalizationCertificate) {
	mem.lock.Lock()
	defer mem.lock.Unlock()

	seq := block.BlockHeader().Seq

	_, ok := mem.data[seq]
	if ok {
		panic(fmt.Sprintf("block with seq %d already indexed!", seq))
	}
	mem.data[seq] = struct {
		simplex.VerifiedBlock
		simplex.FinalizationCertificate
	}{block,
		certificate,
	}

	mem.signal.Signal()
}

func (mem *InMemStorage) SetIndex(seq uint64, block simplex.VerifiedBlock, certificate simplex.FinalizationCertificate) {
	mem.lock.Lock()
	defer mem.lock.Unlock()

	mem.data[seq] = struct {
		simplex.VerifiedBlock
		simplex.FinalizationCertificate
	}{block,
		certificate,
	}
}

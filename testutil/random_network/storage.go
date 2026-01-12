package random_network

import (
	"context"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
)

type Storage struct {
	*testutil.InMemStorage
	mempool *Mempool
}

func NewStorage(mempool *Mempool) *Storage {
	return &Storage{
		InMemStorage: testutil.NewInMemStorage(),
		mempool: mempool,
	}
}

func (s *Storage) Index(ctx context.Context, block simplex.VerifiedBlock, certificate simplex.Finalization) error {
	s.mempool.AcceptBlock(block.(*Block))
	return s.InMemStorage.Index(ctx, block, certificate)
}
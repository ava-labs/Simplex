package random_network

import (
	"context"
	"time"

	"github.com/ava-labs/simplex"
)

type RandomNetworkBlockBuilder struct {
	MinTxDelay time.Duration
	MaxTxDelay time.Duration

	txVVerificationFailure int

	minTxsPerBlock int
	maxTxsPerBlock int

	mempool *Mempool

	l simplex.Logger
}

func NewNetworkBlockBuilder(config *FuzzConfig, l simplex.Logger) *RandomNetworkBlockBuilder {
	return &RandomNetworkBlockBuilder{
		MinTxDelay:             config.MinTxDelay,
		MaxTxDelay:             config.MaxTxDelay,
		txVVerificationFailure: config.TxVVerificationFailure,
		minTxsPerBlock:         config.MinTxsPerBlock,
		maxTxsPerBlock:         config.MaxTxsPerBlock,
		mempool:                NewMempool(),
		l:                      l,
	}
}

func (bb *RandomNetworkBlockBuilder) BuildBlock(ctx context.Context, md simplex.ProtocolMetadata, bl simplex.Blacklist) (simplex.VerifiedBlock, bool) {
	txs := bb.mempool.PackBlock(ctx, bb.maxTxsPerBlock)
	block := NewBlock(md, bl, bb.mempool, txs)
	return block, true
}

func (bb *RandomNetworkBlockBuilder) WaitForPendingBlock(ctx context.Context) {
	bb.mempool.WaitForPendingTxs(ctx)
}

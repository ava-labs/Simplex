package random_network

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
)

var _   testutil.ControlledBlockBuilder = (*RandomNetworkBlockBuilder)(nil)

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
		MinTxDelay:            config.MinTxDelay,
		MaxTxDelay:            config.MaxTxDelay,
		txVVerificationFailure: config.TxVVerificationFailure,
		minTxsPerBlock:        config.MinTxsPerBlock,
		maxTxsPerBlock:        config.MaxTxsPerBlock,
		mempool:               NewMempool(),
		l:                     l,
	}
}

func (bb *RandomNetworkBlockBuilder) BuildBlock(ctx context.Context, md simplex.ProtocolMetadata, bl simplex.Blacklist) (simplex.VerifiedBlock, bool) {
	// Implementation of block building logic goes here
	numTxs := rand.IntN(bb.maxTxsPerBlock-bb.minTxsPerBlock+1) + bb.minTxsPerBlock // randomize between min and max inclusive
	txs := make([]*TX, 0, numTxs)

	for range numTxs {
		tx := CreateNewTX()
		if rand.IntN(100) < bb.txVVerificationFailure {
			// bb.l.Info("Building a block that will fail verification due to tx", zap.Stringer("txID", tx), zap.Uint64("Sequence", md.Seq))
			tx.SetShouldFailVerification()
		}

		txs = append(txs, tx)
	}

	block := NewBlock(md, bl, bb.mempool, txs)

	return block, true
}

func (bb *RandomNetworkBlockBuilder) WaitForPendingBlock(ctx context.Context) {
	// No-op for this implementation
}

func (bb *RandomNetworkBlockBuilder) TriggerNewBlock() {

}

func (bb *RandomNetworkBlockBuilder) ShouldBlockBeBuilt() bool {
	return false
}
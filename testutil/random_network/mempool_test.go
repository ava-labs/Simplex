package random_network

import (
	"context"
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

func TestMempoolAddsTx(t *testing.T) {

}

func TestMempoolVerifiesTx(t *testing.T) {
	logger := testutil.MakeLogger(t, 1)
	ctx := context.Background()
	require := require.New(t)
	round0MD := NewProtocolMetadata(0, 0, simplex.Digest{})
	// round1MD := NewProtocolMetadata(1, 1, round0MD.Prev)
	// round2MD := NewProtocolMetadata(2, 2, round1MD.Prev)

	tests := []struct {
		name      string
		expectErr error
		setup     func() (*Mempool, *Block, error)
	}{
		{
			name:      "ValidTx",
			expectErr: nil,
			setup: func() (*Mempool, *Block, error) {
				mempool := NewMempool(logger)
				tx := CreateNewTX()
				mempool.AddPendingTXs(tx)

				block := NewBlock(round0MD, emptyBlacklist, mempool, []*TX{tx})

				return mempool, block, nil
			},
		},
		{
			name:      "Duplicate Tx In Block",
			expectErr: errDuplicateTxInBlock,
			setup: func() (*Mempool, *Block, error) {
				mempool := NewMempool(logger)
				tx := CreateNewTX()
				mempool.AddPendingTXs(tx)

				block := NewBlock(round0MD, emptyBlacklist, mempool, []*TX{tx, tx})
				return mempool, block, nil
			},
		},
		{
			name:      "Tx Not Found",
			expectErr: errTxNotFound,
			setup: func() (*Mempool, *Block, error) {
				mempool := NewMempool(logger)
				tx := CreateNewTX()
				block := NewBlock(round0MD, emptyBlacklist, mempool, []*TX{tx})

				return mempool, block, nil
			},
		},
		{
			name:      "Already Accepted",
			expectErr: errAlreadyAccepted,
			setup: func() (*Mempool, *Block, error) {
				mempool := NewMempool(logger)
				tx := CreateNewTX()
				mempool.AddPendingTXs(tx)

				block := NewBlock(round0MD, emptyBlacklist, mempool, []*TX{tx})
				mempool.AcceptBlock(block)

				mempool.AddPendingTXs(tx)
				return mempool, block, nil
			},
		},
		{
			name:      "Already In Chain",
			expectErr: errAlreadyInChain,
			setup: func() (*Mempool, *Block, error) {
				mempool := NewMempool(logger)
				tx := CreateNewTX()
				mempool.AddPendingTXs(tx)

				parentBlock := NewBlock(round0MD, emptyBlacklist, mempool, []*TX{tx})
				if err := mempool.VerifyBlock(ctx, parentBlock); err != nil {
					return nil, nil, err
				}

				mempool.AddPendingTXs(tx)
				md := NewProtocolMetadata(1, 1, parentBlock.digest)
				block := NewBlock(md, emptyBlacklist, mempool, []*TX{tx})
				return mempool, block, nil
			},
		},
		{
			name:      "Tx Verification Fails",
			expectErr: errTxVerification,
			setup: func() (*Mempool, *Block, error) {
				mempool := NewMempool(logger)
				tx := CreateNewTX()
				tx.SetShouldFailVerification()
				mempool.AddPendingTXs(tx)

				block := NewBlock(round0MD, emptyBlacklist, mempool, []*TX{tx})

				return mempool, block, nil
			},
		},
		{
			name:      "Correctly verifies transaction not in chain",
			expectErr: nil,
			setup: func() (*Mempool, *Block, error) {
				mempool := NewMempool(logger)
				tx1 := CreateNewTX()
				mempool.AddPendingTXs(tx1)

				blockWithSameTxButNotParent := NewBlock(round0MD, emptyBlacklist, mempool, []*TX{tx1})
				err := mempool.VerifyBlock(ctx, blockWithSameTxButNotParent)

				mempool.AddPendingTXs(tx1)
				block := NewBlock(NewProtocolMetadata(1, 1, simplex.Digest{}), emptyBlacklist, mempool, []*TX{tx1})
				return mempool, block, err
			},
		},
		{
			name:      "Double Block Verification",
			expectErr: errDoubleBlockVerification,
			setup: func() (*Mempool, *Block, error) {
				mempool := NewMempool(logger)
				tx := CreateNewTX()
				mempool.AddPendingTXs(tx)

				block := NewBlock(round0MD, emptyBlacklist, mempool, []*TX{tx})
				err := mempool.VerifyBlock(ctx, block)
				if err != nil {
					return nil, nil, err
				}

				mempool.AddPendingTXs(tx)
				return mempool, block, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mempool, block, err := tt.setup()
			require.NoError(err)
			err = mempool.VerifyBlock(ctx, block)
			require.ErrorIs(err, tt.expectErr)
		})
	}
}

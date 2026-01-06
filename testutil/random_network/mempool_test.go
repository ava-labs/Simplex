package random_network

import (
	"context"
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/stretchr/testify/require"
)

func TestMempoolAddsTx(t *testing.T) {

}

func TestMempoolVerifiesTx(t *testing.T) {
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
				mempool := NewMempool()
				tx := CreateNewTX()
				mempool.AddUnverifiedTX(tx)

				block := NewBlock(round0MD, emptyBlacklist, mempool, []*TX{tx}, nil)

				return mempool, block, nil
			},
		},
		{
			name:      "Duplicate Tx In Block",
			expectErr: errDuplicateTxInBlock,
			setup: func() (*Mempool, *Block, error) {
				mempool := NewMempool()
				tx := CreateNewTX()
				mempool.AddUnverifiedTX(tx)

				block := NewBlock(round0MD, emptyBlacklist, mempool, []*TX{tx, tx}, nil)
				return mempool, block, nil
			},
		},
		{
			name:      "Tx Not Found",
			expectErr: errTxNotFound,
			setup: func() (*Mempool, *Block, error) {
				mempool := NewMempool()
				tx := CreateNewTX()
				block := NewBlock(round0MD, emptyBlacklist, mempool, []*TX{tx}, nil)

				return mempool, block, nil
			},
		},
		{
			name:      "Already Accepted",
			expectErr: errAlreadyAccepted,
			setup: func() (*Mempool, *Block, error) {
				mempool := NewMempool()
				tx := CreateNewTX()
				mempool.AddUnverifiedTX(tx)

				block := NewBlock(round0MD, emptyBlacklist, mempool, []*TX{tx}, nil)
				mempool.AcceptBlock(block)

				mempool.AddUnverifiedTX(tx)
				return mempool, block, nil
			},
		},
		{
			name:      "Already In Chain",
			expectErr: errAlreadyInChain,
			setup: func() (*Mempool, *Block, error) {
				mempool := NewMempool()
				tx := CreateNewTX()
				mempool.AddUnverifiedTX(tx)

				parentBlock := NewBlock(round0MD, emptyBlacklist, mempool, []*TX{tx}, nil)
				if err := mempool.VerifyBlock(ctx, parentBlock); err != nil {
					return nil, nil, err
				}

				mempool.AddUnverifiedTX(tx)
				md := NewProtocolMetadata(1, 1, parentBlock.digest)
				block := NewBlock(md, emptyBlacklist, mempool, []*TX{tx}, parentBlock)
				return mempool, block, nil
			},
		},
		{
			name:      "Tx Verification Fails",
			expectErr: errTxVerification,
			setup: func() (*Mempool, *Block, error) {
				mempool := NewMempool()
				tx := CreateNewTX()
				tx.SetShouldFailVerification(true)
				mempool.AddUnverifiedTX(tx)

				block := NewBlock(round0MD, emptyBlacklist, mempool, []*TX{tx}, nil)

				return mempool, block, nil
			},
		},
		{
			name:      "Correctly verifies transaction not in chain",
			expectErr: nil,
			setup: func() (*Mempool, *Block, error) {
				mempool := NewMempool()
				tx1 := CreateNewTX()
				mempool.AddUnverifiedTX(tx1)

				blockWithSameTxButNotParent := NewBlock(round0MD, emptyBlacklist, mempool, []*TX{tx1}, nil)
				err := mempool.VerifyBlock(ctx, blockWithSameTxButNotParent)

				mempool.AddUnverifiedTX(tx1)
				block := NewBlock(NewProtocolMetadata(1, 1, simplex.Digest{}), emptyBlacklist, mempool, []*TX{tx1}, nil)
				return mempool, block, err
			},
		},
		{
			name:      "Double Block Verification",
			expectErr: errDoubleBlockVerification,
			setup: func() (*Mempool, *Block, error) {
				mempool := NewMempool()
				tx := CreateNewTX()
				mempool.AddUnverifiedTX(tx)

				block := NewBlock(round0MD, emptyBlacklist, mempool, []*TX{tx}, nil)
				err := mempool.VerifyBlock(ctx, block)
				if err != nil {
					return nil, nil, err
				}

				mempool.AddUnverifiedTX(tx)
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

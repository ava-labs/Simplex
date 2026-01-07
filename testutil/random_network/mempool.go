package random_network

import (
	"context"
	"errors"

	"github.com/ava-labs/simplex"
)

var (
	errAlreadyAccepted         = errors.New("tx already accepted")
	errTxNotFound              = errors.New("tx not found")
	errAlreadyInChain          = errors.New("tx already in chain")
	errDuplicateTxInBlock      = errors.New("duplicate tx in block")
	errDoubleBlockVerification = errors.New("block has already been verified")
)

type Mempool struct {
	// txID -> TX
	unverifiedTXs map[txID]*TX

	// block digest -> Blocks
	verifiedButNotAcceptedTXs map[simplex.Digest]*Block

	// txID -> struct{}
	acceptedTXs map[txID]struct{}
}

func NewMempool() *Mempool {
	return &Mempool{
		unverifiedTXs:             make(map[txID]*TX),
		verifiedButNotAcceptedTXs: make(map[simplex.Digest]*Block),
		acceptedTXs:               make(map[txID]struct{}),
	}
}

func (m *Mempool) AddUnverifiedTX(tx *TX) {
	m.unverifiedTXs[tx.ID] = tx
}

// VerifyBlock verifies the block and its transactions. Errors if any tx is invalid or if there are duplicate txs in the block.
func (m *Mempool) VerifyBlock(ctx context.Context, b *Block) error {
	// ensure the block is not already verified
	if _, exists := m.verifiedButNotAcceptedTXs[b.digest]; exists {
		return errDoubleBlockVerification
	}

	// assert there are no duplicate txs in the block
	txIDSet := make(map[txID]struct{})
	for _, tx := range b.txs {
		if _, exists := txIDSet[tx.ID]; exists {
			return errDuplicateTxInBlock
		}
		txIDSet[tx.ID] = struct{}{}
	}

	// verify each transaction
	for _, tx := range b.txs {
		if err := m.verifyTx(ctx, tx, b); err != nil {
			return err
		}
	}

	// update state
	for _, tx := range b.txs {
		delete(m.unverifiedTXs, tx.ID)
	}
	m.verifiedButNotAcceptedTXs[b.digest] = b

	return nil
}

// verifyTx verifies a single transaction against the mempool state and the block's chain.
func (m *Mempool) verifyTx(ctx context.Context, tx *TX, block *Block) error {
	if _, exists := m.unverifiedTXs[tx.ID]; !exists {
		return errTxNotFound
	}

	if _, exists := m.acceptedTXs[tx.ID]; exists {
		return errAlreadyAccepted
	}

	if m.isTxInChain(tx.ID, block.metadata.Prev) {
		return errAlreadyInChain
	}

	if err := tx.Verify(ctx); err != nil {
		return err
	}

	return nil
}

func (m *Mempool) isTxInChain(txID txID, parentDigest simplex.Digest) bool {
	block, exists := m.verifiedButNotAcceptedTXs[parentDigest]
	if !exists {
		return false
	}

	if block.containsTX(txID) {
		return true
	}

	return m.isTxInChain(txID, block.metadata.Prev)
}

func (m *Mempool) AcceptBlock(b *Block) {
	for _, tx := range b.txs {
		m.acceptedTXs[tx.ID] = struct{}{}
	}
	delete(m.verifiedButNotAcceptedTXs, b.digest)
}

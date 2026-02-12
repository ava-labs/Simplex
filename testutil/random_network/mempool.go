// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package random_network

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ava-labs/simplex"
	"go.uber.org/zap"
)

var emptyDigest = simplex.Digest{}

var (
	errAlreadyAccepted         = errors.New("tx already accepted")
	errAlreadyInChain          = errors.New("tx already in chain")
	errDuplicateTxInBlock      = errors.New("duplicate tx in block")
	errDoubleBlockVerification = errors.New("block has already been verified")
)

type Mempool struct {
	config *FuzzConfig

	// txID -> TX
	unacceptedTxs map[txID]*TX

	// block digest -> Blocks
	verifiedButNotAcceptedBlocks map[simplex.Digest]*Block

	// fast lookup of accepted txs, could iterate over accepted blocks
	acceptedTXs map[txID]struct{}

	// all the blocks that have been accepted
	acceptedBlocks map[simplex.Digest]*Block

	lock     *sync.Mutex
	txsReady chan struct{}

	logger simplex.Logger
}

func NewMempool(l simplex.Logger, config *FuzzConfig) *Mempool {
	return &Mempool{
		unacceptedTxs:                make(map[txID]*TX),
		verifiedButNotAcceptedBlocks: make(map[simplex.Digest]*Block),
		acceptedTXs:                  make(map[txID]struct{}),
		acceptedBlocks:               make(map[simplex.Digest]*Block),
		lock:                         &sync.Mutex{},
		txsReady:                     make(chan struct{}, 1),
		logger:                       l,
		config:                       config,
	}
}

func (m *Mempool) AddPendingTXs(txs ...*TX) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, tx := range txs {
		m.unacceptedTxs[tx.ID] = tx
	}
}

func (m *Mempool) NotifyTxsReady() {
	select {
	case m.txsReady <- struct{}{}:
	default:
	}
}

func (m *Mempool) WaitForPendingTxs(ctx context.Context) {
	for {
		// Briefly check if txs are available
		m.lock.Lock()
		m.logger.Debug("Checking for pending txs in mempool", zap.Int("unacceptedTxs", len(m.unacceptedTxs)))
		hasTxs := len(m.unacceptedTxs) > 0
		m.lock.Unlock()

		if hasTxs {
			return
		}

		m.logger.Debug("No pending txs in mempool, waiting for txs to be added")

		// No txs available, wait for notification or cancellation
		select {
		case <-m.txsReady:
			// Might have txs now, loop back to check
		case <-ctx.Done():
			return
		}
	}
}

func (m *Mempool) PackBlock(ctx context.Context, maxTxs int, parentDigest simplex.Digest) []*TX {
	m.lock.Lock()
	defer m.lock.Unlock()

	txs := make([]*TX, 0, maxTxs)
	for _, tx := range m.unacceptedTxs {
		if err := m.verifyTx(ctx, tx, parentDigest); err != nil {
			m.logger.Debug("Skipping tx during block packing due to failed verification", zap.Stringer("txID", tx), zap.Error(err))
			continue
		}
		txs = append(txs, tx)
		if len(txs) >= maxTxs {
			break
		}
	}

	return txs
}

// VerifyBlock verifies the block and its transactions. Errors if any tx is invalid or if there are duplicate txs in the block.
func (m *Mempool) VerifyBlock(ctx context.Context, b *Block) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	// ensure the block is not already verified
	if _, exists := m.verifiedButNotAcceptedBlocks[b.digest]; exists {
		return fmt.Errorf("%w: %s", errDoubleBlockVerification, b.digest)
	}

	if parentInChain := m.isParentAcceptedOrVerified(b); !parentInChain {
		return fmt.Errorf("parent block %s not accepted or verified for block %s", b.metadata.Prev, b.digest)
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
		if err := m.verifyTx(ctx, tx, b.metadata.Prev); err != nil {
			return err
		}
	}

	// update state - don't delete from unverifiedTXs yet, as multiple nodes may build blocks with the same txs
	// txs will be deleted when the block is accepted
	m.verifiedButNotAcceptedBlocks[b.digest] = b

	return nil
}

func (m *Mempool) isParentAcceptedOrVerified(block *Block) bool {
	if block.metadata.Prev == emptyDigest {
		return true
	}

	_, exists := m.acceptedBlocks[block.metadata.Prev]
	if exists {
		return true
	}

	_, exists = m.verifiedButNotAcceptedBlocks[block.metadata.Prev]
	if exists {
		return true
	}

	return false
}

// verifyTx verifies a single transaction against the mempool state and the block's chain.
func (m *Mempool) verifyTx(ctx context.Context, tx *TX, blockParent simplex.Digest) error {
	if _, exists := m.acceptedTXs[tx.ID]; exists {
		return fmt.Errorf("%w: %s", errAlreadyAccepted, tx.ID)
	}

	if m.isTxInChain(tx.ID, blockParent) {
		return errAlreadyInChain
	}

	if err := tx.Verify(ctx); err != nil {
		return err
	}
	return nil
}

func (m *Mempool) isTxInChain(txID txID, parentDigest simplex.Digest) bool {
	block, exists := m.verifiedButNotAcceptedBlocks[parentDigest]
	if !exists {
		return false
	}

	if block.containsTX(txID) {
		return true
	}

	return m.isTxInChain(txID, block.metadata.Prev)
}

func (m *Mempool) AcceptBlock(b *Block) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.acceptedBlocks[b.digest] = b

	for _, tx := range b.txs {
		m.acceptedTXs[tx.ID] = struct{}{}
		delete(m.unacceptedTxs, tx.ID)
	}

	// delete any verified but not accepted blocks that are siblings or uncles and move not conflicting txs back to unaccepted
	delete(m.verifiedButNotAcceptedBlocks, b.digest)

	siblings := []*Block{}
	for _, verifiedBlock := range m.verifiedButNotAcceptedBlocks {
		if verifiedBlock.metadata.Prev == b.metadata.Prev {
			siblings = append(siblings, verifiedBlock)
			delete(m.verifiedButNotAcceptedBlocks, verifiedBlock.digest)
		}
	}

	for _, sibling := range siblings {
		m.purgeChildren(sibling)
	}

	if len(m.unacceptedTxs) > 0 {
		m.logger.Debug("After accepting block, moved txs back to unaccepted due to sibling/uncle blocks being purged", zap.Int("num unaccepted txs", len(m.unacceptedTxs)))
		m.NotifyTxsReady()
	}
}

// go through any blocks that build off of this one and move their txs
func (m *Mempool) purgeChildren(block *Block) {
	for digest, verifiedBlock := range m.verifiedButNotAcceptedBlocks {
		if verifiedBlock.metadata.Prev == block.digest {
			delete(m.verifiedButNotAcceptedBlocks, digest)
			m.moveTxsToUnaccepted(verifiedBlock)
			m.purgeChildren(verifiedBlock)
		}
	}

}

func (m *Mempool) moveTxsToUnaccepted(block *Block) {
	for _, tx := range block.txs {
		if _, exists := m.acceptedTXs[tx.ID]; !exists {
			m.unacceptedTxs[tx.ID] = tx
		}
	}
}

func (m *Mempool) BuildBlock(ctx context.Context, md simplex.ProtocolMetadata, bl simplex.Blacklist) (simplex.VerifiedBlock, bool) {
	m.WaitForPendingTxs(ctx)

	// Pack the block once we have pending txs
	txs := m.PackBlock(ctx, m.config.TxsPerBlock, md.Prev)
	if ctx.Err() != nil {
		return nil, false
	}
	block := NewBlock(md, bl, m, txs)
	m.logger.Debug("Built block with txs", zap.String("block digest", block.digest.String()), zap.Int("num txs", len(block.txs)), zap.Uint64("round", md.Round), zap.Uint64("seq", md.Seq))
	// in the future we can create a malicious block but we need to ensure the number of crashed nodes in under the threshold f(since we cant tolerate more than f malicious nodes)
	err := m.VerifyBlock(ctx, block)
	if err != nil {
		m.logger.Error("Failed to verify built block", zap.String("block digest", block.digest.String()), zap.Error(err))
		return nil, false
	}

	return block, true
}

func (m *Mempool) WaitForPendingBlock(ctx context.Context) {
	m.WaitForPendingTxs(ctx)
}

// IsTxAccepted returns true if the transaction has been accepted
func (m *Mempool) IsTxAccepted(txID txID) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, accepted := m.acceptedTXs[txID]
	return accepted
}

// IsTxPending returns true if the transaction is still pending (unaccepted)
func (m *Mempool) IsTxPending(txID txID) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, pending := m.unacceptedTxs[txID]
	return pending
}

func (m *Mempool) Clear() {
	m.lock.Lock()
	defer m.lock.Unlock()

	// move all the transactions from verified to unaccepted, since we are clearing the mempool but the transactions are still valid and can be re-included in future blocks
	for _, block := range m.verifiedButNotAcceptedBlocks {
		for _, tx := range block.txs {
			if _, accepted := m.acceptedTXs[tx.ID]; !accepted {
				m.unacceptedTxs[tx.ID] = tx
			}
		}
	}

	m.verifiedButNotAcceptedBlocks = make(map[simplex.Digest]*Block)
}

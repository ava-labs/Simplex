package random_network

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ava-labs/simplex"
	"go.uber.org/zap"
)

var (
	maxBackoff = 1 * time.Second

	errAlreadyAccepted         = errors.New("tx already accepted")
	errTxNotFound              = errors.New("tx not found")
	errAlreadyInChain          = errors.New("tx already in chain")
	errDuplicateTxInBlock      = errors.New("duplicate tx in block")
	errDoubleBlockVerification = errors.New("block has already been verified")
)

type Mempool struct {
	config *FuzzConfig

	// txID -> TX
	unacceptedTxs map[txID]*TX

	// block digest -> Blocks
	verifiedButNotAcceptedTXs map[simplex.Digest]*Block

	// txID -> struct{}
	acceptedTXs map[txID]struct{}

	lock             *sync.Mutex
	containsTxSignal sync.Cond

	logger simplex.Logger
}

func NewMempool(l simplex.Logger, config *FuzzConfig) *Mempool {
	lock := &sync.Mutex{}
	return &Mempool{
		unacceptedTxs:             make(map[txID]*TX),
		verifiedButNotAcceptedTXs: make(map[simplex.Digest]*Block),
		acceptedTXs:               make(map[txID]struct{}),
		lock:                      lock,
		containsTxSignal:          sync.Cond{L: lock},
		logger:                    l,
		config:                    config,
	}
}

func (m *Mempool) AddPendingTXs(txs ...*TX) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, tx := range txs {
		m.unacceptedTxs[tx.ID] = tx
	}

	m.containsTxSignal.Broadcast()
}

func (m *Mempool) WaitForPendingTxs(ctx context.Context) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.waitForPendingTxs(ctx)
}

func (m *Mempool) waitForPendingTxs(ctx context.Context) {
	// Check if context is already cancelled
	if ctx.Err() != nil {
		return
	}

	// Start a goroutine to broadcast when context is cancelled
	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			m.containsTxSignal.Broadcast()
		case <-done:
		}
	}()

	for len(m.unacceptedTxs) == 0 {
		// Check context before waiting
		if ctx.Err() != nil {
			return
		}
		m.containsTxSignal.Wait()
		// Check context after waking up
		if ctx.Err() != nil {
			return
		}
	}
}

func (m *Mempool) PackBlock(ctx context.Context, maxTxs int) []*TX {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.waitForPendingTxs(ctx)

	txs := make([]*TX, 0, maxTxs)
	for _, tx := range m.unacceptedTxs {
		txs = append(txs, tx)
		delete(m.unacceptedTxs, tx.ID)
		if len(txs) >= maxTxs {
			break
		}
	}

	return txs
}

func (m *Mempool) VerifyMyBuiltBlock(ctx context.Context, b *Block) {
	m.lock.Lock()
	defer m.lock.Unlock()

	// future function to verify blocks that shouldn't be
	m.verifiedButNotAcceptedTXs[b.digest] = b
}

// VerifyBlock verifies the block and its transactions. Errors if any tx is invalid or if there are duplicate txs in the block.
func (m *Mempool) VerifyBlock(ctx context.Context, b *Block) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.logger.Info("verifying block", zap.Stringer("digest", b.digest), zap.Stringer("parent", b.metadata.Prev))
	// ensure the block is not already verified
	if _, exists := m.verifiedButNotAcceptedTXs[b.digest]; exists {
		m.logger.Warn("Block has already been verified", zap.Error(errDoubleBlockVerification))
		return fmt.Errorf("%w: %s", errDoubleBlockVerification, b.digest)
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

	for _, tx := range b.txs {
		m.logger.Info("Block verified contains tx", zap.Stringer("txID", tx))
		delete(m.unacceptedTxs, tx.ID)
	}

	// update state - don't delete from unverifiedTXs yet, as multiple nodes may build blocks with the same txs
	// txs will be deleted when the block is accepted
	m.logger.Info("AND ADDED Block verified", zap.Stringer("digest", b.digest), zap.Stringer("parent", b.metadata.Prev))
	m.verifiedButNotAcceptedTXs[b.digest] = b

	return nil
}

// verifyTx verifies a single transaction against the mempool state and the block's chain.
func (m *Mempool) verifyTx(ctx context.Context, tx *TX, block *Block) error {
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

	m.logger.Info("checking if block", zap.Stringer("digest", block.digest), zap.Stringer("parent", parentDigest))
	if block.containsTX(txID) {
		return true
	}

	return m.isTxInChain(txID, block.metadata.Prev)
}

func (m *Mempool) AcceptBlock(b *Block) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, tx := range b.txs {
		m.acceptedTXs[tx.ID] = struct{}{}
		delete(m.unacceptedTxs, tx.ID)
	}
	delete(m.verifiedButNotAcceptedTXs, b.digest)
}

// backoff waits for `backoff` duration before returning the next backoff duration.
// It doubles the backoff duration each time it is called, up to a maximum of `maxBackoff`.
func backoff(ctx context.Context, backoff time.Duration) time.Duration {
	select {
	case <-ctx.Done():
		return 0
	case <-time.After(backoff):
	}

	return min(maxBackoff, 2*backoff) // exponential backoff
}

func (m *Mempool) BuildBlock(ctx context.Context, md simplex.ProtocolMetadata, bl simplex.Blacklist) (simplex.VerifiedBlock, bool) {
	txs := m.PackBlock(ctx, m.config.MaxTxsPerBlock)
	block := NewBlock(md, bl, m, txs)
	m.logger.Info("Building block with txs", zap.Int("num txs", len(txs)), zap.Any("prev", md.Prev), zap.Stringer("digest", block.digest), zap.Any("md", md))
	m.VerifyMyBuiltBlock(ctx, block)

	return block, true
}

func (m *Mempool) WaitForPendingBlock(ctx context.Context) {
	m.WaitForPendingTxs(ctx)
}

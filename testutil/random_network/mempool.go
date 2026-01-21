package random_network

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ava-labs/simplex"
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

	lock     *sync.Mutex
	txsReady chan struct{}

	logger simplex.Logger
}

func NewMempool(l simplex.Logger, config *FuzzConfig) *Mempool {
	return &Mempool{
		unacceptedTxs:             make(map[txID]*TX),
		verifiedButNotAcceptedTXs: make(map[simplex.Digest]*Block),
		acceptedTXs:               make(map[txID]struct{}),
		lock:                      &sync.Mutex{},
		txsReady:                  make(chan struct{}, 1),
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
		hasTxs := len(m.unacceptedTxs) > 0
		m.lock.Unlock()

		if hasTxs {
			return
		}

		// No txs available, wait for notification or cancellation
		select {
		case <-m.txsReady:
			// Might have txs now, loop back to check
		case <-ctx.Done():
			return
		}
	}
}

func (m *Mempool) PackBlock(ctx context.Context, maxTxs int) []*TX {
	m.WaitForPendingTxs(ctx)

	m.lock.Lock()
	defer m.lock.Unlock()

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
	// ensure the block is not already verified
	if _, exists := m.verifiedButNotAcceptedTXs[b.digest]; exists {
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
		delete(m.unacceptedTxs, tx.ID)
	}

	// update state - don't delete from unverifiedTXs yet, as multiple nodes may build blocks with the same txs
	// txs will be deleted when the block is accepted
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

func (m *Mempool) NumVerifiedBlocks() int {
	m.lock.Lock()
	defer m.lock.Unlock()

	return len(m.verifiedButNotAcceptedTXs)
}

func (m *Mempool) BuildBlock(ctx context.Context, md simplex.ProtocolMetadata, bl simplex.Blacklist) (simplex.VerifiedBlock, bool) {
	txs := m.PackBlock(ctx, m.config.MaxTxsPerBlock)
	if ctx.Err() != nil {
		return nil, false
	}
	block := NewBlock(md, bl, m, txs)
	m.VerifyMyBuiltBlock(ctx, block)

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

	m.verifiedButNotAcceptedTXs = make(map[simplex.Digest]*Block)
	m.acceptedTXs = make(map[txID]struct{})
}

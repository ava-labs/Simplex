package random_network

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ava-labs/simplex"
)

var (
	maxBackoff = 1 * time.Second

	errAlreadyAccepted    = errors.New("tx already accepted")
	errTxNotFound         = errors.New("tx not found")
	errAlreadyInChain     = errors.New("tx already in chain")
	errDuplicateTxInBlock = errors.New("duplicate tx in block")
)

type Mempool struct {
	// txID -> TX
	unverifiedTXs map[txID]*TX

	// block digest -> Blocks
	verifiedButNotAcceptedTXs map[simplex.Digest]*Block

	// txID -> struct{}
	acceptedTXs map[txID]struct{}

	lock             *sync.Mutex
	containsTxSignal sync.Cond

	logger simplex.Logger
}

func NewMempool(l simplex.Logger) *Mempool {
	lock := &sync.Mutex{}
	return &Mempool{
		unverifiedTXs:             make(map[txID]*TX),
		verifiedButNotAcceptedTXs: make(map[simplex.Digest]*Block),
		acceptedTXs:               make(map[txID]struct{}),
		lock:                      lock,
		containsTxSignal:          sync.Cond{L: lock},
		logger:                    l,
	}
}

func (m *Mempool) AddPendingTXs(txs ...*TX) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, tx := range txs {
		m.unverifiedTXs[tx.ID] = tx
	}

	m.containsTxSignal.Broadcast()
}

func (m *Mempool) WaitForPendingTxs(ctx context.Context) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.waitForPendingTxs(ctx)
}

func (m *Mempool) waitForPendingTxs(ctx context.Context) {
	for len(m.unverifiedTXs) == 0 {
		m.containsTxSignal.Wait()
	}
}

func (m *Mempool) PackBlock(ctx context.Context, maxTxs int) []*TX {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.waitForPendingTxs(ctx)

	txs := make([]*TX, 0, maxTxs)
	for _, tx := range m.unverifiedTXs {
		txs = append(txs, tx)
		if len(txs) >= maxTxs {
			break
		}
	}

	return txs
}

func (m *Mempool) VerifyMyBuiltBlock(ctx context.Context, b *Block) {
	m.lock.Lock()
	defer m.lock.Unlock()

	// don't delete txs here - they will be deleted when the block is accepted
	// this allows the leader to verify its own block during replication if needed
	m.verifiedButNotAcceptedTXs[b.digest] = b
}

// VerifyBlock verifies the block and its transactions. Errors if any tx is invalid or if there are duplicate txs in the block.
func (m *Mempool) VerifyBlock(ctx context.Context, b *Block) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	// if the block is already verified, return success
	if _, exists := m.verifiedButNotAcceptedTXs[b.digest]; exists {
		return nil
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

	// update state - don't delete from unverifiedTXs yet, as multiple nodes may build blocks with the same txs
	// txs will be deleted when the block is accepted
	m.verifiedButNotAcceptedTXs[b.digest] = b

	return nil
}

// verifyTx verifies a single transaction against the mempool state and the block's chain.
func (m *Mempool) verifyTx(ctx context.Context, tx *TX, block *Block) error {
	initBackoff := 10 * time.Millisecond

	for curBackoff := initBackoff; ; curBackoff = backoff(ctx, curBackoff) {
		// check if context is done
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if _, exists := m.unverifiedTXs[tx.ID]; !exists {
			// wait to receive the tx
			continue
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
		delete(m.unverifiedTXs, tx.ID)
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

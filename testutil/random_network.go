package testutil

import (
	"errors"

	"github.com/ava-labs/simplex"
)

var (
	errAlreadyAccepted = errors.New("tx already accepted")
	errTxNotFound      = errors.New("tx not found")
	errAlreadyInChain  = errors.New("tx already in chain")
)
type txID [32]byte

type TX struct {
	ID txID
}

// type DigestInfo struct {
// 	Digest simplex.Digest
// 	Parent simplex.Digest
// }
type Block struct {
	Seq    uint64
	Digest simplex.Digest
	Parent simplex.Digest
	TXs    []*TX
}

type Mempool struct {
	// txID -> TX
	unverifiedTXs map[txID]*TX

	// block digest -> TXs
	verifiedTXs   map[simplex.Digest]*Block

	// txID -> struct{}
	acceptedTXs map[txID]struct{}
}

func NewMempool() *Mempool {
	return &Mempool{
		unverifiedTXs: make(map[txID]*TX),
		verifiedTXs:   make(map[simplex.Digest]*Block),
		acceptedTXs:   make(map[txID]struct{}),
	}
}

func (m *Mempool) AddUnverifiedTX(tx *TX) {
	m.unverifiedTXs[tx.ID] = tx
}

// before calling verify tx we should ensure there are no duplicate txs in the block
// after verifying all the txs we should add the block to the verified txs
func (m *Mempool) VerifyTx(tx *TX, block *Block) error {
	if _, exists := m.unverifiedTXs[tx.ID]; !exists {
		return errTxNotFound
	}

	if _, exists := m.acceptedTXs[tx.ID]; exists {
		return errAlreadyAccepted
	}

	if !m.verifyTxDoesntExistInChain(tx.ID, block) {
		return errAlreadyInChain
	}

	return nil
}

func (m *Mempool) verifyTxDoesntExistInChain(txID txID, block *Block) bool {
	parentBlock, exists := m.verifiedTXs[block.Parent]
	if !exists {
		return true
	}

	if parentBlock.ContainsTX(txID) {
		return false
	}

	return m.verifyTxDoesntExistInChain(txID, parentBlock)
}

func (b *Block) ContainsTX(txID txID) bool {
	for _, tx := range b.TXs {
		if tx.ID == txID {
			return true
		}
	}
	return false
}

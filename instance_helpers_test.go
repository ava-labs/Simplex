// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex/avalanchego"
	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/ava-labs/simplex/testutil"
	"github.com/ava-labs/simplex/wal"

	"github.com/stretchr/testify/require"
)

type testInnerBlock struct {
	Height_ uint64
	TS      time.Time
	Payload []byte
}

func (b *testInnerBlock) Bytes() []byte {
	out := make([]byte, 16, 16+len(b.Payload))
	binary.BigEndian.PutUint64(out[0:8], b.Height_)
	binary.BigEndian.PutUint64(out[8:16], uint64(b.TS.UnixMilli()))
	out = append(out, b.Payload...)
	return out
}

func (b *testInnerBlock) Digest() [32]byte {
	bytes := b.Bytes()
	return sha256.Sum256(bytes)
}

func (b *testInnerBlock) Height() uint64                       { return b.Height_ }
func (b *testInnerBlock) Timestamp() time.Time                 { return b.TS }
func (b *testInnerBlock) Verify(context.Context, uint64) error { return nil }

func parseTestInnerBlock(buff []byte) (*testInnerBlock, error) {
	b := &testInnerBlock{}
	b.Height_ = binary.BigEndian.Uint64(buff[0:8])
	b.TS = time.UnixMilli(int64(binary.BigEndian.Uint64(buff[8:16])))
	b.Payload = append([]byte(nil), buff[16:]...)
	return b, nil
}

type testPlatformChain struct {
	baseHeight           uint64
	validatorSetAtHeight map[uint64]metadata.NodeBLSMappings // height --> validator set
	lock                 sync.Mutex
	cond                 *sync.Cond
	height               uint64
}

func newTestPlatformChain(baseHeight uint64, validatorSetsAtHeight map[uint64]metadata.NodeBLSMappings) *testPlatformChain {
	pc := &testPlatformChain{
		baseHeight:           baseHeight,
		validatorSetAtHeight: validatorSetsAtHeight,
		height:               baseHeight,
	}
	pc.cond = sync.NewCond(&pc.lock)
	return pc
}

func (pc *testPlatformChain) advanceTo(h uint64) {
	pc.lock.Lock()
	defer pc.lock.Unlock()
	pc.height = h
	pc.cond.Broadcast() // wake any WaitForProgress waiters
}

func (pc *testPlatformChain) currentHeight() uint64 {
	pc.lock.Lock()
	defer pc.lock.Unlock()
	return pc.height
}

func (pc *testPlatformChain) validatorSet(height uint64) metadata.NodeBLSMappings {
	heights := make([]uint64, 0, len(pc.validatorSetAtHeight))
	for h := range pc.validatorSetAtHeight {
		heights = append(heights, h)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })

	var lastCheckpoint uint64
	for _, h := range heights {
		if h > height {
			break
		}
		lastCheckpoint = h
	}
	// Return a copy instead of the original slice so the reference won't be used in other goroutines concurrently.
	// Since we allocate a nil slice, a new underlying array is allocated and the copy is safe to use concurrently.
	src := pc.validatorSetAtHeight[lastCheckpoint]
	return append(metadata.NodeBLSMappings(nil), src...)
}

func (pc *testPlatformChain) GetValidatorSet(height uint64) (metadata.NodeBLSMappings, error) {
	return pc.validatorSet(height), nil
}

func (pc *testPlatformChain) GenesisValidatorSet() metadata.NodeBLSMappings {
	return pc.validatorSet(pc.baseHeight)
}

func (pc *testPlatformChain) GetMinimumHeight() uint64 {
	return pc.currentHeight()
}

func (pc *testPlatformChain) GetCurrentHeight() uint64 {
	return pc.currentHeight()
}

func (pc *testPlatformChain) WaitForProgress(ctx context.Context, pChainHeight uint64) error {
	stop := pc.signalWhenContextFinished(ctx)
	defer stop()

	pc.lock.Lock()
	defer pc.lock.Unlock()
	for pc.height == pChainHeight {
		if err := ctx.Err(); err != nil {
			return err
		}
		pc.cond.Wait()
	}
	return nil
}

func (pc *testPlatformChain) signalWhenContextFinished(ctx context.Context) func() bool {
	stop := context.AfterFunc(ctx, func() {
		pc.lock.Lock()
		defer pc.lock.Unlock()
		pc.cond.Broadcast()
	})
	return stop
}

func (pc *testPlatformChain) LastNonSimplexBlockPChainHeight() uint64 {
	return pc.baseHeight
}

type testCryptoOps struct{}

func (c *testCryptoOps) Sign(message []byte) ([]byte, error) {
	// A deterministic, non-empty placeholder signature.
	d := sha256.Sum256(message)
	return d[:], nil
}

func (c *testCryptoOps) AggregateKeys(keys ...[]byte) ([]byte, error) {
	var out []byte
	for _, k := range keys {
		out = append(out, k...)
	}
	return out, nil
}

func (c *testCryptoOps) VerifySignature(_ []byte, _ []byte, _ []byte) error {
	return nil
}

func (c *testCryptoOps) CreateSignatureAggregator(nodes []common.Node) common.SignatureAggregator {
	return &testutil.TestSignatureAggregator{N: len(nodes)}
}

func (c *testCryptoOps) DeserializeQuorumCertificate(bytes []byte) (common.QuorumCertificate, error) {
	var qc []common.Signature
	if _, err := asn1.Unmarshal(bytes, &qc); err != nil {
		return nil, err
	}
	return testutil.TestQC(qc), nil
}

type MockStorage struct {
	t *testing.T
	*testutil.InMemStorage

	snapLock sync.Mutex
	blocks   map[uint64]storedBlock
	wals     []*testutil.TestWAL
}

type storedBlock struct {
	rawBlock []byte
	fin      common.Finalization
}

func NewMockStorage(t *testing.T) *MockStorage {
	return &MockStorage{
		t:            t,
		InMemStorage: testutil.NewInMemStorage(),
		blocks:       make(map[uint64]storedBlock),
	}
}

func (m *MockStorage) Index(ctx context.Context, block common.VerifiedBlock, certificate common.Finalization) error {
	// We serialized the block so that the original reference isn't shared with other goroutines that may concurrently mutate it.
	encoded := block.Bytes()
	seq := m.NumBlocks()
	m.snapLock.Lock()
	m.blocks[seq] = storedBlock{rawBlock: encoded, fin: certificate}
	m.snapLock.Unlock()
	return m.InMemStorage.Index(ctx, block, certificate)
}

func (m *MockStorage) GetBlock(seq uint64) (metadata.StateMachineBlock, *common.Finalization, error) {
	_, f, err := m.Retrieve(seq)
	if err != nil {
		return metadata.StateMachineBlock{}, nil, err
	}
	sb, ok := m.blockAt(seq)
	if !ok {
		return metadata.StateMachineBlock{}, nil, fmt.Errorf("no snapshot for seq %d", seq)
	}
	return sb, &f, nil
}

// blockAt reconstructs an independent copy of the block at seq from its
// stored bytes. Test-only readers use it instead of GetBlock so they never touch
// the instance's live block objects (whose canoto digest cache the instance keeps
// mutating).
func (m *MockStorage) blockAt(seq uint64) (metadata.StateMachineBlock, bool) {
	m.snapLock.Lock()
	sb, ok := m.blocks[seq]
	m.snapLock.Unlock()
	if !ok {
		return metadata.StateMachineBlock{}, false
	}
	return m.parseStored(sb.rawBlock), true
}

func (m *MockStorage) parseStored(encoded []byte) metadata.StateMachineBlock {
	raw := &metadata.RawBlock{}
	require.NoError(m.t, raw.UnmarshalCanoto(encoded))
	var inner avalanchego.VMBlock
	if len(raw.InnerBlockBytes) > 0 {
		parsed, err := parseTestInnerBlock(raw.InnerBlockBytes)
		require.NoError(m.t, err)
		inner = parsed
	}
	return metadata.StateMachineBlock{InnerBlock: inner, Metadata: raw.Metadata}
}

func (m *MockStorage) CreateWAL() (wal.DeletableWAL, error) {
	w := testutil.NewTestWAL(m.t)
	m.snapLock.Lock()
	m.wals = append(m.wals, w)
	m.snapLock.Unlock()
	return w, nil
}

// containsNotarization reports whether any WAL this storage handed out holds a notarization for
// the given round.
func (m *MockStorage) containsNotarization(round uint64) bool {
	m.snapLock.Lock()
	wals := append([]*testutil.TestWAL(nil), m.wals...)
	m.snapLock.Unlock()

	for _, w := range wals {
		if w.ContainsNotarization(round) {
			return true
		}
	}
	return false
}

// toRawBlock re-encodes a verified block into the wire RawBlock the receiving
// instance parses in HandleBlockMessage.
func toRawBlock(t *testing.T, vb common.VerifiedBlock) *metadata.RawBlock {
	bytes := vb.Bytes()
	raw := &metadata.RawBlock{}
	require.NoError(t, raw.UnmarshalCanoto(bytes))
	return raw
}

// reparseBlock reconstructs an independent *ParsedBlock from a verified block's
// wire bytes. Each call yields a fresh object sharing no pointers with the
// sender's live block, so that remaining references to the sender's block don't race with the receiver.
func reparseBlock(t *testing.T, vb common.VerifiedBlock) *ParsedBlock {
	raw := toRawBlock(t, vb)
	var inner avalanchego.VMBlock
	if len(raw.InnerBlockBytes) > 0 {
		parsed, err := parseTestInnerBlock(raw.InnerBlockBytes)
		require.NoError(t, err)
		inner = parsed
	}
	return &ParsedBlock{
		StateMachineBlock: metadata.StateMachineBlock{InnerBlock: inner, Metadata: raw.Metadata},
	}
}

func verifiedQuorumRoundToQuorumRound(t *testing.T, vqr common.VerifiedQuorumRound) common.QuorumRound {
	qr := common.QuorumRound{
		Notarization:      vqr.Notarization,
		Finalization:      vqr.Finalization,
		EmptyNotarization: vqr.EmptyNotarization,
	}
	if vqr.VerifiedBlock != nil {
		qr.Block = reparseBlock(t, vqr.VerifiedBlock)
	}
	return qr
}
